/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_provider.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/selection/selection.hh"
#include "index/secondary_index_manager.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "query/query-request.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

#include <algorithm>

namespace cql3::statements {

static logging::logger fts_log("fulltext_search");

namespace {

/// One of the values a full-text search reports, as it reaches a row: which slot delivers it, what
/// type that slot has, and - for a value generated from the row's own text - where to record the
/// column that text comes from.
struct bm25_value {
    /// Must be the declared return type of the function reporting it on its own, since that is what
    /// typed the selector reading the slot.
    data_type slot_type;
    /// Where the index of that slot is kept, allocated on the value's first occurrence.
    std::optional<size_t> bm25_ordering_info::*slot;
    /// Null for a value the index reports itself.
    const column_definition* bm25_ordering_info::*source_column;
};

/// What a call to the family names.  Every one of them describes the one search the rows are ranked
/// by and is validated by the one set of rules in prepare_bm25_selectors(); this is everything that
/// differs between them.  A call naming the pair the search answers with delivers both of its
/// halves and combines them, which is why the values are a list.
struct bm25_selection {
    /// The name a message about the call names it by.
    std::string_view name;
    std::vector<bm25_value> values;
    /// What the values are combined into, or null when the call names a single one.
    data_type combined_type;
};

/// What the given call names, or std::nullopt when it is not a call to the family.
///
/// The values are built per call rather than kept in a static: a data_type is thread_local, one
/// instance per shard, so a table shared between shards would hand every shard the types of
/// whichever one built it first.
std::optional<bm25_selection> selected_bm25_value(const expr::function_call& fc) {
    const bm25_value score{float_type, &bm25_ordering_info::score_temporary_index, nullptr};
    const bm25_value rank{int32_type, &bm25_ordering_info::rank_temporary_index, nullptr};
    const bm25_value fragment{utf8_type, &bm25_ordering_info::highlight_temporary_index, &bm25_ordering_info::highlighted_column};

    if (expr::is_native_function_call(fc, functions::BM25_FUNCTION_NAME)) {
        return bm25_selection{"BM25", {score, rank}, functions::search_hit_type()};
    }
    if (expr::is_native_function_call(fc, functions::BM25_SCORE_FUNCTION_NAME)) {
        return bm25_selection{"BM25_SCORE", {score}, nullptr};
    }
    if (expr::is_native_function_call(fc, functions::BM25_RANK_FUNCTION_NAME)) {
        return bm25_selection{"BM25_RANK", {rank}, nullptr};
    }
    if (expr::is_native_function_call(fc, functions::BM25_HIGHLIGHT_FUNCTION_NAME)) {
        return bm25_selection{"BM25_HIGHLIGHT", {fragment}, nullptr};
    }
    return std::nullopt;
}

/// The expression a selection is delivered by, allocating each slot on its first occurrence.  The
/// slots hold the values themselves even when a call combines them, so naming the pair and naming a
/// half in one query costs one slot per value rather than one per spelling.
///
/// A slot standing for a whole call carries it, so that an unaliased selector is still named after
/// what the user wrote; the ones inside a combination carry nothing, because none of them is the
/// call - naming that one is the caller's job.
expr::expression deliver_bm25_selection(const bm25_selection& selection, const expr::expression& call, const column_definition* col,
        bm25_ordering_info& info, expr::temporary_allocator& temporaries_allocator) {
    auto deliver = [&] (const bm25_value& value, std::optional<expr::expression> replaced) {
        auto& slot = info.*value.slot;
        if (!slot) {
            slot = temporaries_allocator.allocate();
        }
        if (value.source_column) {
            // Checked equal to the ranked column by the caller.
            info.*value.source_column = col;
        }
        return expr::expression(expr::temporary{.index = *slot, .type = value.slot_type, .replaced_expr = std::move(replaced)});
    };

    if (!selection.combined_type) {
        return deliver(selection.values.front(), call);
    }
    auto elements = std::vector<expr::expression>{};
    elements.reserve(selection.values.size());
    for (const auto& value : selection.values) {
        elements.push_back(deliver(value, std::nullopt));
    }
    return expr::expression(expr::tuple_constructor{.elements = std::move(elements), .type = selection.combined_type});
}

std::optional<expr::expression> validate_bm25_where_restriction(const expr::binary_operator& binop,
        const bm25_ordering_info& ordering_info) {
    // Whichever of the family the user wrote, preparation rewrote it to the reading a relation
    // compares, so this is the score.
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    if (!expr::is_native_function_call(fc, functions::BM25_SCORE_FUNCTION_NAME)) {
        // Not the score, so not this search's relevance: a fragment is generated from a row the
        // search has already selected, and another search's score does not belong here either.
        throw exceptions::invalid_request_exception(seastar::format("{}() is not supported in the WHERE clause",
                std::get<shared_ptr<db::functions::function>>(fc.func)->name().name));
    }
    auto [col, where_term] = external_search::extract_call_arguments(fc, "BM25");
    if (col->name_as_text() != ordering_info.index.target_column()) {
        throw exceptions::invalid_request_exception("Full-text search queries must reference the same column in both WHERE and ORDER BY clauses");
    }

    if (binop.op != expr::oper_t::GT) {
        throw exceptions::invalid_request_exception(
                seastar::format("Unsupported \"{}\" relation for BM25 function restriction, only \">\" is supported", binop.op));
    }
    const auto* rhs_const = expr::as_if<expr::constant>(&binop.rhs);
    if (!rhs_const || rhs_const->is_null() || rhs_const->view().deserialize<float>(*float_type) != 0.0f) {
        throw exceptions::invalid_request_exception("BM25 function comparison value must be the literal 0");
    }

    return external_search::check_query_value(where_term, ordering_info.search_term,
            "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses");
}

/// Asks the index to mark the terms of `search_term` in each of `documents`.  One request for them
/// all, answered positionally, so the fragments come back lined up with the rows the documents were
/// collected from.
future<vector_search::vector_store_client::highlights> fetch_highlights(vector_search::vector_store_client& client, const sstring& keyspace,
        const sstring& index, const sstring& search_term, std::vector<sstring> documents, abort_source& as) {

    if (documents.empty()) {
        // Nothing matched, so there is nothing to mark up and no reason to ask.
        co_return vector_search::vector_store_client::highlights{};
    }

    auto fragments = co_await client.highlight(keyspace, index, search_term, std::move(documents), as);
    if (!fragments.has_value()) {
        // Being unable to ask fails the query.  A null fragment means the index found none, and the
        // two must not arrive as the same answer.
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, fragments.error())));
    }
    co_return std::move(fragments.value());
}

/// The fragments as the slot that reports them holds each one: text, or null for a row the index
/// found no fragment in - such a row is kept, with the value left absent.
std::vector<cql3::raw_value> to_values(const vector_search::vector_store_client::highlights& fragments) {
    auto values = std::vector<cql3::raw_value>{};
    values.reserve(fragments.size());
    for (const auto& fragment : fragments) {
        values.push_back(fragment ? cql3::raw_value::make_value(utf8_type->decompose(*fragment)) : cql3::raw_value::make_null());
    }
    return values;
}

} // anonymous namespace

void prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator, prepare_context& ctx) {
    for (auto& ps : prepared_selectors) {
        // What the user wrote, before any of it is lowered - the name an unaliased selector that
        // named the whole pair has to keep, since a tuple of slots formats as neither call.
        const auto written = ps.expr;
        bool named_the_pair = false;

        ps.expr = expr::search_and_replace(ps.expr, [&](const expr::expression& candidate) -> std::optional<expr::expression> {
            const auto* fc = expr::as_if<expr::function_call>(&candidate);
            if (!fc) {
                return std::nullopt;
            }
            const auto value = selected_bm25_value(*fc);
            if (!value) {
                return std::nullopt;
            }

            if (!ordering_info) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() is not supported in the SELECT clause without matching ORDER BY and WHERE clauses", value->name));
            }
            auto& info = *ordering_info;

            // Every call in the statement describes the one search the rows are ranked by, whichever
            // of its values it asks for, so they all have to name the column and the search term the
            // other two clauses do.
            auto [col, sel_term] = external_search::extract_call_arguments(*fc, value->name);
            if (col->name_as_text() != info.index.target_column()) {
                throw exceptions::invalid_request_exception(
                        seastar::format("{}() in SELECT must reference the same column as BM25() in WHERE and ORDER BY", value->name));
            }

            if (auto deferred = external_search::check_query_value(sel_term, info.search_term,
                        seastar::format("{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", value->name))) {
                // Lifted out of the selector tree, so nothing else registers its bind marker.
                expr::fill_prepare_context(*deferred, ctx);
                info.deferred_select_terms.push_back({std::move(*deferred), value->name});
            }

            // Every occurrence of one value reports the same thing, so one slot serves them all.
            named_the_pair |= bool(value->combined_type);
            return deliver_bm25_selection(*value, candidate, col, info, temporaries_allocator);
        });

        if (named_the_pair && !ps.alias) {
            ps.alias = ::make_shared<column_identifier>(fmt::format("{:result_set_metadata}", written), true);
        }
    }
}

std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc) {

    if (!expr::is_native_function_call(fc, functions::BM25_FUNCTION_NAME)) {
        return std::nullopt;
    }
    auto [column, search_term] = external_search::extract_call_arguments(fc, "BM25");

    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();

    for (const auto& idx : sim.list_indexes()) {
        if (idx.supports_bm25_expression(*column)) {
            return bm25_ordering_info{idx, std::move(search_term)};
        }
    }

    throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
}

::shared_ptr<cql3::statements::select_statement> fulltext_indexed_table_select_statement::prepare(data_dictionary::database db,
        schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        std::optional<bm25_ordering_info> ordering_info,
        std::unique_ptr<attributes> attrs) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a LIMIT");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Full-text search queries do not support per-partition limits");
    }

    // GROUP BY is aggregation too, and a selected score does not make the selection look aggregate.
    if (selection->is_aggregate() || !group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception("Full-text search queries cannot be run with aggregation");
    }

    if (!ordering_info) {
        throw exceptions::invalid_request_exception("Full-text search queries require an ORDER BY BM25() clause");
    }

    const auto& scoring_restrictions = restrictions->get_scoring_function_restrictions();
    if (scoring_restrictions.empty()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a WHERE BM25() > 0 clause");
    }
    if (scoring_restrictions.size() > 1) {
        throw exceptions::invalid_request_exception("Full-text search queries support only one WHERE BM25() restriction");
    }

    ordering_info->deferred_where_term = validate_bm25_where_restriction(scoring_restrictions.front(), *ordering_info);

    // Reject any WHERE restrictions beyond the single BM25 clause.
    // BM25 restrictions are excluded from `restrictions`.
    if (!restrictions->partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions->get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions->get_nonprimary_key_restrictions())) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries do not support additional WHERE restrictions");
    }

    // A score or rank slot was allocated, so what the search said about each row is selected, and a
    // provider will fill those slots by matching each row to the index's response by primary key.
    if (ordering_info->score_temporary_index || ordering_info->rank_temporary_index) {
        external_search::fetch_primary_key_columns(*selection, *schema);
    }

    // A fragment slot was allocated, so BM25_HIGHLIGHT() was selected and the index will be asked to
    // generate the fragment from the row's own text - which it stores none of, so the text has to be
    // read from every row even when the query does not select the column itself.
    if (ordering_info->highlight_temporary_index) {
        external_search::fetch_column(*selection, *ordering_info->highlighted_column);
    }

    return ::make_shared<cql3::statements::fulltext_indexed_table_select_statement>(
            schema,
            bound_terms,
            parameters,
            std::move(selection),
            std::move(restrictions),
            std::move(group_by_cell_indices),
            is_reversed,
            std::move(ordering_comparator),
            std::move(limit),
            std::move(per_partition_limit),
            stats,
            std::move(*ordering_info),
            std::move(attrs));
}

fulltext_indexed_table_select_statement::fulltext_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        bm25_ordering_info ordering_info, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions,
              group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit,
              stats, ordering_info.index, std::move(attrs)}
    , _bm25_ordering_info{std::move(ordering_info)} {
}

future<shared_ptr<cql_transport::messages::result_message>> fulltext_indexed_table_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_fts_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Full-text search queries require a LIMIT that is not greater than {}. LIMIT was {}", max_fts_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    auto search_term_val = expr::evaluate(_bm25_ordering_info.search_term, options);
    if (search_term_val.is_null()) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception("Full-text search query term must not be null"));
    }

    if (_bm25_ordering_info.deferred_where_term
            && expr::evaluate(*_bm25_ordering_info.deferred_where_term, options) != search_term_val) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses"));
    }

    for (const auto& sel_term : _bm25_ordering_info.deferred_select_terms) {
        if (expr::evaluate(sel_term.term, options) != search_term_val) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(seastar::format(
                    "{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", sel_term.function_name)));
        }
    }

    auto search_term_bytes = std::move(search_term_val).to_bytes();
    sstring search_term_text = value_cast<sstring>(utf8_type->deserialize(search_term_bytes));

    auto pkeys = co_await qp.vector_store_client().bm25(_schema->ks_name(), _index.metadata().name(), _schema, search_term_text, limit, aoe.abort_source());
    if (!pkeys.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, pkeys.error())));
    }

    throwing_assert(pkeys->size() <= limit);

    auto read = co_await query_base_table(qp, state, options, timeout, pkeys.value());

    const auto score_slot = _bm25_ordering_info.score_temporary_index;
    const auto rank_slot = _bm25_ordering_info.rank_temporary_index;
    const auto fragment_slot = _bm25_ordering_info.highlight_temporary_index;

    auto provider = std::unique_ptr<external_search_provider>{};
    if (read.rows && (score_slot || rank_slot || fragment_slot)) {
        // What the search says about a row can only be lined up with it now that the rows are read,
        // and a fragment does not exist at all until the index has been sent their text.
        auto answers = match_search_results(*read.rows.value(), read.command->slice, *_schema, *_selection, pkeys.value(), score_slot,
                rank_slot, fragment_slot ? _bm25_ordering_info.highlighted_column : nullptr);

        if (fragment_slot) {
            auto fragments = co_await fetch_highlights(qp.vector_store_client(), _schema->ks_name(), _index.metadata().name(), search_term_text,
                    std::move(answers.documents), aoe.abort_source());
            answers.slots.emplace_back(*fragment_slot, to_values(fragments));
        }
        provider = std::make_unique<external_search_provider>(std::move(answers.slots), std::move(answers.dropped));
    }
    co_return co_await emit_result_set(std::move(read), options, provider.get());
}

} // namespace cql3::statements
