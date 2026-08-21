/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_score_provider.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/secondary_index_manager.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

namespace cql3::statements {

static logging::logger fts_log("fulltext_search");

namespace {

/// Which of a full-text search's readings a call names: the pair the search answers with, or one
/// of its halves.  They are read from one search and validated by one set of rules; they differ
/// only in which slot delivers them.
enum class bm25_reading { hit, score, rank };

struct named_bm25_reading {
    bm25_reading reading;
    std::string_view name;
};

std::optional<named_bm25_reading> bm25_reading_of(const expr::function_call& fc) {
    if (expr::is_native_function_call(fc, functions::BM25_FUNCTION_NAME)) {
        return named_bm25_reading{bm25_reading::hit, "BM25"};
    }
    if (expr::is_native_function_call(fc, functions::BM25_SCORE_FUNCTION_NAME)) {
        return named_bm25_reading{bm25_reading::score, "BM25_SCORE"};
    }
    if (expr::is_native_function_call(fc, functions::BM25_RANK_FUNCTION_NAME)) {
        return named_bm25_reading{bm25_reading::rank, "BM25_RANK"};
    }
    return std::nullopt;
}

/// The expression a reading is delivered by, allocating its slot on first use.  The slots hold the
/// halves even when the whole pair is asked for, so that naming the pair and naming a half in one
/// query costs one slot each rather than one of each per spelling.
///
/// A slot standing for a whole call carries it, so that an unaliased selector is still named after
/// what the user wrote; the two inside a pair carry nothing, because neither of them is the call -
/// naming that one is the caller's job.
expr::expression deliver_bm25_reading(bm25_reading reading, const expr::expression& call, bm25_ordering_info& info,
        expr::temporary_allocator& temporaries_allocator) {
    auto slot = [&] (std::optional<size_t>& index, data_type type, std::optional<expr::expression> replaced) {
        if (!index) {
            index = temporaries_allocator.allocate();
        }
        return expr::temporary{.index = *index, .type = std::move(type), .replaced_expr = std::move(replaced)};
    };

    switch (reading) {
    case bm25_reading::score:
        return slot(info.score_temporary_index, float_type, call);
    case bm25_reading::rank:
        return slot(info.rank_temporary_index, int32_type, call);
    case bm25_reading::hit:
        return expr::tuple_constructor{
                .elements = {slot(info.score_temporary_index, float_type, std::nullopt),
                        slot(info.rank_temporary_index, int32_type, std::nullopt)},
                .type = functions::search_hit_type(),
        };
    }
    on_internal_error(fts_log, "unhandled BM25 reading");
}

std::optional<expr::expression> validate_bm25_where_restriction(const expr::binary_operator& binop,
        const bm25_ordering_info& ordering_info) {
    // Whichever of the family the user wrote, preparation rewrote it to the reading a relation
    // compares, so this is the score.
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    if (!expr::is_native_function_call(fc, functions::BM25_SCORE_FUNCTION_NAME)) {
        throw exceptions::invalid_request_exception(seastar::format(
                "{}() is not supported in the WHERE clause",
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
            const auto reading = fc ? bm25_reading_of(*fc) : std::nullopt;
            if (!reading) {
                return std::nullopt;
            }

            if (!ordering_info) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() is not supported in the SELECT clause without matching ORDER BY and WHERE clauses", reading->name));
            }

            // Every call to the family reads the one search the rows are ranked by, so they all
            // have to name the column and the search term the other two clauses do.
            auto [col, sel_term] = external_search::extract_call_arguments(*fc, reading->name);
            if (col->name_as_text() != ordering_info->index.target_column()) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() in SELECT must reference the same column as BM25() in WHERE and ORDER BY", reading->name));
            }

            if (auto deferred = external_search::check_query_value(sel_term, ordering_info->search_term,
                        seastar::format("{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", reading->name))) {
                // Lifted out of the selector tree, so nothing else registers its bind marker.
                expr::fill_prepare_context(*deferred, ctx);
                ordering_info->deferred_select_terms.push_back(std::move(*deferred));
            }

            named_the_pair |= reading->reading == bm25_reading::hit;
            return deliver_bm25_reading(reading->reading, candidate, *ordering_info, temporaries_allocator);
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

    // A slot was allocated, so the search's answer about each row is selected and a provider will
    // fill that slot by matching the row to the index's response by primary key.
    if (ordering_info->score_temporary_index || ordering_info->rank_temporary_index) {
        external_search::fetch_primary_key_columns(*selection, *schema);
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
        if (expr::evaluate(sel_term, options) != search_term_val) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(
                    "BM25() in SELECT must use the same search term as BM25() in WHERE and ORDER BY"));
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

    const auto score_slot = _bm25_ordering_info.score_temporary_index;
    const auto rank_slot = _bm25_ordering_info.rank_temporary_index;
    auto provider = (score_slot || rank_slot)
                            ? std::make_unique<external_score_provider>(pkeys.value(), score_slot, rank_slot, *_schema)
                            : nullptr;
    co_return co_await query_base_table(qp, state, options, pkeys.value(), timeout, std::move(provider));
}

} // namespace cql3::statements
