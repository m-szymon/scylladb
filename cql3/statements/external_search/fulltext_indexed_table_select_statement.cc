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
#include "keys/keys.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/exception.hh>

#include <algorithm>

namespace cql3::statements {

namespace {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
logging::logger flogger("fulltext_search");

/// One of the two values a full-text search reports, as a SELECT occurrence can ask for it.  Both
/// are read from the one search the rows are ranked by and validated by the one set of rules in
/// prepare_bm25_selectors(); this is everything that differs between them.
struct bm25_value {
    /// The function it is called by: what a call is matched against, and what a message about that
    /// call names.
    const functions::function_name& function;
    std::string_view name;
    /// The type of the temporary slot it is delivered in.  Must be the function's declared return
    /// type, since that is what the selector reading the slot was typed by.
    data_type slot_type;
    /// Where the index of that slot is kept, allocated on the value's first occurrence.
    std::optional<size_t> bm25_ordering_info::*slot;
    /// Where to record the column the value is computed from, for a value generated from the row's
    /// own text - which has to be read from every row.  Null for a value the index reports itself.
    const column_definition* bm25_ordering_info::*source_column;
};

/// The value the given call asks for, or std::nullopt when it is not a call to either of them.
///
/// The table is built per call rather than kept in a static: a data_type is thread_local, one
/// instance per shard, so a table shared between shards would hand every shard the types of
/// whichever one built it first.
std::optional<bm25_value> selected_bm25_value(const expr::function_call& fc) {
    const bm25_value values[] = {
            {functions::BM25_FUNCTION_NAME, "BM25", float_type, &bm25_ordering_info::score_temporary_index, nullptr},
            {functions::BM25_HIGHLIGHT_FUNCTION_NAME, "BM25_HIGHLIGHT", utf8_type, &bm25_ordering_info::highlight_temporary_index,
                    &bm25_ordering_info::highlighted_column},
    };
    auto it = std::ranges::find_if(values, [&fc](const bm25_value& value) { return expr::is_native_function_call(fc, value.function); });
    return it != std::ranges::end(values) ? std::optional<bm25_value>(*it) : std::nullopt;
}

std::optional<expr::expression> validate_bm25_where_restriction(const expr::binary_operator& binop,
        const bm25_ordering_info& ordering_info) {
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    if (expr::is_native_function_call(fc, functions::BM25_HIGHLIGHT_FUNCTION_NAME)) {
        // A fragment is generated from a row the search has already selected, so there is nothing
        // here to restrict by.
        throw exceptions::invalid_request_exception("BM25_HIGHLIGHT() is only supported in the SELECT clause");
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

/// Walks a base-table result collecting the text of one column per row.
///
/// It has to emit exactly the rows result_set_builder::visitor emits, in the same order, or the
/// fragments - which come back positionally - would attach to the wrong rows.  It does because it
/// is the same walk: the same merged result read with the same slice.  The one thing that does not
/// follow from that is repeated here, that visitor's rule for a partition holding nothing but a
/// static row.
class document_collector {
    const schema& _schema;
    const selection::selection& _selection;
    const column_definition& _column;
    size_t _column_index;
    std::vector<sstring>& _documents;

    clustering_key_prefix _clustering_key = clustering_key_prefix::make_empty();
    uint64_t _row_count = 0;

    void collect(const query::result_row_view& static_row, const query::result_row_view* row) {
        // A row whose text is null or absent still needs a document.  The index answers an empty
        // one with no fragment, which is what such a row should report anyway.
        auto value = _column.is_clustering_key() ? clustering_key_component() : expr::get_non_pk_values(_selection, static_row, row)[_column_index];
        _documents.push_back(value ? value_cast<sstring>(_column.type->deserialize(managed_bytes_view(*value))) : sstring());
    }

    std::optional<managed_bytes> clustering_key_component() const {
        auto components = _clustering_key.explode(_schema);
        if (components.size() <= _column.component_index()) {
            return std::nullopt;
        }
        return managed_bytes(components[_column.component_index()]);
    }

public:
    document_collector(const schema& schema, const selection::selection& selection, const column_definition& column, size_t column_index,
            std::vector<sstring>& documents)
        : _schema(schema)
        , _selection(selection)
        , _column(column)
        , _column_index(column_index)
        , _documents(documents) {
    }

    void accept_new_partition(const partition_key&, uint64_t row_count) {
        _row_count = row_count;
    }

    void accept_new_partition(uint64_t row_count) {
        _row_count = row_count;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) {
        _clustering_key = key;
        accept_new_row(static_row, row);
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        collect(static_row, &row);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_row_count == 0) {
            // The builder emits one row for a partition that holds only a static row, so there is a
            // document to collect for it too.
            _clustering_key = clustering_key_prefix::make_empty();
            collect(static_row, nullptr);
        }
    }
};

/// Where in the values get_non_pk_values() hands back the highlighted column sits.  That vector is
/// aligned with the selection's columns, which is also the order the slice asks the replicas for
/// them in, so the three agree by construction.
size_t column_index_in(const selection::selection& selection, const column_definition& column) {
    const auto& columns = selection.get_columns();
    auto it = std::ranges::find(columns, &column);
    if (it == columns.end()) {
        on_internal_error(flogger, seastar::format("column {} is to be highlighted but was not asked for", column.name_as_text()));
    }
    return std::distance(columns.begin(), it);
}

/// The text of `column` in every row of `result`, for the index to mark the matched terms in.  One
/// document per row, in the order the rows will be emitted in, and an empty one where the row has no
/// text: the index answers positionally, so a row passed over here would shift the fragment of every
/// row after it.
std::vector<sstring> collect_documents(const query::result& result, const query::partition_slice& slice, const selection::selection& selection,
        const column_definition& column, const schema& schema) {
    auto documents = std::vector<sstring>{};
    query::result_view::consume(result, slice, document_collector(schema, selection, column, column_index_in(selection, column), documents));
    return documents;
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

} // anonymous namespace

void prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator, prepare_context& ctx) {
    for (auto& ps : prepared_selectors) {
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
            auto& slot = info.*value->slot;
            if (!slot) {
                slot = temporaries_allocator.allocate();
            }
            if (value->source_column) {
                // Checked equal to the ranked column just above.
                info.*value->source_column = col;
            }

            return expr::expression(expr::temporary{
                    .index = *slot,
                    .type = value->slot_type,
                    .replaced_expr = candidate,
            });
        });
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

    // A score slot was allocated, so BM25() was selected and a provider will fill that slot per row
    // by matching each row to the full-text index's response.
    if (ordering_info->score_temporary_index) {
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

    // The fragment is generated from the row's own text, so it can only be asked for now that the
    // rows are read.
    auto highlights = vector_search::vector_store_client::highlights{};
    if (_bm25_ordering_info.highlight_temporary_index && read.rows) {
        highlights = co_await fetch_highlights(qp.vector_store_client(), _schema->ks_name(), _index.metadata().name(), search_term_text,
                collect_documents(*read.rows.value(), read.command->slice, *_selection, *_bm25_ordering_info.highlighted_column, *_schema),
                aoe.abort_source());
    }

    auto provider = _bm25_ordering_info.score_temporary_index || _bm25_ordering_info.highlight_temporary_index
                            ? std::make_unique<external_search_provider>(pkeys.value(), _bm25_ordering_info.score_temporary_index, *_schema,
                                      _bm25_ordering_info.highlight_temporary_index, std::move(highlights))
                            : nullptr;
    co_return co_await emit_result_set(std::move(read), options, provider.get());
}

} // namespace cql3::statements
