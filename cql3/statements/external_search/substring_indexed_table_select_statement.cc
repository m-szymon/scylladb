/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/substring_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/substring_pattern.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/substring_index.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/exception.hh>

namespace cql3::statements {

namespace {

logging::logger sslogger("substring_search");

sstring bytes_to_text(const bytes& b) {
    return sstring(reinterpret_cast<const char*>(b.data()), b.size());
}

/// Finds the single `column LIKE pattern` the query is made of, rejecting anything else the
/// restriction analysis let through.
expr::binary_operator find_like_restriction(const restrictions::select_restrictions& select_restrictions, const secondary_index::index& index) {
    if (!select_restrictions.partition_key_restrictions_is_empty() || !restrictions::is_empty_restriction(select_restrictions.get_clustering_columns_restrictions())) {
        throw exceptions::invalid_request_exception("Substring search queries do not support additional WHERE restrictions");
    }
    const auto& non_pk = select_restrictions.get_non_pk_restriction();
    if (non_pk.size() != 1) {
        throw exceptions::invalid_request_exception(
                "Substring search queries support exactly one LIKE restriction, on the indexed column, and no other WHERE restrictions");
    }
    const auto& [column, restriction] = *non_pk.begin();
    if (column->name_as_text() != index.target_column()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Substring search queries must restrict the indexed column {}", index.target_column()));
    }
    // The analysis keeps each column's restrictions as a conjunction of its factors.
    auto factors = expr::boolean_factors(restriction);
    const auto* binop = factors.size() == 1 ? expr::as_if<expr::binary_operator>(&factors.front()) : nullptr;
    if (!binop || binop->op != expr::oper_t::LIKE) {
        throw exceptions::invalid_request_exception(
                seastar::format("Substring search queries support only a single LIKE restriction on the indexed column {}", index.target_column()));
    }
    return *binop;
}

} // anonymous namespace

::shared_ptr<cql3::statements::select_statement> substring_indexed_table_select_statement::prepare(data_dictionary::database db,
        schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        const secondary_index::index& index,
        std::unique_ptr<attributes> attrs) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Substring search queries require a LIMIT");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Substring search queries do not support per-partition limits");
    }

    if (selection->is_aggregate() || !group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception("Substring search queries cannot be run with aggregation");
    }

    if (!restrictions->get_scoring_function_restrictions().empty()) {
        throw exceptions::invalid_request_exception("Substring search queries cannot be combined with scoring functions");
    }

    const auto like = find_like_restriction(*restrictions, index);
    const auto* target_column = expr::as<expr::column_value>(like.lhs).col;
    const unsigned min_gram = secondary_index::substring_index::min_gram(index.metadata());

    std::optional<sstring> keyword;
    std::optional<expr::expression> deferred_pattern;
    if (const auto* pattern = expr::as_if<expr::constant>(&like.rhs)) {
        if (pattern->is_null()) {
            throw exceptions::invalid_request_exception("The LIKE pattern of a substring search query must not be null");
        }
        // The index was chosen for this query because it can answer the literal pattern, so the
        // parse cannot fail here; a failure means the two checks disagree.
        auto parsed = external_search::parse_contains_pattern(pattern->view().deserialize<sstring>(*pattern->type), min_gram);
        if (!parsed) {
            on_internal_error(sslogger,
                    seastar::format("substring index chosen for a LIKE pattern it cannot answer: {}", parsed.error().message));
        }
        keyword = std::move(*parsed);
    } else {
        deferred_pattern = like.rhs;
    }

    return ::make_shared<cql3::statements::substring_indexed_table_select_statement>(
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
            index,
            target_column,
            min_gram,
            std::move(keyword),
            std::move(deferred_pattern),
            std::move(attrs));
}

substring_indexed_table_select_statement::substring_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::select_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        const secondary_index::index& index, const column_definition* target_column, unsigned min_gram,
        std::optional<sstring> keyword, std::optional<expr::expression> deferred_pattern,
        std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions,
              group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit,
              stats, index, std::move(attrs)}
    , _target_column{target_column}
    , _min_gram{min_gram}
    , _keyword{std::move(keyword)}
    , _deferred_pattern{std::move(deferred_pattern)} {
}

sstring substring_indexed_table_select_statement::evaluate_keyword(const query_options& options) const {
    if (_keyword) {
        return *_keyword;
    }
    auto pattern = expr::evaluate(*_deferred_pattern, options);
    if (pattern.is_null()) {
        throw exceptions::invalid_request_exception("The LIKE pattern of a substring search query must not be null");
    }
    auto parsed = external_search::parse_contains_pattern(bytes_to_text(std::move(pattern).to_bytes()), _min_gram);
    if (!parsed) {
        throw exceptions::invalid_request_exception(seastar::format(
                "{}. Only such patterns are served by the substring index on column {}; other LIKE patterns need ALLOW FILTERING on a column without one",
                parsed.error().message, _target_column->name_as_text()));
    }
    return std::move(*parsed);
}

future<shared_ptr<cql_transport::messages::result_message>> substring_indexed_table_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_substring_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Substring search queries require a LIMIT that is not greater than {}. LIMIT was {}", max_substring_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    // Throws invalid_request_exception for a bound pattern the index does not serve; a throw in a
    // coroutine body becomes the exceptional future the caller expects.
    const auto keyword = evaluate_keyword(options);

    auto pkeys = co_await qp.vector_store_client().contains(
            _schema->ks_name(), _index.metadata().name(), _schema, std::string(keyword), limit, aoe.abort_source());
    if (!pkeys.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::contains_error_visitor{}, pkeys.error())));
    }

    throwing_assert(pkeys->size() <= limit);

    // Nothing ranks the rows, so no score provider: they come back in whatever order the base
    // table read yields, as with any other LIKE.
    co_return co_await query_base_table(qp, state, options, pkeys.value(), timeout);
}

} // namespace cql3::statements
