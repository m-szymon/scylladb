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
#include <algorithm>

namespace cql3::statements {

namespace {

logging::logger sslogger("substring_search");

sstring bytes_to_text(const bytes& b) {
    return sstring(reinterpret_cast<const char*>(b.data()), b.size());
}

/// Finds the single `column LIKE pattern` the query is made of, rejecting anything else the
/// restriction analysis let through.
/// The LIKE on the indexed column, and any range bounds on the ordered column.
struct substring_restrictions {
    expr::binary_operator like;
    std::vector<expr::binary_operator> sort_bounds;
};

substring_restrictions find_like_restriction(const restrictions::select_restrictions& select_restrictions, const secondary_index::index& index) {
    if (!select_restrictions.partition_key_restrictions_is_empty() || !restrictions::is_empty_restriction(select_restrictions.get_clustering_columns_restrictions())) {
        throw exceptions::invalid_request_exception("Substring search queries do not support additional WHERE restrictions");
    }
    const auto& non_pk = select_restrictions.get_non_pk_restriction();
    auto order_by = secondary_index::substring_index::order_by(index.metadata());
    // The indexed column carries the LIKE; the sort column, if the index has one, may carry a
    // range alongside it. Nothing else may be restricted -- an unsupported predicate would be left
    // to post-filtering, which these statements do not do, so it would be silently ignored rather
    // than applied. Which column is at fault is worth saying, so the rejection is left to the loop
    // over the columns below rather than made here on the count.
    auto like_entry = std::ranges::find_if(non_pk, [&](const auto& entry) {
        return entry.first->name_as_text() == index.target_column();
    });
    if (like_entry == non_pk.end()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Substring search queries must restrict the indexed column {}", index.target_column()));
    }
    const auto& [column, restriction] = *like_entry;
    // The analysis keeps each column's restrictions as a conjunction of its factors.
    auto factors = expr::boolean_factors(restriction);
    const auto* binop = factors.size() == 1 ? expr::as_if<expr::binary_operator>(&factors.front()) : nullptr;
    if (!binop || binop->op != expr::oper_t::LIKE) {
        throw exceptions::invalid_request_exception(
                seastar::format("Substring search queries support only a single LIKE restriction on the indexed column {}", index.target_column()));
    }
    std::vector<expr::binary_operator> sort_bounds;
    for (const auto& [other_column, other_restriction] : non_pk) {
        if (other_column == column) {
            continue;
        }
        if (!order_by) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "Substring search queries cannot restrict {}: only the indexed column {} may be restricted, because this index "
                    "was created without an 'order_by' option and so has no other column's values to restrict by",
                    other_column->name_as_text(), index.target_column()));
        }
        if (other_column->name_as_text() != *order_by) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "Substring search queries cannot restrict {}: only the indexed column {} and the ordered column {} may be restricted",
                    other_column->name_as_text(), index.target_column(), *order_by));
        }
        // Every factor on the sort column has to be a bound the index node can apply. A predicate
        // left here would be dropped, not applied, because these statements do no post-filtering.
        for (const auto& factor : expr::boolean_factors(other_restriction)) {
            const auto* bound = expr::as_if<expr::binary_operator>(&factor);
            if (!bound || !is_slice(bound->op)) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "Substring search queries support only range restrictions (<, <=, >, >=) on the ordered column {}",
                        other_column->name_as_text()));
            }
            sort_bounds.push_back(*bound);
        }
    }
    return substring_restrictions{*binop, std::move(sort_bounds)};
}

/// Checks an ORDER BY against the column the index was created to order by.
///
/// The coordinator's own ordering checks are skipped for a substring query (see the guard in
/// select_statement.cc), so this is the only thing standing between a user and a query that claims
/// an order the index cannot provide. An index with no sort column can serve no ORDER BY at all:
/// its results come back in whatever order the walk found them.
void validate_ordering(const schema& schema, const select_statement::parameters& parameters,
        const secondary_index::index& index) {
    const auto& orderings = parameters.orderings();
    if (orderings.empty()) {
        return;
    }
    if (orderings.size() != 1) {
        throw exceptions::invalid_request_exception("Substring search queries support ordering by a single column");
    }
    auto order_by = secondary_index::substring_index::order_by(index.metadata());
    if (!order_by) {
        throw exceptions::invalid_request_exception(
                "ORDER BY requires the substring index to have been created with an 'order_by' option");
    }
    const auto& [raw_column, ordering] = orderings.front();
    if (!raw_column) {
        throw exceptions::invalid_request_exception("Substring search queries cannot be ordered by a scoring function");
    }
    auto column = raw_column->prepare_column_identifier(schema);
    if (column->name() != to_bytes(*order_by)) {
        throw exceptions::invalid_request_exception(format(
                "Substring search queries can only be ordered by {}, the column the index was created with, not {}",
                *order_by, column->to_string()));
    }
    // The index node walks its sort column from the highest value down, so ascending order would
    // need it to walk the other way. Nothing here is inherently descending, but nothing takes a
    // direction either; rejecting is better than quietly answering in the wrong one.
    if (!std::holds_alternative<raw::select_statement::ordering>(ordering)
            || std::get<raw::select_statement::ordering>(ordering) != raw::select_statement::ordering::descending) {
        throw exceptions::invalid_request_exception("Substring search queries can only be ordered DESC");
    }
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

    validate_ordering(*schema, *parameters, index);

    auto found = find_like_restriction(*restrictions, index);
    const auto& like = found.like;
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
            std::move(found.sort_bounds),
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
        std::vector<expr::binary_operator> sort_bounds, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions,
              group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit,
              stats, index, std::move(attrs)}
    , _target_column{target_column}
    , _min_gram{min_gram}
    , _keyword{std::move(keyword)}
    , _deferred_pattern{std::move(deferred_pattern)}
    , _sort_bounds{std::move(sort_bounds)}
    , _ordered{secondary_index::substring_index::order_by(index.metadata()).has_value()} {
}

std::pair<std::optional<uint64_t>, std::optional<uint64_t>>
substring_indexed_table_select_statement::evaluate_sort_bounds(const query_options& options) const {
    std::optional<uint64_t> min;
    std::optional<uint64_t> max;
    for (const auto& bound : _sort_bounds) {
        auto value = expr::evaluate(bound.rhs, options);
        if (value.is_null()) {
            throw exceptions::invalid_request_exception("A range bound of a substring search query must not be null");
        }
        const auto* column = expr::as<expr::column_value>(bound.lhs).col;
        auto bytes = to_bytes(value.view());
        auto key = secondary_index::to_sort_key(*column->type, bytes);
        if (!key) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "Substring search queries cannot range over column {} of type {}", column->name_as_text(), column->type->name()));
        }
        // An exclusive bound is narrowed to the inclusive one next to it, because the index node's
        // range bounds are inclusive. Sort keys are integers, so the neighbour is exact.
        switch (bound.op) {
        case expr::oper_t::GT:
            min = std::max(min.value_or(0), *key == std::numeric_limits<uint64_t>::max() ? *key : *key + 1);
            break;
        case expr::oper_t::GTE:
            min = std::max(min.value_or(0), *key);
            break;
        case expr::oper_t::LT:
            max = std::min(max.value_or(std::numeric_limits<uint64_t>::max()), *key == 0 ? *key : *key - 1);
            break;
        case expr::oper_t::LTE:
            max = std::min(max.value_or(std::numeric_limits<uint64_t>::max()), *key);
            break;
        default:
            on_internal_error(sslogger, "a non-slice bound reached evaluate_sort_bounds");
        }
    }
    return {min, max};
}

std::pair<std::optional<uint64_t>, uint64_t>
substring_indexed_table_select_statement::resume_point(const query_options& options, uint64_t limit) const {
    auto state = options.get_paging_state();
    if (!_ordered || !state) {
        return {std::nullopt, limit};
    }
    // The state's remaining count is what the first page's LIMIT left over. It is trusted no
    // further than the statement's own LIMIT, which a client could otherwise exceed by carrying a
    // paging state from a query with a larger one.
    return {state->get_index_cursor(), std::min(limit, state->get_remaining())};
}

uint64_t substring_indexed_table_select_statement::page_size_for(const query_options& options, uint64_t remaining) const {
    auto page_size = options.get_page_size();
    if (!_ordered || page_size <= 0) {
        return remaining;
    }
    return std::min(static_cast<uint64_t>(page_size), remaining);
}

lw_shared_ptr<const service::pager::paging_state> substring_indexed_table_select_statement::next_page_state(
        const vector_search::vector_store_client::contains_page& page, uint64_t remaining) const {
    auto left = remaining - page.keys.size();
    if (!_ordered || !page.next_cursor || left == 0) {
        return nullptr;
    }
    // Only the cursor and the remaining count mean anything here: this statement reads the base
    // table by primary key rather than scanning it, so there is no partition to resume at and no
    // saved reader to name. The rest of the state is the neutral value for each field.
    return make_lw_shared<const service::pager::paging_state>(partition_key::make_empty(), std::nullopt,
            static_cast<uint32_t>(left), query_id::create_null_id(), service::pager::paging_state::replicas_per_token_range{},
            std::nullopt, 0, static_cast<uint32_t>(left >> 32), 0, bound_weight::equal, partition_region::partition_start,
            std::nullopt, *page.next_cursor);
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

    // Throws for a bound the index cannot range over, or a null one.
    auto [min_sort_key, max_sort_key] = evaluate_sort_bounds(options);

    // An ordered index walks its matches downwards and reports where it stopped, so a page can pick
    // up from there. Without one there is no defined position to resume from, and the query keeps
    // the stage-1 shape: one page holding the whole result, with the warning do_execute adds.
    auto [cursor, remaining] = resume_point(options, limit);
    auto page_limit = page_size_for(options, remaining);

    auto page = co_await qp.vector_store_client().contains(_schema->ks_name(), _index.metadata().name(), _schema,
            std::string(keyword), page_limit, cursor, min_sort_key, max_sort_key, aoe.abort_source());
    if (!page.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::contains_error_visitor{}, page.error())));
    }

    throwing_assert(page->keys.size() <= page_limit);

    // The index node returns the keys in the order it wants them read -- its own for an unordered
    // index, newest-first for one with a sort column -- and query_base_table preserves that order
    // on both of its paths, so no score provider and no re-sorting are needed here.
    co_return co_await query_base_table(
            qp, state, options, page->keys, timeout, nullptr, next_page_state(*page, remaining));
}

} // namespace cql3::statements
