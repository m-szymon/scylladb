/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/substring_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/substring_pattern.hh"
#include "cql3/statements/external_search/filter.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/selection/selection.hh"
#include "query/query-result-reader.hh"
#include "transport/messages/result_message.hh"
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

/// Checks an ORDER BY against the column the index was created to order by, and returns the
/// direction it asked for -- empty when the query has none.
///
/// The coordinator's own ordering checks are skipped for a substring query (see the guard in
/// select_statement.cc), so this is the only thing standing between a user and a query that claims
/// an order the index cannot provide. An index with no sort column can serve no ORDER BY at all:
/// its results come back in whatever order the walk found them.
std::optional<vector_search::vector_store_client::contains_order> validate_ordering(const schema& schema,
        const select_statement::parameters& parameters, const secondary_index::index& index) {
    const auto& orderings = parameters.orderings();
    if (orderings.empty()) {
        return std::nullopt;
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
    // The index node walks its sort column either way; the direction is passed to it and the rows
    // come back in that order, so nothing here re-sorts them.
    const auto* direction = std::get_if<raw::select_statement::ordering>(&ordering);
    if (!direction) {
        throw exceptions::invalid_request_exception("Substring search queries can only be ordered ASC or DESC");
    }
    return *direction == raw::select_statement::ordering::ascending
            ? vector_search::vector_store_client::contains_order::ascending
            : vector_search::vector_store_client::contains_order::descending;
}

/// Drops the rows the index node nominated but that do not hold the pattern. The value is the
/// target column, fetched for this purpose (`add_column_for_post_processing`) whether or not the
/// query selects it; a row without one cannot match.
class pattern_filter {
    const external_search::contains_pattern& _pattern;
    int32_t _value_index;
    mutable uint64_t _dropped = 0;

public:
    pattern_filter(const external_search::contains_pattern& pattern, int32_t value_index)
        : _pattern(pattern)
        , _value_index(value_index) {
    }

    bool operator()(const selection::selection& selection, const std::vector<bytes>&, const std::vector<bytes>&,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        auto values = expr::get_non_pk_values(selection, static_row, row);
        const auto& value = values.at(_value_index);
        const bool matches = value && _pattern.matches(std::string_view(reinterpret_cast<const char*>(to_bytes(*value).data()), value->size()));
        if (!matches) {
            ++_dropped;
        }
        return matches;
    }
    void reset(const partition_key* = nullptr) {
    }
    uint64_t get_rows_dropped() const {
        return _dropped;
    }
};

} // anonymous namespace

::shared_ptr<cql3::statements::select_statement> substring_indexed_table_select_statement::prepare(data_dictionary::database db,
        schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        const secondary_index::index& index,
        std::unique_ptr<attributes> attrs) {

    // Whether a LIMIT is needed depends on how the query is executed -- paged or not -- which
    // only execution knows; execute_search checks it.

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Substring search queries do not support per-partition limits");
    }

    if (selection->is_aggregate() || !group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception("Substring search queries cannot be run with aggregation");
    }

    if (!restrictions->get_scoring_function_restrictions().empty()) {
        throw exceptions::invalid_request_exception("Substring search queries cannot be combined with scoring functions");
    }

    auto order = validate_ordering(*schema, *parameters, index);

    auto found = find_like_restriction(*restrictions, index);
    const auto& like = found.like;
    const auto* target_column = expr::as<expr::column_value>(like.lhs).col;
    const unsigned min_gram = secondary_index::substring_index::min_gram(index.metadata());
    const unsigned max_gram = secondary_index::substring_index::max_gram(index.metadata());
    // Checking here needs a case-sensitive index (its test is byte for byte) and an ordered one:
    // a page the check leaves short has to resume from the node's cursor, and only an ordered
    // index reports one. Otherwise the node keeps checking its own candidates.
    // 'verify_candidates' = 'index' keeps it on the node even then, to compare the two.
    const bool verifies_candidates = secondary_index::substring_index::case_sensitive(index.metadata())
            && secondary_index::substring_index::order_by(index.metadata()).has_value()
            && !secondary_index::substring_index::node_verifies_candidates(index.metadata());

    std::optional<external_search::contains_pattern> literal;
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
        literal = std::move(*parsed);
    } else {
        deferred_pattern = like.rhs;
    }

    // The value is read along with the row whenever this side may have to check it: a literal
    // pattern past max_gram, or a bound one, whose length only execution knows.
    if (verifies_candidates && (!literal || literal->framed_length() > max_gram)) {
        selection->add_column_for_post_processing(*target_column);
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
            max_gram,
            verifies_candidates,
            std::move(literal),
            std::move(deferred_pattern),
            std::move(found.sort_bounds),
            order,
            std::move(attrs));
}

substring_indexed_table_select_statement::substring_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::select_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        const secondary_index::index& index, const column_definition* target_column, unsigned min_gram, unsigned max_gram,
        bool verifies_candidates, std::optional<external_search::contains_pattern> pattern,
        std::optional<expr::expression> deferred_pattern, std::vector<expr::binary_operator> sort_bounds,
        std::optional<vector_search::vector_store_client::contains_order> order, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions,
              group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit,
              stats, index, std::move(attrs)}
    , _target_column{target_column}
    , _min_gram{min_gram}
    , _max_gram{max_gram}
    , _verifies_candidates{verifies_candidates}
    , _pattern{std::move(pattern)}
    , _deferred_pattern{std::move(deferred_pattern)}
    , _sort_bounds{std::move(sort_bounds)}
    , _ordered{secondary_index::substring_index::order_by(index.metadata()).has_value()}
    , _order{order} {
}

std::pair<std::optional<substring_indexed_table_select_statement::sort_bound>, std::optional<substring_indexed_table_select_statement::sort_bound>>
substring_indexed_table_select_statement::evaluate_sort_bounds(const query_options& options) const {
    // A bound as evaluated, before it is encoded: the value's bytes, so that two bounds on the
    // same side can be compared with the column's own comparator, and whether it includes them.
    struct raw_bound {
        cql3::raw_value value;
        bool inclusive;
    };
    std::optional<raw_bound> min;
    std::optional<raw_bound> max;
    const column_definition* column = nullptr;
    for (const auto& bound : _sort_bounds) {
        auto value = expr::evaluate(bound.rhs, options);
        if (value.is_null()) {
            throw exceptions::invalid_request_exception("A range bound of a substring search query must not be null");
        }
        column = expr::as<expr::column_value>(bound.lhs).col;
        const bool inclusive = bound.op == expr::oper_t::GTE || bound.op == expr::oper_t::LTE;
        const bool lower = bound.op == expr::oper_t::GT || bound.op == expr::oper_t::GTE;
        if (!lower && bound.op != expr::oper_t::LT && bound.op != expr::oper_t::LTE) {
            on_internal_error(sslogger, "a non-slice bound reached evaluate_sort_bounds");
        }
        // Of two bounds on one side the tighter wins: the larger lower bound, the smaller upper
        // one, and at an equal value the exclusive one.
        auto& side = lower ? min : max;
        if (!side) {
            side = raw_bound{std::move(value), inclusive};
            continue;
        }
        auto order = column->type->compare(to_bytes(value.view()), to_bytes(side->value.view()));
        const bool tighter = lower ? order > 0 : order < 0;
        if (tighter || (order == 0 && !inclusive)) {
            side = raw_bound{std::move(value), inclusive};
        }
    }
    auto encode = [&](std::optional<raw_bound>& bound) -> std::optional<sort_bound> {
        if (!bound) {
            return std::nullopt;
        }
        return sort_bound{external_search::value_to_json(column->type, bound->value), bound->inclusive};
    };
    return {encode(min), encode(max)};
}

std::pair<std::optional<sstring>, uint64_t>
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
    // A driver's default page is thousands of rows; a page is a bound, not a promise, so a
    // shorter one keeps a single round trip's base-table read to what one LIMIT may ask for.
    return std::min({static_cast<uint64_t>(page_size), remaining, max_substring_query_limit});
}

lw_shared_ptr<const service::pager::paging_state> substring_indexed_table_select_statement::next_page_state(
        const std::optional<sstring>& cursor, uint64_t left) const {
    if (!_ordered || !cursor || left == 0) {
        return nullptr;
    }
    // Only the cursor and the remaining count mean anything here: this statement reads the base
    // table by primary key rather than scanning it, so there is no partition to resume at and no
    // saved reader to name. The rest of the state is the neutral value for each field.
    return make_lw_shared<const service::pager::paging_state>(partition_key::make_empty(), std::nullopt,
            static_cast<uint32_t>(left), query_id::create_null_id(), service::pager::paging_state::replicas_per_token_range{},
            std::nullopt, 0, static_cast<uint32_t>(left >> 32), 0, bound_weight::equal, partition_region::partition_start,
            std::nullopt, *cursor);
}

bool substring_indexed_table_select_statement::verifies_here(const external_search::contains_pattern& pattern) const {
    return _verifies_candidates && pattern.framed_length() > _max_gram;
}

external_search::contains_pattern substring_indexed_table_select_statement::evaluate_pattern(const query_options& options) const {
    if (_pattern) {
        return *_pattern;
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

    // Every round trip has to cost a bounded amount. A paged query on an ordered index is
    // bounded by its page, which resumes from the node's cursor at a flat cost, so its LIMIT
    // only caps the total and may be anything or absent. Any other query returns its whole
    // result at once, so the LIMIT is that bound and is required.
    const bool bounded_by_page = _ordered && options.get_page_size() > 0;
    if (!bounded_by_page) {
        if (!_limit) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(_ordered
                    ? "Substring search queries require a LIMIT unless they are paged"
                    : "Substring search queries require a LIMIT: this index was created without an 'order_by' option, so its results "
                      "cannot be paged"));
        }
        if (limit > max_substring_query_limit) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(fmt::format(
                    "Substring search queries require a LIMIT that is not greater than {} unless they are paged on an index with an "
                    "'order_by' option. LIMIT was {}",
                    max_substring_query_limit, limit)));
        }
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    // Throws invalid_request_exception for a bound pattern the index does not serve; a throw in a
    // coroutine body becomes the exceptional future the caller expects.
    const auto pattern = evaluate_pattern(options);

    // Throws for a null bound. The values go to the index node as they are; it encodes them.
    auto [min_sort_value, max_sort_value] = evaluate_sort_bounds(options);

    // An ordered index walks its matches in the direction asked for and reports where it stopped,
    // so a page can pick up from there. Without one there is no defined position to resume from,
    // and the query keeps the stage-1 shape: one page holding the whole result, with the warning
    // do_execute adds.
    auto [cursor, remaining] = resume_point(options, limit);
    auto page_limit = page_size_for(options, remaining);
    const bool verify_here = verifies_here(pattern);
    // The value to check is in the selection only when prepare saw that it might be needed; a
    // pattern that needs it without it is a disagreement between prepare and execute.
    const int32_t value_index = verify_here ? _selection->index_of(*_target_column) : -1;
    if (verify_here && value_index < 0) {
        on_internal_error(sslogger, "substring search verifies a pattern but did not fetch the value to verify it on");
    }

    // The rows are read a node page at a time into one result set. The index node returns the
    // keys in the order it wants them read -- its own for an unordered index, the ORDER BY's
    // direction for one with a sort column -- and query_base_table preserves that order on both
    // of its paths, so no score provider and no re-sorting are needed. When this side does the
    // checking, a page can come back short; it is topped up from the node's cursor a bounded
    // number of times, and past that handed over short, which a paging client takes in its stride.
    auto command = prepare_command_for_base_query(qp, state, options, page_limit);
    cql3::selection::result_set_builder builder(*_selection, _query_start_time_point, &options);
    // The bounds are sent with every round, and a JSON value only moves.
    auto copy_of = [](const std::optional<sort_bound>& bound) -> std::optional<sort_bound> {
        if (!bound) {
            return std::nullopt;
        }
        return sort_bound{rjson::copy(bound->value), bound->inclusive};
    };
    uint64_t returned = 0;
    std::optional<sstring> next_cursor;
    for (unsigned round = 0;; ++round) {
        auto page = co_await qp.vector_store_client().contains(_schema->ks_name(), _index.metadata().name(), _schema,
                std::string(pattern.keyword), pattern.kind, page_limit - returned, std::move(cursor), _order, copy_of(min_sort_value),
                copy_of(max_sort_value), !verify_here, aoe.abort_source());
        if (!page.has_value()) {
            co_await coroutine::return_exception(
                    exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::contains_error_visitor{}, page.error())));
        }
        throwing_assert(page->keys.size() <= page_limit - returned);
        if (!page->verified && value_index < 0) {
            on_internal_error(sslogger, "the index node returned candidates for a pattern this statement expected it to verify");
        }

        auto rows = co_await query_base_table(qp, state, options, command, timeout, page->keys);
        if (!rows) {
            co_return ::make_shared<cql_transport::messages::result_message::exception>(std::move(rows).assume_error());
        }
        co_await builder.with_thread_if_needed([&] {
            using builder_t = cql3::selection::result_set_builder;
            if (page->verified) {
                query::result_view::consume(*rows.value(), command->slice,
                        builder_t::visitor<builder_t::nop_filter>(builder, *_query_schema, *_selection, builder_t::nop_filter()));
            } else {
                query::result_view::consume(*rows.value(), command->slice,
                        builder_t::visitor<pattern_filter>(builder, *_query_schema, *_selection, pattern_filter(pattern, value_index)));
            }
        });
        returned = builder.result_set_size();
        next_cursor = page->next_cursor;
        cursor = page->next_cursor;
        const bool short_page = returned < page_limit && returned < remaining;
        // A paged client resumes a short page from its cursor, so the top-ups there only save it
        // round trips and are bounded. An unpaged one ignores the cursor, so its one reply has to
        // hold everything the LIMIT asks for: it is topped up until full or out of candidates.
        const bool paged = options.get_page_size() > 0;
        if (!_ordered || !cursor || !short_page || (paged && round == max_top_ups_per_page)) {
            break;
        }
    }

    std::unique_ptr<cql3::result_set> result_set;
    co_await builder.with_thread_if_needed([&] {
        result_set = builder.build();
    });
    // The builder gave the result its own copy of the selection's metadata, so the paging state
    // goes on the result and nothing shared between executions is touched.
    if (auto next_page = next_page_state(next_cursor, remaining - returned)) {
        result_set->get_metadata().maybe_set_paging_state(std::move(next_page));
    } else {
        result_set->get_metadata().clear_paging_state();
    }
    update_stats_rows_read(result_set->size());
    co_return ::make_shared<cql_transport::messages::result_message::rows>(result(std::move(result_set)));
}

} // namespace cql3::statements
