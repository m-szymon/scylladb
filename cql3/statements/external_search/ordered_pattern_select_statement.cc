/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/ordered_pattern_select_statement.hh"
#include "cql3/statements/external_search/filter.hh"
#include "cql3/statements/external_search/pattern_search.hh"
#include "cql3/statements/index_latency.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/selection/selection.hh"
#include "index/pattern_index.hh"
#include "query/query-result-reader.hh"
#include "transport/messages/result_message.hh"
#include "exceptions/exceptions.hh"
#include "utils/assert.hh"
#include "utils/like_matcher.hh"
#include "utils/log.hh"

#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/exception.hh>
#include <algorithm>

namespace cql3::statements {

namespace {

logging::logger opslogger("ordered_pattern_search");

/// The LIKE on the indexed column, and any range bounds on the ordered column.
struct ordered_pattern_restrictions {
    expr::binary_operator like;
    std::vector<expr::binary_operator> sort_bounds;
};

ordered_pattern_restrictions find_restrictions(
        const restrictions::select_restrictions& select_restrictions, const secondary_index::index& index, const sstring& order_by) {
    if (!select_restrictions.partition_key_restrictions_is_empty() || !restrictions::is_empty_restriction(select_restrictions.get_clustering_columns_restrictions())) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support additional WHERE restrictions");
    }
    const auto& non_pk = select_restrictions.get_non_pk_restriction();
    // The indexed column carries the LIKE; the sort column may carry a range alongside it. Nothing
    // else may be restricted -- an unsupported predicate would be left to post-filtering, which these
    // statements do not do, so it would be silently ignored rather than applied.
    auto like_entry = std::ranges::find_if(non_pk, [&](const auto& entry) {
        return entry.first->name_as_text() == index.target_column();
    });
    if (like_entry == non_pk.end()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Pattern search queries must restrict the indexed column {}", index.target_column()));
    }
    const auto& [column, restriction] = *like_entry;
    // The analysis keeps each column's restrictions as a conjunction of its factors.
    auto factors = expr::boolean_factors(restriction);
    const auto* binop = factors.size() == 1 ? expr::as_if<expr::binary_operator>(&factors.front()) : nullptr;
    if (!binop || binop->op != expr::oper_t::LIKE) {
        throw exceptions::invalid_request_exception(
                seastar::format("Pattern search queries support only a single LIKE restriction on the indexed column {}", index.target_column()));
    }
    std::vector<expr::binary_operator> sort_bounds;
    for (const auto& [other_column, other_restriction] : non_pk) {
        if (other_column == column) {
            continue;
        }
        if (other_column->name_as_text() != order_by) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "Pattern search queries cannot restrict {}: only the indexed column {} and the ordered column {} may be restricted",
                    other_column->name_as_text(), index.target_column(), order_by));
        }
        // Every factor on the sort column has to be a bound the index node can apply.
        for (const auto& factor : expr::boolean_factors(other_restriction)) {
            const auto* bound = expr::as_if<expr::binary_operator>(&factor);
            if (!bound || !is_slice(bound->op)) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "Pattern search queries support only range restrictions (<, <=, >, >=) on the ordered column {}",
                        other_column->name_as_text()));
            }
            sort_bounds.push_back(*bound);
        }
    }
    return ordered_pattern_restrictions{*binop, std::move(sort_bounds)};
}

/// Checks an ORDER BY against the column the index was created to order by, and returns the
/// direction it asked for -- empty when the query has none.
///
/// The coordinator's own ordering checks are skipped for an external search, so this is the only
/// thing standing between a user and a query that claims an order the index cannot provide.
std::optional<ordered_pattern_select_statement::like_order> validate_ordering(
        const schema& schema, const raw::select_statement::parameters& parameters, const sstring& order_by) {
    const auto& orderings = parameters.orderings();
    if (orderings.empty()) {
        return std::nullopt;
    }
    if (orderings.size() != 1) {
        throw exceptions::invalid_request_exception("Pattern search queries support ordering by a single column");
    }
    const auto& [raw_column, ordering] = orderings.front();
    if (!raw_column) {
        throw exceptions::invalid_request_exception("Pattern search queries cannot be ordered by a scoring function");
    }
    auto column = raw_column->prepare_column_identifier(schema);
    if (column->name() != to_bytes(order_by)) {
        throw exceptions::invalid_request_exception(format(
                "Pattern search queries can only be ordered by {}, the column the index was created with, not {}", order_by, column->to_string()));
    }
    // The index node walks its sort column either way; the direction is passed to it and the rows
    // come back in that order, so nothing here re-sorts them.
    const auto* direction = std::get_if<raw::select_statement::ordering>(&ordering);
    if (!direction) {
        throw exceptions::invalid_request_exception("Pattern search queries can only be ordered ASC or DESC");
    }
    return *direction == raw::select_statement::ordering::ascending ? ordered_pattern_select_statement::like_order::ascending
                                                                     : ordered_pattern_select_statement::like_order::descending;
}

/// Drops the rows the index node nominated but that do not match the pattern, applying it the
/// way a filtered LIKE does. The value is the target column, fetched for this purpose
/// (`add_column_for_post_processing`) whether or not the query selects it; a row without one
/// cannot match.
class pattern_filter {
    const like_matcher& _matcher;
    int32_t _value_index;
    mutable uint64_t _dropped = 0;

public:
    pattern_filter(const like_matcher& matcher, int32_t value_index)
        : _matcher(matcher)
        , _value_index(value_index) {
    }

    bool operator()(const selection::selection& selection, const std::vector<bytes>&, const std::vector<bytes>&,
            const query::result_row_view& static_row, const query::result_row_view* row) const {
        auto values = expr::get_non_pk_values(selection, static_row, row);
        const auto& value = values.at(_value_index);
        const bool matches = value && _matcher(to_bytes(*value));
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

::shared_ptr<select_statement> ordered_pattern_select_statement::prepare(secondary_index::index index, external_statement_args args) {
    // Whether a LIMIT is needed depends on how the query is executed -- paged or not -- which only
    // execution knows; execute_ordered() checks it.
    if (args.per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support per-partition limits");
    }
    if (args.selection->is_aggregate() || !args.group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception("Pattern search queries cannot be run with aggregation");
    }
    if (!args.restrictions->get_scoring_function_restrictions().empty()) {
        throw exceptions::invalid_request_exception("Pattern search queries cannot be combined with ANN or BM25");
    }
    auto order_by = secondary_index::pattern_index::order_by(index.metadata());
    throwing_assert(order_by);
    auto order = validate_ordering(*args.schema, *args.parameters, *order_by);
    auto found = find_restrictions(*args.restrictions, index, *order_by);
    const auto* target_column = expr::as<expr::column_value>(found.like.lhs).col;

    // verify_candidates = 'scylla' is refused at CREATE INDEX unless the index is case-sensitive,
    // so its check is ScyllaDB's own LIKE, which needs the value read along with the row.
    const bool verifies_candidates = secondary_index::pattern_index::scylla_verifies_candidates(index.metadata());
    if (verifies_candidates) {
        args.selection->add_column_for_post_processing(*target_column);
    }

    return ::make_shared<ordered_pattern_select_statement>(std::move(index), target_column, std::move(found.like.rhs),
            std::move(found.sort_bounds), order, verifies_candidates, std::move(args));
}

ordered_pattern_select_statement::ordered_pattern_select_statement(secondary_index::index index, const column_definition* target_column,
        expr::expression pattern, std::vector<expr::binary_operator> sort_bounds, std::optional<like_order> order, bool verifies_candidates,
        external_statement_args args)
    : external_index_select_statement(
              std::vector{search_source{
                      .family = functions::search_family::pattern,
                      .index = index,
                      .column = target_column,
                      .query_value = pattern,
              }},
              external_search::prepared_filter{{}, args.parameters->allow_filtering()}, std::move(args))
    , _index(std::move(index))
    , _target_column(target_column)
    , _pattern(std::move(pattern))
    , _sort_bounds(std::move(sort_bounds))
    , _order(order)
    , _verifies_candidates(verifies_candidates) {
}

sstring ordered_pattern_select_statement::evaluate_pattern(const query_options& options) const {
    auto pattern = expr::evaluate(_pattern, options);
    if (pattern.is_null()) {
        throw exceptions::invalid_request_exception("The LIKE pattern of a pattern search query must not be null");
    }
    return pattern_search::query_pattern(pattern);
}

std::pair<std::optional<ordered_pattern_select_statement::sort_bound>, std::optional<ordered_pattern_select_statement::sort_bound>>
ordered_pattern_select_statement::evaluate_sort_bounds(const query_options& options) const {
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
            throw exceptions::invalid_request_exception("A range bound of a pattern search query must not be null");
        }
        column = expr::as<expr::column_value>(bound.lhs).col;
        const bool inclusive = bound.op == expr::oper_t::GTE || bound.op == expr::oper_t::LTE;
        const bool lower = bound.op == expr::oper_t::GT || bound.op == expr::oper_t::GTE;
        if (!lower && bound.op != expr::oper_t::LT && bound.op != expr::oper_t::LTE) {
            on_internal_error(opslogger, "a non-slice bound reached evaluate_sort_bounds");
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

std::pair<std::optional<sstring>, uint64_t> ordered_pattern_select_statement::resume_point(const query_options& options, uint64_t limit) const {
    auto state = options.get_paging_state();
    if (!state) {
        return {std::nullopt, limit};
    }
    // The state's remaining count is what the first page's LIMIT left over. It is trusted no
    // further than the statement's own LIMIT, which a client could otherwise exceed by carrying a
    // paging state from a query with a larger one.
    return {state->get_index_cursor(), std::min(limit, state->get_remaining())};
}

uint64_t ordered_pattern_select_statement::page_size_for(const query_options& options, uint64_t remaining) const {
    auto page_size = options.get_page_size();
    if (page_size <= 0) {
        return remaining;
    }
    // A driver's default page is thousands of rows; a page is a bound, not a promise, so a
    // shorter one keeps a single round trip's base-table read to what one LIMIT may ask for.
    return std::min({static_cast<uint64_t>(page_size), remaining, max_query_limit});
}

lw_shared_ptr<const service::pager::paging_state> ordered_pattern_select_statement::next_page_state(
        const std::optional<sstring>& cursor, uint64_t left) const {
    if (!cursor || left == 0) {
        return nullptr;
    }
    // Only the cursor and the remaining count mean anything here: this statement reads the base
    // table by primary key rather than scanning it, so there is no partition to resume at and no
    // saved reader to name. The rest of the state is the neutral value for each field.
    return make_lw_shared<const service::pager::paging_state>(partition_key::make_empty(), std::nullopt, static_cast<uint32_t>(left),
            query_id::create_null_id(), service::pager::paging_state::replicas_per_token_range{}, std::nullopt, 0,
            static_cast<uint32_t>(left >> 32), 0, bound_weight::equal, partition_region::partition_start, std::nullopt, *cursor);
}

future<::shared_ptr<cql_transport::messages::result_message>> ordered_pattern_select_statement::do_execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {
    auto limit = get_limit(options, _limit);
    co_return co_await measure_index_latency(*_schema, _index, [this, &qp, &state, &options, limit]() -> future<::shared_ptr<cql_transport::messages::result_message>> {
        setup_execute(state, options);
        co_return co_await execute_ordered(qp, state, options, limit);
    });
}

future<::shared_ptr<cql_transport::messages::result_message>> ordered_pattern_select_statement::execute_ordered(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {
    // Every round trip has to cost a bounded amount. A paged query is bounded by its page, which
    // resumes from the node's cursor at a flat cost, so its LIMIT only caps the total and may be
    // anything or absent. An unpaged query returns its whole result at once, so the LIMIT is that
    // bound and is required.
    if (options.get_page_size() <= 0) {
        if (!_limit) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception("Pattern search queries require a LIMIT unless they are paged"));
        }
        if (limit > max_query_limit) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(fmt::format(
                    "Pattern search queries require a LIMIT that is not greater than {} unless they are paged. LIMIT was {}", max_query_limit, limit)));
        }
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    const auto pattern = evaluate_pattern(options);
    // Throws for a null bound. The values go to the index node as they are; it encodes them.
    auto [min_sort_value, max_sort_value] = evaluate_sort_bounds(options);

    auto [cursor, remaining] = resume_point(options, limit);
    auto page_limit = page_size_for(options, remaining);
    // The value to check is in the selection only when prepare saw that it might be needed.
    const int32_t value_index = _verifies_candidates ? _selection->index_of(*_target_column) : -1;
    if (_verifies_candidates && value_index < 0) {
        on_internal_error(opslogger, "pattern search verifies candidates but did not fetch the value to verify them on");
    }
    // Compiled on the first page that needs checking here: with the index node verifying, none
    // does, and compiling a pattern costs a cheap query a sizeable share of its CPU.
    std::optional<like_matcher> matcher;

    // The rows are read a node page at a time into one result set. The index node returns the keys
    // in the order the ORDER BY asks for, and query_base_table preserves that order on both of its
    // paths, so no values provider and no re-sorting are needed. When this side does the checking a
    // page can come back short; it is topped up from the node's cursor a bounded number of times,
    // and past that handed over short, which a paging client takes in its stride.
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
        auto page = co_await qp.vector_store_client().like_page(_schema->ks_name(), _index.metadata().name(), _schema, pattern,
                page_limit - returned, std::move(cursor), _order, copy_of(min_sort_value), copy_of(max_sort_value), !_verifies_candidates,
                aoe.abort_source());
        if (!page.has_value()) {
            co_await coroutine::return_exception(
                    exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::like_error_visitor{}, page.error())));
        }
        // An index node that returns more than it was asked for is cut to what was asked.
        if (page->keys.size() > page_limit - returned) {
            page->keys.erase(page->keys.begin() + (page_limit - returned), page->keys.end());
        }
        if (!page->verified && value_index < 0) {
            // Only a request that declined verification may get candidates back. Taking them as
            // matches would return rows that do not match, so the reply fails the query instead.
            co_await coroutine::return_exception(exceptions::invalid_request_exception(
                    "Pattern search failed: the Vector Store returned unverified candidates although it was asked to verify them"));
        }

        std::vector<vector_search::search_candidate> candidates;
        candidates.reserve(page->keys.size());
        for (auto& key : page->keys) {
            candidates.push_back(vector_search::search_candidate{std::move(key.partition), std::move(key.clustering), {}});
        }
        auto read = co_await query_base_table(qp, state, options, timeout, candidates);
        if (!read) {
            co_return ::make_shared<cql_transport::messages::result_message::exception>(std::move(read).assume_error());
        }
        co_await builder.with_thread_if_needed([&] {
            using builder_t = cql3::selection::result_set_builder;
            const auto& rows = *read.value().rows;
            const auto& slice = read.value().command->slice;
            if (page->verified) {
                query::result_view::consume(rows, slice, builder_t::visitor<builder_t::nop_filter>(builder, *_query_schema, *_selection, builder_t::nop_filter()));
            } else {
                if (!matcher) {
                    matcher.emplace(bytes_view(reinterpret_cast<const int8_t*>(pattern.data()), pattern.size()));
                }
                query::result_view::consume(rows, slice, builder_t::visitor<pattern_filter>(builder, *_query_schema, *_selection, pattern_filter(*matcher, value_index)));
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
        if (!cursor || !short_page || (paged && round == max_top_ups_per_page)) {
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
