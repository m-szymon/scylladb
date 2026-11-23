/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "vector_search/vector_store_client.hh"

#include <optional>
#include <vector>

namespace cql3::statements {

/// A pattern search on an index created with an `order_by` column:
///
///     SELECT ... WHERE name LIKE '%kw%' [AND sort_column > ...] [ORDER BY sort_column DESC|ASC] [LIMIT n]
///
/// The index node walks its matches in the order of the sort column, restricted to the range, and
/// reports where it stopped, so a page resumes there: the position travels in the driver's paging
/// state, and a paged query needs no LIMIT. Rows are read from the base table in the node's order.
///
/// Every pattern index keeps an order -- its `order_by` columns, or table order without them --
/// so this statement serves every pattern search.
class ordered_pattern_select_statement : public external_index_select_statement {
public:
    using like_order = vector_search::vector_store_client::like_order;
    using sort_bound = vector_search::vector_store_client::sort_bound;

private:
    secondary_index::index _index;
    /// The LIKE pattern, unevaluated because it may be a bind marker. It goes to the index node as
    /// written, and the node decides whether it can serve it.
    expr::expression _pattern;
    /// The range restriction on the ordered column, if the query carried one. Kept unevaluated
    /// because a bound may be a bind marker; it is turned into typed bounds at execution.
    std::vector<expr::binary_operator> _sort_bounds;
    /// The direction the query's ORDER BY asked for, empty when it had none. It is sent to the
    /// index node, which walks its sort column that way; without one the node picks its default.
    std::optional<like_order> _order;

public:
    /// The largest LIMIT of an unpaged query, and the largest page of a paged one.
    static constexpr uint64_t max_query_limit = external_index_select_statement::max_query_limit;

    static ::shared_ptr<select_statement> prepare(secondary_index::index index, external_statement_args args);

    ordered_pattern_select_statement(secondary_index::index index, const column_definition* target_column, expr::expression pattern,
            std::vector<expr::binary_operator> sort_bounds, std::optional<like_order> order, external_statement_args args);

private:
    future<::shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp, service::query_state& state, const query_options& options) const override;

    future<::shared_ptr<cql_transport::messages::result_message>> execute_ordered(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const;

    sstring evaluate_pattern(const query_options& options) const;
    /// The range restriction as the bounds the index node takes: {min, max}, either may be empty,
    /// each the column's own value and whether it is included. Several bounds on one side
    /// collapse to the tightest, compared the way the column's type compares.
    std::pair<std::optional<sort_bound>, std::optional<sort_bound>> evaluate_sort_bounds(const query_options& options) const;
    /// Where this page starts: the cursor the previous page ended at, and how much of the LIMIT is
    /// still unspent. The first page starts at the beginning with the whole LIMIT. The cursor is
    /// the index node's, carried back uninterpreted.
    std::pair<std::optional<sstring>, uint64_t> resume_point(const query_options& options, uint64_t limit) const;
    /// How many keys to ask the index node for: the page size when the client set one, otherwise
    /// everything still owed.
    uint64_t page_size_for(const query_options& options, uint64_t remaining) const;
    /// The paging state to hand the client: the index node's cursor and what is left of the
    /// LIMIT, or nullptr when there is nothing more to read.
    lw_shared_ptr<const service::pager::paging_state> next_page_state(const std::optional<sstring>& cursor, uint64_t left) const;

    /// The part of the statement's timeout a paged query may spend topping a page up from the
    /// node's cursor before it hands the page to the client as it is: one in this many. The index
    /// node returns a short page, or an empty one, with a cursor when a request has used its
    /// budget, and a page is also short by the rows the table no longer has. A short page is a
    /// correct page -- the client asks for the next one -- so this bounds one round trip, and
    /// leaves the rest of the timeout to the reads of the rows. The rows of every round are read
    /// before the next is asked for, so the page is ready whenever the topping up stops. An
    /// unpaged query is topped up until full, or until the timeout fails it.
    static constexpr unsigned top_up_timeout_share = 2;
};

} // namespace cql3::statements
