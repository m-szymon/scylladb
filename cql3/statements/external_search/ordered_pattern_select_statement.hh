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
/// A pattern index without `order_by` answers in no particular order and in one page, which the
/// shared external_index_select_statement does; this statement is chosen only for an ordered one.
class ordered_pattern_select_statement : public external_index_select_statement {
public:
    using like_order = vector_search::vector_store_client::like_order;
    using sort_bound = vector_search::vector_store_client::sort_bound;

private:
    secondary_index::index _index;
    const column_definition* _target_column;
    /// The LIKE pattern, unevaluated because it may be a bind marker. It goes to the index node as
    /// written, and the node decides whether it can serve it.
    expr::expression _pattern;
    /// The range restriction on the ordered column, if the query carried one. Kept unevaluated
    /// because a bound may be a bind marker; it is turned into typed bounds at execution.
    std::vector<expr::binary_operator> _sort_bounds;
    /// The direction the query's ORDER BY asked for, empty when it had none. It is sent to the
    /// index node, which walks its sort column that way; without one the node picks its default.
    std::optional<like_order> _order;
    /// Whether the index was created with verify_candidates = 'scylla': the node is asked not to
    /// check its candidates, and this statement checks the rows it reads instead.
    bool _verifies_candidates;

public:
    /// The largest LIMIT of an unpaged query, and the largest page of a paged one.
    static constexpr uint64_t max_query_limit = external_index_select_statement::max_query_limit;

    static ::shared_ptr<select_statement> prepare(secondary_index::index index, external_statement_args args);

    ordered_pattern_select_statement(secondary_index::index index, const column_definition* target_column, expr::expression pattern,
            std::vector<expr::binary_operator> sort_bounds, std::optional<like_order> order, bool verifies_candidates,
            external_statement_args args);

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

    /// How many times a paged query's page is topped up from the node's cursor after the check
    /// here left it short, before it is handed to the client as it is. A short page is a correct
    /// page -- the client asks for the next one -- so this only bounds the work a keyword with many
    /// false candidates can cost in one round trip. An unpaged query is topped up until full.
    static constexpr unsigned max_top_ups_per_page = 3;
};

} // namespace cql3::statements
