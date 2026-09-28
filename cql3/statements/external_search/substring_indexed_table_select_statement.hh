/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "external_index_select_statement.hh"
#include "cql3/statements/external_search/substring_pattern.hh"

#include <optional>

namespace cql3::statements {

/// `SELECT ... WHERE column LIKE '%keyword%' LIMIT n` (or `'keyword%'`, `'%keyword'`) on a column with a substring index: the
/// Vector Store answers the containment and the rows are then read from the base table.
class substring_indexed_table_select_statement : public external_index_select_statement {
    const column_definition* _target_column;
    unsigned _min_gram;
    // Exactly one of the two is set: the keyword and kind a literal pattern yielded at prepare, or
    // the pattern that only execution can evaluate and check, a bind marker standing there.
    std::optional<external_search::contains_pattern> _pattern;
    std::optional<expr::expression> _deferred_pattern;
    /// The range restriction on the ordered column, if the query carried one. Kept unevaluated
    /// because a bound may be a bind marker; it is turned into typed bounds at execution.
    std::vector<expr::binary_operator> _sort_bounds;
    /// Whether the index was created with a sort column. Only such an index walks its matches in a
    /// defined order, and only then is there a position for a later page to resume from.
    bool _ordered;
    /// The direction the query's ORDER BY asked for, empty when it had none. It is sent to the
    /// index node, which walks its sort column that way; without one the node picks its default.
    std::optional<vector_search::vector_store_client::contains_order> _order;

public:
    static constexpr size_t max_substring_query_limit = 1000;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db,
            schema_ptr schema,
            uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::select_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            cql_stats& stats,
            const secondary_index::index& index,
            std::unique_ptr<cql3::attributes> attrs);

    substring_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit, cql_stats& stats,
            const secondary_index::index& index, const column_definition* target_column, unsigned min_gram,
            std::optional<external_search::contains_pattern> pattern, std::optional<expr::expression> deferred_pattern,
            std::vector<expr::binary_operator> sort_bounds, std::optional<vector_search::vector_store_client::contains_order> order,
            std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Substring Search";
    }

    bool supports_cursor_paging() const override {
        return _ordered;
    }

    /// The keyword to search for and where it has to sit: settled at prepare, or the bound pattern's.
    external_search::contains_pattern evaluate_pattern(const query_options& options) const;
    /// The range restriction as the bounds the index node takes: {min, max}, either may be empty,
    /// each the column's own value and whether it is included. Several bounds on one side
    /// collapse to the tightest, compared the way the column's type compares.
    using sort_bound = vector_search::vector_store_client::sort_bound;
    std::pair<std::optional<sort_bound>, std::optional<sort_bound>> evaluate_sort_bounds(const query_options& options) const;

    /// Where this page starts: the cursor the previous page ended at, and how much of the LIMIT is
    /// still unspent. The first page of a query, and every page of an unordered index, starts at
    /// the beginning with the whole LIMIT. The cursor is the index node's, carried back uninterpreted.
    std::pair<std::optional<sstring>, uint64_t> resume_point(const query_options& options, uint64_t limit) const;
    /// How many keys to ask the index node for: the page size when the client set one and this
    /// index can page, otherwise everything still owed.
    uint64_t page_size_for(const query_options& options, uint64_t remaining) const;
    /// The paging state to hand the client, or nullptr when this page is the last one.
    lw_shared_ptr<const service::pager::paging_state> next_page_state(
            const vector_search::vector_store_client::contains_page& page, uint64_t remaining) const;

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
