/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "external_index_select_statement.hh"

#include <optional>

namespace cql3::statements {

/// `SELECT ... WHERE column LIKE '%keyword%' LIMIT n` on a column with a substring index: the
/// Vector Store answers the containment and the rows are then read from the base table.
class substring_indexed_table_select_statement : public external_index_select_statement {
    const column_definition* _target_column;
    unsigned _min_gram;
    // Exactly one of the two is set: the keyword a literal pattern yielded at prepare, or the
    // pattern that only execution can evaluate and check, a bind marker standing there.
    std::optional<sstring> _keyword;
    std::optional<expr::expression> _deferred_pattern;

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
            std::optional<sstring> keyword, std::optional<expr::expression> deferred_pattern,
            std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Substring Search";
    }

    /// The keyword to search for: the one settled at prepare, or the bound pattern's.
    sstring evaluate_keyword(const query_options& options) const;

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
