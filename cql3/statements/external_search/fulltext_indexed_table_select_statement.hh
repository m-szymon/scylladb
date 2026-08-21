/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "external_index_select_statement.hh"
#include "cql3/expr/temporary_allocator.hh"

#include <optional>

namespace cql3::statements {

/// A SELECT occurrence's search term that prepare could not tell apart from the ORDER BY term,
/// together with the name of the function it was written in, so that execution can name that
/// function when the two turn out to differ.
struct deferred_select_term {
    expr::expression term;
    std::string_view function_name;
};

struct bm25_ordering_info {
    secondary_index::index index;
    expr::expression search_term;
    // Temporary slots the values a full-text search reports are delivered in, allocated on the
    // first SELECT occurrence naming each and filled per row by external_search_provider.  One slot
    // per value serves every occurrence of it, since all of them are required to name the same
    // search.  The score and the rank come from the search's answer; the fragment from a second
    // request made once the rows have been read.
    std::optional<size_t> score_temporary_index;
    std::optional<size_t> rank_temporary_index;
    std::optional<size_t> highlight_temporary_index;
    // The column the fragment is generated from, recorded when a fragment is selected: its text has
    // to be read from every row and sent to the index.  Always the column the rows are ranked by.
    const column_definition* highlighted_column = nullptr;
    // The SELECT occurrences' search terms that only execution can compare, a bind marker standing
    // where at least one of the two values will be, each with the function it was written in so that
    // execution can name it.
    std::vector<deferred_select_term> deferred_select_terms;
    // The WHERE clause's term, likewise.
    std::optional<expr::expression> deferred_where_term;
};

class fulltext_indexed_table_select_statement : public external_index_select_statement {
    bm25_ordering_info _bm25_ordering_info;

public:
    static constexpr size_t max_fts_query_limit = 1000;
    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db,
            schema_ptr schema,
            uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            cql_stats& stats,
            std::optional<bm25_ordering_info> ordering_info,
            std::unique_ptr<cql3::attributes> attrs);

    fulltext_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit, cql_stats& stats, bm25_ordering_info ordering_info,
            std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Full-Text Search";
    }

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
