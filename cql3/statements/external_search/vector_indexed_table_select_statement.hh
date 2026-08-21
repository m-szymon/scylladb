/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "cql3/statements/external_search/filter.hh"
#include "cql3/expr/temporary_allocator.hh"

#include <optional>

namespace cql3::statements {

/// ANN ordering metadata resolved during prepare.
struct ann_ordering_info {
    secondary_index::index index;
    raw::select_statement::prepared_ann_ordering_type prepared_ann_ordering;
    bool is_rescoring_enabled;
    /// Temporary slots the search's two readings are delivered in, allocated on the first SELECT
    /// occurrence naming each and filled per row by external_search_provider.  A rescoring index
    /// fills neither: it recomputes the score locally, and has no rank to report until the
    /// recomputed scores are ranked.
    std::optional<size_t> score_temporary_index;
    std::optional<size_t> rank_temporary_index;
    /// The SELECT occurrences' query vectors that only execution can compare, a bind marker
    /// standing where at least one of the two values will be.
    std::vector<expr::expression> deferred_select_vectors;
};

/// The similarity a rescored ANN search's score is computed with on the coordinator.  It reads the
/// fetched vector column and the query vector, so it needs nothing injected per row - which is also
/// what lets it be evaluated in the position a nested occurrence asks for.
expr::expression make_similarity_expression(const secondary_index::index& index,
        const raw::select_statement::prepared_ann_ordering_type& prepared_ann_ordering,
        data_dictionary::database db, const schema_ptr& schema);

class vector_indexed_table_select_statement : public external_index_select_statement {
    ann_ordering_info _ann_ordering_info;
    external_search::prepared_filter _prepared_filter;

public:
    static constexpr size_t max_ann_query_limit = 1000;

    static ::shared_ptr<cql3::statements::select_statement> prepare(data_dictionary::database db, schema_ptr schema, uint32_t bound_terms,
            lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<const restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
            ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit, cql_stats& stats, ann_ordering_info ordering_info, std::unique_ptr<cql3::attributes> attrs);

    vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
            ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
            ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
            std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit,
            cql_stats& stats, ann_ordering_info ordering_info, external_search::prepared_filter prepared_filter, std::unique_ptr<cql3::attributes> attrs);

private:
    std::string_view index_search_type_name() const override {
        return "Vector Search";
    }

    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;
};

} // namespace cql3::statements
