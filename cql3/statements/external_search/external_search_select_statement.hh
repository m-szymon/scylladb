/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "cql3/statements/external_search/external_search_plan.hh"
#include "cql3/statements/external_search/filter.hh"

#include <vector>

namespace cql3::statements {

/// A SELECT answered by one or more external searches.
///
/// Every such query has the same shape whatever it is searching and however many searches it runs:
/// ask each index, gather the rows they between them named, and report what each search said about
/// each row. What differs by kind - what a query value means, how the request is made, whether an
/// excerpt can be asked for afterwards - is small enough to live behind one interface, and keeping
/// it there is what lets a query run an ANN search and a full-text search at once.
class external_search_select_statement : public external_index_select_statement {
    std::vector<search_source> _sources;
    /// The restrictions a vector search can prefilter by, serialized for the index.  Only a query
    /// running that one search has any: a full-text index takes none, and a query running several
    /// searches would have to agree them across indexes that do not filter alike.
    external_search::prepared_filter _prepared_filter;

public:
    /// The most candidates an index will be asked for.  Both kinds of search have always capped a
    /// query at this, and it is what bounds the base-table read that follows.
    static constexpr uint64_t max_query_limit = 1000;

    /// How many more candidates than the limit each search of a hybrid query is asked for.
    ///
    /// Fusing is only worth anything when the searches disagree, and they can only disagree about
    /// rows they both returned: asked for exactly the limit, two searches that agree on nothing
    /// hand back two disjoint lists and the fusion is a concatenation. A placeholder - the right
    /// factor depends on the query, and choosing it should be the query's to make.
    static constexpr uint64_t hybrid_candidate_factor = 4;

    static ::shared_ptr<select_statement> prepare(
            data_dictionary::database db, std::vector<search_source> sources, external_statement_args args);

    external_search_select_statement(std::vector<search_source> sources, external_search::prepared_filter prepared_filter,
            external_statement_args args);

private:
    future<::shared_ptr<cql_transport::messages::result_message>> execute_search(
            query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const override;

    std::string_view index_search_type_name() const override;
};

} // namespace cql3::statements
