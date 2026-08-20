/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/selection/selection.hh"
#include "cql3/values.hh"
#include "vector_search/vector_store_client.hh"

#include <optional>
#include <utility>
#include <vector>

class schema;
class column_definition;

namespace query {
class result;
class partition_slice;
}

namespace cql3::statements {

/// What an external search has to say about each row of a base-table read, in the order the rows
/// will be emitted.
struct search_answers {
    /// Ready to hand to a provider: the relevance the index gave each row, under the slot that
    /// reports it.  Empty if the query did not ask for it.
    std::vector<std::pair<size_t, std::vector<cql3::raw_value>>> slots;
    /// The rows to leave out of the result set, because the search turned out to have no relevance
    /// for them: the index named a key whose row is no longer in the base table, or scored it with
    /// something that is not a number.  Only a query that reports the relevance drops such a row -
    /// one that does not is missing nothing - so this is empty when no slot was asked for.
    std::vector<bool> dropped;
    /// The text a fragment is to be generated from, one document per row - an empty one where the
    /// row has no text, so that the documents stay aligned with the rows.
    std::vector<sstring> documents;
};

/// Lines an external search's response up with the rows just read.
///
/// `results` is what the index answered.  Given a `score_slot`, every row is matched against it by
/// primary key and reported under that slot, and a row with no match is reported as one to drop.
/// `document_column`, when given, has its text collected from every row, for a second request asking
/// the index to mark the search's terms in it.
search_answers match_search_results(const query::result& rows, const query::partition_slice& slice, const schema& schema,
        const selection::selection& selection, const vector_search::vector_store_client::primary_keys& results,
        std::optional<size_t> score_slot, const column_definition* document_column);

/// The values an external search injects into the rows of its result set: one per slot per row,
/// computed by match_search_results() before the result set is built.  A pure lookup - it hands out
/// the values of each row in the order the rows are offered to it, and says which rows to leave out.
///
/// Single-use and tied to one response: it cannot be rewound or replayed, which is worth keeping in
/// mind when paging arrives.
class external_search_provider final : public cql3::selection::external_values_provider {
    /// The slots this provider fills, and for each of them the value of every row.
    std::vector<std::pair<size_t, std::vector<cql3::raw_value>>> _slots;
    /// The rows to leave out of the result set.
    std::vector<bool> _dropped;
    mutable size_t _next_row = 0;

public:
    external_search_provider(std::vector<std::pair<size_t, std::vector<cql3::raw_value>>> slots, std::vector<bool> dropped);

    bool try_fill(std::vector<cql3::raw_value>& temporaries) const override;
};

} // namespace cql3::statements
