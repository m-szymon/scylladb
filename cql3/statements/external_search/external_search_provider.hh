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
#include <span>
#include <utility>
#include <vector>

class schema;
class column_definition;

namespace query {
class result;
class partition_slice;
}

namespace cql3::statements {

/// One search's answer, and where each of the things it says about a row is delivered.
struct search_answer_request {
    /// What the index answered: its candidate keys, in the order it ranked them.
    const vector_search::vector_store_client::primary_keys& results;
    /// The slots reporting what it said about a row.  A search the query only ranks by has none.
    std::optional<size_t> score_slot;
    std::optional<size_t> rank_slot;
    /// Non-null when an excerpt of this column's text is wanted, which the text has to be collected
    /// for.  Only a full-text search has one.
    const column_definition* document_column = nullptr;
};

/// What the searches have to say about each row of a base-table read, in the order the rows will be
/// emitted.
struct search_answers {
    /// Ready to hand to a provider: one entry per slot asked for, holding that slot's value for
    /// every row.
    std::vector<std::pair<size_t, std::vector<cql3::raw_value>>> slots;
    /// The rows to leave out of the result set, because no search turned out to have anything to
    /// say about them: every index that named the key has since lost the row, or scored it with
    /// something that is not a number.
    std::vector<bool> dropped;
    /// The text an excerpt is to be generated from, one document per row - an empty one where the
    /// row has no text, so that the documents stay aligned with the rows.  One entry per request,
    /// in the order the requests were given, empty for a request wanting no excerpt.
    std::vector<std::vector<sstring>> documents;
};

/// Lines what every search answered up with the rows just read.
///
/// Each row is looked up by primary key in each search's answer and what that search said about it
/// reported under that search's slots, with nulls where a search did not find it.  A row no search
/// found is reported as one to drop.
///
/// By key rather than by walking each answer in step with the rows: the rows can only be read in one
/// order, and with more than one search that order is at most one of theirs.
search_answers match_search_results(const query::result& rows, const query::partition_slice& slice, const schema& schema,
        const selection::selection& selection, std::span<const search_answer_request> requests);

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
