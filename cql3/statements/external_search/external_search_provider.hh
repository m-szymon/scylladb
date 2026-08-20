/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/selection/selection.hh"
#include "vector_search/vector_store_client.hh"

#include <optional>

class schema;

namespace cql3::statements {

/// The per-row values one external search's response injects into a result row.
///
/// Given both of them: the score comes with the ranked keys, and the fragment - which the index
/// generates from text the coordinator sent it after reading the rows - has been asked for by the
/// time this is built.  So nothing here fetches anything; it hands out what it was given.
///
/// One provider owns both because they are matched to a row differently and the two matchings are
/// coupled.  The score is matched against the response by primary key, and its cursor only moves
/// forward: base-table results are merged in external search primary-key order, so a row can only
/// ever match at or after the current position, and entries stepped over are keys the index still
/// knows about but that are no longer in the base table.  Stepping over one drops that row - and
/// the fragment collected for it has to be consumed with it, because fragments are matched by
/// position: the index answers with fragments alone, so there is no key on either side to match on.
///
/// A provider instance is therefore single-use and tied to one response - it cannot be rewound or
/// replayed, which is worth keeping in mind when paging arrives.
class external_search_provider : public cql3::selection::external_values_provider {
    const vector_search::vector_store_client::primary_keys& _results;
    mutable size_t _next_result;    // cursor into _results: which entry to match next
    const std::optional<size_t> _score_slot;
    const schema& _schema;

    // One entry per fetched row, in the order the rows are emitted; empty unless a fragment is
    // wanted.  Absent where the index found nothing worth marking in that row's text.
    const vector_search::vector_store_client::highlights _highlights;
    const std::optional<size_t> _highlight_slot;
    mutable size_t _next_row;       // index into _highlights: which row is being filled

public:
    external_search_provider(const vector_search::vector_store_client::primary_keys& results, std::optional<size_t> score_slot, const schema& schema,
            std::optional<size_t> highlight_slot = std::nullopt, vector_search::vector_store_client::highlights highlights = {});

    bool try_fill(std::vector<cql3::raw_value>& temporaries, std::span<const bytes> partition_key, std::span<const bytes> clustering_key,
            const query::result_row_view& static_row, const query::result_row_view* row) const override;
};

} // namespace cql3::statements
