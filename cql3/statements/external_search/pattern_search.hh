/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_search_plan.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "index/pattern_index.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "schema/schema.hh"

#include <optional>

namespace cql3::restrictions {
class select_restrictions;
}

/// The parts of running a pattern search that are specific to it. Unlike ANN and BM25, a pattern
/// search:
/// - is introduced by WHERE, a LIKE on a pattern-indexed column, rather than by ORDER BY,
/// - is never combined with another search.
namespace cql3::statements::pattern_search {

/// The pattern index on a column restricted by LIKE in the unprepared WHERE clause, if any.
std::optional<secondary_index::index> index_for(data_dictionary::database db, const schema_ptr& schema,
        const expr::expression& where_clause);

/// How a query's ORDER BY fits an index's order.
struct ordering_fit {
    /// Why it does not fit; empty when it does.
    sstring mismatch;
    /// The direction asked for the index's first ordering column; none when the query has no
    /// ORDER BY.
    std::optional<bool> first_descending;
};

/// Whether a query's ORDER BY fits an index's order, as an ORDER BY fits a table's clustering
/// columns:
/// - an index without order_by keeps table order, which is what a LIKE without ORDER BY returns,
///   so it serves only queries without ORDER BY;
/// - an index with order_by columns serves only queries that ORDER BY a prefix of them, in their
///   declared directions or all reversed.
ordering_fit fit_ordering(const schema& schema, const raw::select_statement::parameters& parameters,
        const secondary_index::pattern_index::order& order);

/// The pattern an evaluated query value holds.
sstring query_pattern(const cql3::raw_value& value);

} // namespace cql3::statements::pattern_search
