/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/external_search/external_search_plan.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"
#include "schema/schema.hh"

#include <optional>

namespace cql3::restrictions {
class select_restrictions;
}

/// The parts of running a pattern search that are specific to it. Unlike the other searches, it is
/// introduced by the WHERE clause, a LIKE on a pattern-indexed column, rather than by ORDER BY, and
/// it is not combined with any other search.
namespace cql3::statements::pattern_search {

/// The pattern index the WHERE clause is served by, if index selection chose one. Rejects a LIKE on
/// a pattern-indexed column that index selection passed over, rather than leave it to filtering.
/// Index selection passes over the index when the LIKE has another restriction next to it, or when
/// `continues_paged_query`: a filtered LIKE paged before the index was created keeps its plan.
std::optional<secondary_index::index> index_for(data_dictionary::database db, const schema_ptr& schema,
        const restrictions::select_restrictions& restrictions, bool continues_paged_query);

/// The search the statement runs on `index`. Checks that the WHERE clause is the single LIKE the
/// index answers, and that the statement asks for no order, since the rows come back in none.
search_source source_for(secondary_index::index index, const schema& schema, const restrictions::select_restrictions& restrictions,
        bool has_ordering);

/// The pattern an evaluated query value holds.
sstring query_pattern(const cql3::raw_value& value);

} // namespace cql3::statements::pattern_search
