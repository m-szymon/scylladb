/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <expected>
#include <string_view>

#include <seastar/core/sstring.hh>

#include "vector_search/vector_store_client.hh"

namespace cql3::statements::external_search {

/// Why a LIKE pattern cannot be answered by a substring index, worded for the user.
struct contains_pattern_error {
    seastar::sstring message;
};

/// A LIKE pattern a substring index can answer, taken apart: the literal keyword and where the
/// pattern wants it -- anywhere in the value, at its start or at its end.
struct contains_pattern {
    seastar::sstring keyword;
    vector_search::vector_store_client::contains_kind kind;
};

/// Reads the keyword and its kind out of a LIKE pattern a substring index can answer: exactly
/// `%keyword%` (containment), `keyword%` (prefix) or `%keyword` (suffix), with a keyword that is at
/// least `min_gram` characters (Unicode code points, not bytes) long and contains neither of the LIKE
/// wildcards `%` and `_` nor the escape `\`. Any other pattern is left to the regular filtering path,
/// which is what the caller does with the error.
std::expected<contains_pattern, contains_pattern_error> parse_contains_pattern(std::string_view like_pattern, unsigned min_gram);

} // namespace cql3::statements::external_search
