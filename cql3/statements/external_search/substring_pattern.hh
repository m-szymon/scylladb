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

namespace cql3::statements::external_search {

/// Why a LIKE pattern cannot be answered by a substring index, worded for the user.
struct contains_pattern_error {
    seastar::sstring message;
};

/// Reads the keyword out of a LIKE pattern a substring index can answer: exactly `%keyword%`,
/// with a keyword that is at least `min_gram` characters (Unicode code points, not bytes) long and
/// contains neither of the LIKE wildcards `%` and `_` nor the escape `\`. Any other pattern is left to the regular
/// filtering path, which is what the caller does with the error.
std::expected<seastar::sstring, contains_pattern_error> parse_contains_pattern(std::string_view like_pattern, unsigned min_gram);

} // namespace cql3::statements::external_search
