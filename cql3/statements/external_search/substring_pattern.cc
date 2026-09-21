/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/substring_pattern.hh"

#include <algorithm>

#include <seastar/core/format.hh>

namespace cql3::statements::external_search {

std::expected<seastar::sstring, contains_pattern_error> parse_contains_pattern(std::string_view like_pattern, unsigned min_gram) {
    // "%%" is the shortest pattern with both wildcards, and its keyword is empty.
    if (like_pattern.size() < 3 || like_pattern.front() != '%' || like_pattern.back() != '%') {
        return std::unexpected{contains_pattern_error{
                "A LIKE pattern served by a substring index must be of the form '%keyword%'"}};
    }
    const auto keyword = like_pattern.substr(1, like_pattern.size() - 2);
    // `\` escapes the next character in a LIKE pattern, so a keyword holding one is not literal either.
    if (keyword.find_first_of("%_\\") != std::string_view::npos) {
        return std::unexpected{contains_pattern_error{
                "The keyword of a LIKE pattern served by a substring index must not contain the wildcards '%' and '_' or the escape '\\'"}};
    }
    // In UTF-8 every code point starts with a byte that is not a continuation byte (10xxxxxx).
    const auto characters = static_cast<unsigned>(std::ranges::count_if(keyword, [](char c) {
        return (static_cast<unsigned char>(c) & 0xC0) != 0x80;
    }));
    if (characters < min_gram) {
        return std::unexpected{contains_pattern_error{seastar::format(
                "The keyword '{}' has {} characters, but the substring index only answers keywords of at least min_gram={} characters",
                keyword, characters, min_gram)}};
    }
    return seastar::sstring(keyword);
}

} // namespace cql3::statements::external_search
