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

std::expected<contains_pattern, contains_pattern_error> parse_contains_pattern(std::string_view like_pattern, unsigned min_gram) {
    using contains_kind = vector_search::vector_store_client::contains_kind;
    constexpr auto form_error = "A LIKE pattern served by a substring index must be of the form '%keyword%', 'keyword%' or '%keyword'";

    std::string_view keyword;
    contains_kind kind;
    const bool leading = !like_pattern.empty() && like_pattern.front() == '%';
    const bool trailing = !like_pattern.empty() && like_pattern.back() == '%';
    if (leading && trailing) {
        // "%%" is the shortest pattern with both wildcards, and its keyword is empty; a lone "%"
        // is its own front and back.
        if (like_pattern.size() < 3) {
            return std::unexpected{contains_pattern_error{form_error}};
        }
        keyword = like_pattern.substr(1, like_pattern.size() - 2);
        kind = contains_kind::containment;
    } else if (trailing) {
        keyword = like_pattern.substr(0, like_pattern.size() - 1);
        kind = contains_kind::prefix;
    } else if (leading) {
        keyword = like_pattern.substr(1);
        kind = contains_kind::suffix;
    } else {
        return std::unexpected{contains_pattern_error{form_error}};
    }
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
    return contains_pattern{seastar::sstring(keyword), kind};
}

} // namespace cql3::statements::external_search
