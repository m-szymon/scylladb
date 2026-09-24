/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema.hh"

#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/index_target.hh"
#include "index/external_index.hh"

#include <optional>
#include <vector>

namespace secondary_index {

/// The integer a substring index orders a value of `type` by, or nothing when the type has no such
/// image.
///
/// **This must agree, bit for bit, with `to_sort_key` in the index node** (vector-store,
/// `cql_types.rs`): the node stores the key this produces a bound for, so a disagreement would
/// filter on one ordering and sort by another, dropping rows from the middle of a result rather
/// than raising an error. Signed values are biased by flipping the sign bit, because a plain cast
/// would sort every negative value above every positive one.
std::optional<uint64_t> to_sort_key(const abstract_type& type, bytes_view value);

/// An index answering `LIKE '%keyword%'` on a text column: the Vector Store indexes every
/// substring of a value between `min_gram` and `max_gram` characters, so containment is an exact
/// lookup rather than a scan.
class substring_index : public external_index {
public:
    static constexpr std::string_view INDEX_TYPE_NAME = "substring";
    static constexpr std::string_view SEARCH_TYPE_NAME = "Substring Search";

    // These defaults are the Vector Store's; the two must agree because the node applies them to
    // an index created without the option.
    static constexpr unsigned default_min_gram = 1;
    static constexpr unsigned default_max_gram = 3;
    static constexpr unsigned max_gram_limit = 8;

    std::string_view index_type_name() const override {
        return INDEX_TYPE_NAME;
    }

    substring_index() = default;
    ~substring_index() override = default;
    std::optional<cql3::description> describe(const index_metadata& im, const schema& base_schema) const override;
    void validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
            const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service& fs,
            const data_dictionary::database& db) const override;
    static bool has_index(const schema& s) {
        return has_index_impl<substring_index>(s);
    }
    static void check_cdc_options(const schema& s) {
        check_cdc_options_impl<substring_index>(s);
    }

    /// Whether the index described by `im` is a substring index.
    static bool is_substring_index(const index_metadata& im);
    /// The `min_gram` the index was created with, or the default.
    static unsigned min_gram(const index_metadata& im);
    /// The column the index orders its results by, or nothing when it was created without one and
    /// so answers in unspecified order.
    static std::optional<sstring> order_by(const index_metadata& im);

private:
    void check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const;
    void check_index_options(const cql3::statements::index_specific_prop_defs& properties) const;
    void check_order_by_column(const schema& schema, const cql3::statements::index_specific_prop_defs& properties) const;
};

std::unique_ptr<secondary_index::custom_index> substring_index_factory();

} // namespace secondary_index
