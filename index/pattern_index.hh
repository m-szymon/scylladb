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

#include <vector>

namespace secondary_index {

/// Serves `LIKE` on a text column from the external index.
class pattern_index : public external_index {
public:
    static constexpr std::string_view INDEX_TYPE_NAME = "pattern";
    static constexpr std::string_view SEARCH_TYPE_NAME = "Pattern Search";

    // These defaults are the Vector Store's; the two must agree because the node applies them to
    // an index created without the option.
    static constexpr unsigned default_min_gram = 1;
    static constexpr unsigned default_max_gram = 3;
    static constexpr unsigned max_gram_limit = 8;

    std::string_view index_type_name() const override {
        return INDEX_TYPE_NAME;
    }

    pattern_index() = default;
    ~pattern_index() override = default;
    std::optional<cql3::description> describe(const index_metadata& im, const schema& base_schema) const override;
    void validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
            const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service& fs,
            const data_dictionary::database& db) const override;
    static bool has_index(const schema& s) {
        return has_index_impl<pattern_index>(s);
    }
    static bool is_pattern_index_on_column(const index_metadata& im, const sstring& column);
    static void check_cdc_options(const schema& s) {
        check_cdc_options_impl<pattern_index>(s);
    }

    /// What an index answers in: its `order_by` option.
    struct order {
        /// No `order_by`: table order, the order a scan returns, which is what a filtered LIKE
        /// returns. Otherwise the declared columns, then table order.
        enum class kind { table, columns };
        struct column {
            sstring name;
            bool descending;
        };
        kind what = kind::table;
        std::vector<column> columns;
    };
    /// The most columns an index may order by; the index node's limit.
    static constexpr size_t max_order_columns = 4;
    /// Reads an `order_by` value: columns with optional ASC or DESC, separated by commas. Names
    /// follow CQL's rules: unquoted ones are lowercased, double-quoted ones taken as written. Throws invalid_request_exception for a value that does not parse.
    /// The index node reads the option with the same rules (`OrderBy` in its lib.rs).
    static order parse_order(std::string_view value);
    /// The order of an index, whose option was validated at CREATE INDEX.
    static order order_of(const index_metadata& im);

private:
    void check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const;
    void check_index_options(const cql3::statements::index_specific_prop_defs& properties) const;
    void check_order(const schema& schema, const cql3::statements::index_specific_prop_defs& properties) const;
};

std::unique_ptr<secondary_index::custom_index> pattern_index_factory();

} // namespace secondary_index
