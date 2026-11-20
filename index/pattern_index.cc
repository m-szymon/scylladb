/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/pattern_index.hh"
#include "index/index_option_utils.hh"
#include "index/secondary_index_manager.hh"
#include "dht/i_partitioner.hh"
#include "types/tuple.hh"
#include <boost/algorithm/string/case_conv.hpp>
#include <seastar/core/sstring.hh>
#include <limits>
#include <algorithm>
#include <cctype>

namespace secondary_index {

namespace {

const sstring min_gram_option = "min_gram";
const sstring max_gram_option = "max_gram";
const sstring order_by_option = "order_by";
const sstring segment_rows_option = "segment_rows";
const sstring rewrite_option = "rewrite";
const sstring rarest_grams_option = "rarest_grams";

/// Whether `value` equals `expected`, ignoring letter case: the enumerated options are validated
/// that way, so they have to be read that way too.
bool option_is(const sstring& value, std::string_view expected) {
    return std::ranges::equal(value, expected, [](char a, char b) {
        return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
    });
}

void validate_row_count_option(std::string_view index, const sstring& name, const sstring& value) {
    const bool digits = !value.empty() && value.size() <= 10 && std::ranges::all_of(value, [](char c) {
        return c >= '0' && c <= '9';
    });
    if (!digits || std::stoull(value) > std::numeric_limits<uint32_t>::max()) {
        throw exceptions::invalid_request_exception(
                format("Index {}: option '{}' must be a number of rows, or 0 for no cap, but is '{}'", index, name, value));
    }
}

void validate_gram_count_option(std::string_view index, const sstring& name, const sstring& value) {
    const bool digits = !value.empty() && value.size() <= 3 && std::ranges::all_of(value, [](char c) {
        return c >= '0' && c <= '9';
    });
    if (!digits) {
        throw exceptions::invalid_request_exception(
                format("Index {}: option '{}' must be a number of grams, or 0 for all of them, but is '{}'", index, name, value));
    }
}

const std::unordered_map<sstring, std::function<void(std::string_view, const sstring&, const sstring&)>> pattern_index_options = {
        // 'case_sensitive' set to false lowercases both the indexed values and the patterns.
        {"case_sensitive", std::bind_front(util::validate_enumerated_option, util::boolean_values)},
        // The length in characters of the shortest indexed substring. A shorter keyword cannot be answered.
        {min_gram_option, std::bind_front(util::validate_positive_option, pattern_index::max_gram_limit)},
        // The length in characters of the longest indexed substring. A keyword up to this length is a
        // single exact lookup; a longer one is answered from its substrings and verified.
        {max_gram_option, std::bind_front(util::validate_positive_option, pattern_index::max_gram_limit)},
        // What the index answers in: the columns it orders by; absent, table order.
        // Only the syntax is checked here; the columns are checked against the schema in
        // check_order().
        {order_by_option, [](std::string_view, const sstring&, const sstring& value) {
             pattern_index::parse_order(value);
         }},
        // The most rows the index node puts in one segment of an ordered index, which keeps segments
        // narrow in the sort column; 0 for no cap. Absent, the node picks it for an ordered index.
        {segment_rows_option, validate_row_count_option},
        // Whether the index node rewrites segments that span far more of the sort column than their
        // share of the rows. Absent, on wherever there is a cap.
        {rewrite_option, std::bind_front(util::validate_enumerated_option, util::boolean_values)},
        // How many of a long keyword's substrings the index node intersects, the rarest first; the
        // verification drops what the others would have. 4 by default, 0 for all of them. Read by
        // the index node only.
        {rarest_grams_option, validate_gram_count_option},
};

/// Reads an already validated positive integer option, or the default when absent.
unsigned gram_option(const index_options_map& options, const sstring& name, unsigned default_value) {
    auto it = options.find(name);
    return it == options.end() ? default_value : static_cast<unsigned>(std::stoul(it->second));
}

} // anonymous namespace

std::optional<cql3::description> pattern_index::describe(const index_metadata& im, const schema& base_schema) const {
    auto target = im.options().at(cql3::statements::index_target::target_option_name);
    auto target_column = cql3::statements::index_target::column_name_from_target_string(target);
    return describe_with_target(im, base_schema, cql3::util::maybe_quote(target_column));
}

void pattern_index::check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const {
    using cql3::statements::index_target;

    if (targets.size() != 1) {
        throw exceptions::invalid_request_exception("Pattern index must have exactly one target");
    }

    auto& target = targets[0];
    if (!std::holds_alternative<index_target::single_column>(target->value)) {
        throw exceptions::invalid_request_exception("Pattern index target must be a single column");
    }

    auto& column = std::get<index_target::single_column>(target->value);
    auto c_name = column->to_string();
    auto const* c_def = schema.get_column_definition(column->name());
    if (c_def == nullptr) {
        throw exceptions::invalid_request_exception(format("Column {} not found in schema", c_name));
    }

    // Only a regular column can be served:
    // - partition and clustering key columns: ScyllaDB keeps every WHERE condition on a key
    //   column, `LIKE` included, as a key restriction, and a `LIKE` served by a pattern index rejects key
    //   restrictions, so a `LIKE` on a key column could never reach the index,
    // - static columns: the Vector Store fetches each row by its full primary key, which
    //   ScyllaDB refuses when the only selected column is static.
    if (!c_def->is_regular()) {
        throw exceptions::invalid_request_exception(
                format("Pattern index is only supported on regular columns, but column {} is a {} column", c_name, to_sstring(c_def->kind)));
    }

    auto kind = c_def->type->get_kind();
    if (kind != abstract_type::kind::utf8 && kind != abstract_type::kind::ascii) {
        throw exceptions::invalid_request_exception(
                format("Pattern index is only supported on text, varchar, or ascii columns, but column {} has an incompatible type", c_name));
    }
}

void pattern_index::check_index_options(const cql3::statements::index_specific_prop_defs& properties) const {
    const auto& options = properties.get_raw_options();
    for (const auto& option : options) {
        auto it = pattern_index_options.find(option.first);
        if (it == pattern_index_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for pattern index", option.first));
        }
        it->second(index_type_name(), option.first, option.second);
    }

    // Each option is valid on its own; the pair must also make sense together.
    auto min_gram = gram_option(options, min_gram_option, default_min_gram);
    auto max_gram = gram_option(options, max_gram_option, default_max_gram);
    if (min_gram > max_gram) {
        throw exceptions::invalid_request_exception(
                format("Invalid options for pattern index: min_gram ({}) must not be greater than max_gram ({})", min_gram, max_gram));
    }
}

namespace {

/// Whether a primary-key column of this type can take part in table order on the index node: the
/// types whose order the node reproduces (`is_key_type_supported` in its pattern_index/order.rs).
bool is_table_order_key_type(const abstract_type& type) {
    switch (type.without_reversed().get_kind()) {
    case abstract_type::kind::byte:
    case abstract_type::kind::short_kind:
    case abstract_type::kind::int32:
    case abstract_type::kind::long_kind:
    case abstract_type::kind::varint:
    case abstract_type::kind::utf8:
    case abstract_type::kind::ascii:
    case abstract_type::kind::bytes:
    case abstract_type::kind::boolean:
    case abstract_type::kind::timestamp:
    case abstract_type::kind::simple_date:
    case abstract_type::kind::time:
    case abstract_type::kind::uuid:
    case abstract_type::kind::timeuuid:
    case abstract_type::kind::inet:
    case abstract_type::kind::float_kind:
    case abstract_type::kind::double_kind:
        return true;
    case abstract_type::kind::tuple: {
        const auto& tuple = static_cast<const tuple_type_impl&>(type.without_reversed());
        return std::ranges::all_of(tuple.all_types(), [](const data_type& element) {
            return is_table_order_key_type(*element);
        });
    }
    default:
        return false;
    }
}

} // anonymous namespace

pattern_index::order pattern_index::parse_order(std::string_view value) {
    auto fail = [&](std::string_view why) {
        return exceptions::invalid_request_exception(format("Pattern index option 'order_by' = '{}': {}", value, why));
    };
    struct item {
        std::string name;
        bool quoted;
        std::optional<bool> descending;
    };
    std::vector<item> items;
    size_t at = 0;
    auto skip_spaces = [&] {
        while (at < value.size() && std::isspace(static_cast<unsigned char>(value[at]))) {
            ++at;
        }
    };
    for (;;) {
        skip_spaces();
        item next{.quoted = false};
        if (at < value.size() && value[at] == '"') {
            next.quoted = true;
            ++at;
            for (;;) {
                if (at == value.size()) {
                    throw fail("an unterminated quoted name");
                }
                if (value[at] == '"') {
                    if (at + 1 < value.size() && value[at + 1] == '"') {
                        next.name += '"';
                        at += 2;
                        continue;
                    }
                    ++at;
                    break;
                }
                next.name += value[at++];
            }
            if (next.name.empty()) {
                throw fail("an empty column name");
            }
        } else {
            // Unquoted: CQL's identifier characters, ASCII only; anything else has to be quoted.
            while (at < value.size() && (std::isalnum(static_cast<unsigned char>(value[at])) || value[at] == '_')) {
                next.name += value[at++];
            }
            if (next.name.empty()) {
                throw fail("expected a column name");
            }
        }
        skip_spaces();
        std::string word;
        while (at < value.size() && std::isalpha(static_cast<unsigned char>(value[at]))) {
            word += value[at++];
        }
        if (option_is(sstring(word), "asc")) {
            next.descending = false;
        } else if (option_is(sstring(word), "desc")) {
            next.descending = true;
        } else if (!word.empty()) {
            throw fail(format("expected ASC, DESC or a comma after {}, got {}", next.name, word));
        }
        items.push_back(std::move(next));
        skip_spaces();
        if (at == value.size()) {
            break;
        }
        if (value[at] != ',') {
            throw fail(format("unexpected '{}'", value[at]));
        }
        ++at;
    }
    if (items.size() > max_order_columns) {
        throw fail(format("at most {} columns can be ordered by", max_order_columns));
    }
    order result{.what = order::kind::columns};
    for (auto& item : items) {
        auto name = sstring(item.quoted ? item.name : boost::to_lower_copy(item.name));
        if (std::ranges::any_of(result.columns, [&](const order::column& column) {
                return column.name == name;
            })) {
            throw fail(format("column {} is named twice", name));
        }
        result.columns.push_back(order::column{std::move(name), item.descending.value_or(false)});
    }
    return result;
}

pattern_index::order pattern_index::order_of(const index_metadata& im) {
    auto it = im.options().find(order_by_option);
    return it == im.options().end() ? order{} : parse_order(it->second);
}

void pattern_index::check_order(const schema& schema, const cql3::statements::index_specific_prop_defs& properties) const {
    const auto& options = properties.get_raw_options();
    auto it = options.find(order_by_option);
    auto order = it == options.end() ? pattern_index::order{} : parse_order(it->second);
    for (const auto& column : order.columns) {
        const auto& name = column.name;
        auto const* c_def = schema.get_column_definition(to_bytes(name));
        if (c_def == nullptr) {
            throw exceptions::invalid_request_exception(format("Pattern index orders by column {}, which is not in the table", name));
        }
        // The index node keeps an ordering value as a fixed-width integer, so only the types with
        // an order-preserving image in one can be ordered by. This is the node's rule
        // (`is_orderable` in its cql_types.rs) repeated so that a bad index is refused at CREATE
        // rather than later; a disagreement refuses an index, it cannot misorder one.
        switch (c_def->type->without_reversed().get_kind()) {
        case abstract_type::kind::byte:
        case abstract_type::kind::short_kind:
        case abstract_type::kind::int32:
        case abstract_type::kind::long_kind:
        case abstract_type::kind::counter:
        case abstract_type::kind::timestamp:
        case abstract_type::kind::time:
        case abstract_type::kind::date:
        case abstract_type::kind::simple_date:
            break;
        default:
            throw exceptions::invalid_request_exception(
                    format("Pattern index cannot order by column {}: only integer and time columns can be ordered by", name));
        }
        if (!c_def->is_regular() && !c_def->is_primary_key()) {
            throw exceptions::invalid_request_exception(format("Pattern index cannot order by column {}, which is a {} column", name, to_sstring(c_def->kind)));
        }
    }
    // Ties, and an index without order_by, are in table order, which the index node computes:
    // the token with Murmur3, and the key's order with its own copy of each type's comparator.
    if (!schema.get_partitioner().name().ends_with("Murmur3Partitioner")) {
        throw exceptions::invalid_request_exception(
                "Pattern index keeps table order, which needs the Murmur3 partitioner");
    }
    for (const auto& c_def : schema.primary_key_columns()) {
        if (!is_table_order_key_type(*c_def.type)) {
            throw exceptions::invalid_request_exception(format(
                    "Pattern index keeps table order, which it cannot for primary key column {} of type {}",
                    c_def.name_as_text(), c_def.type->without_reversed().as_cql3_type().to_string()));
        }
    }
}

void pattern_index::validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service&, const data_dictionary::database& db) const {
    check_uses_tablets(schema, db);
    check_target(schema, targets);
    check_cdc_options(schema);
    check_index_options(properties);
    check_order(schema, properties);
}

bool pattern_index::is_pattern_index_on_column(const index_metadata& im, const sstring& column) {
    auto target_it = im.options().find(cql3::statements::index_target::target_option_name);
    return target_it != im.options().end() && secondary_index_manager::is_custom_index<pattern_index>(im)
            && cql3::statements::index_target::column_name_from_target_string(target_it->second) == column;
}

std::unique_ptr<secondary_index::custom_index> pattern_index_factory() {
    return std::make_unique<pattern_index>();
}

} // namespace secondary_index
