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
#include "index/substring_index.hh"
#include "index/index_option_utils.hh"
#include "index/secondary_index_manager.hh"
#include <seastar/core/sstring.hh>

namespace secondary_index {

namespace {

const sstring min_gram_option = "min_gram";
const sstring max_gram_option = "max_gram";
const sstring case_sensitive_option = "case_sensitive";

const std::unordered_map<sstring, std::function<void(std::string_view, const sstring&, const sstring&)>> substring_index_options = {
        // The length in characters of the shortest indexed substring. A shorter keyword cannot be answered.
        {min_gram_option, std::bind_front(util::validate_positive_option, substring_index::max_gram_limit)},
        // The length in characters of the longest indexed substring. A keyword up to this length is a
        // single exact lookup; a longer one is answered from its substrings and verified.
        {max_gram_option, std::bind_front(util::validate_positive_option, substring_index::max_gram_limit)},
        // Whether matching is case-sensitive, as CQL LIKE is, or lowercases both the values and the keywords.
        {case_sensitive_option, std::bind_front(util::validate_enumerated_option, util::boolean_values)},
};

/// Reads an already validated positive integer option, or the default when absent.
unsigned gram_option(const index_options_map& options, const sstring& name, unsigned default_value) {
    auto it = options.find(name);
    return it == options.end() ? default_value : static_cast<unsigned>(std::stoul(it->second));
}

} // anonymous namespace

std::optional<cql3::description> substring_index::describe(const index_metadata& im, const schema& base_schema) const {
    auto target = im.options().at(cql3::statements::index_target::target_option_name);
    auto target_column = cql3::statements::index_target::column_name_from_target_string(target);
    return describe_with_target(im, base_schema, cql3::util::maybe_quote(target_column));
}

bool substring_index::is_substring_index(const index_metadata& im) {
    auto custom_class = secondary_index_manager::get_custom_class(im);
    return custom_class && dynamic_cast<substring_index*>(custom_class->get()) != nullptr;
}

unsigned substring_index::min_gram(const index_metadata& im) {
    return gram_option(im.options(), min_gram_option, default_min_gram);
}

void substring_index::check_target(const schema& schema, const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const {
    using cql3::statements::index_target;

    if (targets.size() != 1) {
        throw exceptions::invalid_request_exception("Substring index must have exactly one target column");
    }

    auto& target = targets[0];
    if (!std::holds_alternative<index_target::single_column>(target->value)) {
        throw exceptions::invalid_request_exception("Substring index target must be a single column");
    }

    auto& column = std::get<index_target::single_column>(target->value);
    auto c_name = column->to_string();
    auto const* c_def = schema.get_column_definition(column->name());
    if (c_def == nullptr) {
        throw exceptions::invalid_request_exception(format("Column {} not found in schema", c_name));
    }

    auto kind = c_def->type->get_kind();
    if (kind != abstract_type::kind::utf8 && kind != abstract_type::kind::ascii) {
        throw exceptions::invalid_request_exception(
                format("Substring index is only supported on text, varchar, or ascii columns, but column {} has an incompatible type", c_name));
    }

    // A LIKE on a primary-key or static column goes through the key-restriction analysis, which
    // this index does not take part in yet.
    if (!c_def->is_regular()) {
        throw exceptions::invalid_request_exception(
                format("Substring index is only supported on regular columns, but column {} is a {} column", c_name, to_sstring(c_def->kind)));
    }
}

void substring_index::check_index_options(const cql3::statements::index_specific_prop_defs& properties) const {
    const auto& options = properties.get_raw_options();
    for (const auto& option : options) {
        auto it = substring_index_options.find(option.first);
        if (it == substring_index_options.end()) {
            throw exceptions::invalid_request_exception(format("Unsupported option {} for substring index", option.first));
        }
        it->second(index_type_name(), option.first, option.second);
    }

    // Each option is valid on its own; the pair must also make sense together.
    auto min_gram = gram_option(options, min_gram_option, default_min_gram);
    auto max_gram = gram_option(options, max_gram_option, default_max_gram);
    if (min_gram > max_gram) {
        throw exceptions::invalid_request_exception(
                format("Invalid options for substring index: min_gram ({}) must not be greater than max_gram ({})", min_gram, max_gram));
    }
}

void substring_index::validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service&, const data_dictionary::database& db) const {
    check_uses_tablets(schema, db);
    check_target(schema, targets);
    check_cdc_options(schema);
    check_index_options(properties);
}

std::unique_ptr<secondary_index::custom_index> substring_index_factory() {
    return std::make_unique<substring_index>();
}

} // namespace secondary_index
