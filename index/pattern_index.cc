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
#include <seastar/core/sstring.hh>
#include <limits>
#include <algorithm>
#include <cctype>

namespace secondary_index {

namespace {

const sstring min_gram_option = "min_gram";
const sstring max_gram_option = "max_gram";
const sstring case_sensitive_option = "case_sensitive";
const sstring order_by_option = "order_by";
const sstring segment_rows_option = "segment_rows";
const sstring rewrite_option = "rewrite";
const sstring verify_candidates_option = "verify_candidates";
const std::vector<sstring> verify_candidates_values = {"index", "scylla"};

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

const std::unordered_map<sstring, std::function<void(std::string_view, const sstring&, const sstring&)>> pattern_index_options = {
        // 'case_sensitive' set to false lowercases both the indexed values and the patterns.
        {case_sensitive_option, std::bind_front(util::validate_enumerated_option, util::boolean_values)},
        // The length in characters of the shortest indexed substring. A shorter keyword cannot be answered.
        {min_gram_option, std::bind_front(util::validate_positive_option, pattern_index::max_gram_limit)},
        // The length in characters of the longest indexed substring. A keyword up to this length is a
        // single exact lookup; a longer one is answered from its substrings and verified.
        {max_gram_option, std::bind_front(util::validate_positive_option, pattern_index::max_gram_limit)},
        // The column results are ordered by. Only that it names something is checked here; that the
        // column exists and can be ordered is checked against the schema in check_order_by_column().
        {order_by_option, [](std::string_view index, const sstring& name, const sstring& value) {
             if (value.empty()) {
                 throw exceptions::invalid_request_exception(
                         format("Index {}: option '{}' must name a column", index, name));
             }
         }},
        // The most rows the index node puts in one segment of an ordered index, which keeps segments
        // narrow in the sort column; 0 for no cap. Absent, the node picks it for an ordered index.
        {segment_rows_option, validate_row_count_option},
        // Whether the index node rewrites segments that span far more of the sort column than their
        // share of the rows. Absent, on wherever there is a cap.
        {rewrite_option, std::bind_front(util::validate_enumerated_option, util::boolean_values)},
        // Who checks the candidates of a keyword longer than the longest indexed substring: the index
        // node against its stored value ('index', the default), or ScyllaDB against the rows it reads
        // anyway ('scylla'). Read by ScyllaDB only.
        {verify_candidates_option, std::bind_front(util::validate_enumerated_option, verify_candidates_values)},
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

void pattern_index::check_verify_candidates(const cql3::statements::index_specific_prop_defs& properties) const {
    const auto& options = properties.get_raw_options();
    auto it = options.find(verify_candidates_option);
    if (it == options.end() || !option_is(it->second, "scylla")) {
        return;
    }
    // ScyllaDB's LIKE is case-sensitive, so it can repeat only a case-sensitive index's check, and a
    // page it leaves short resumes from the node's cursor, which only an ordered index reports.
    auto case_sensitive = options.find(case_sensitive_option);
    if (case_sensitive != options.end() && option_is(case_sensitive->second, "false")) {
        throw exceptions::invalid_request_exception(
                "Pattern index option verify_candidates = 'scylla' needs a case-sensitive index: ScyllaDB cannot repeat the "
                "index node's case folding");
    }
    if (!options.contains(order_by_option)) {
        throw exceptions::invalid_request_exception(
                "Pattern index option verify_candidates = 'scylla' needs an 'order_by' option: without one the index reports "
                "no cursor to resume a page from once ScyllaDB has dropped a candidate");
    }
}

void pattern_index::check_order_by_column(const schema& schema, const cql3::statements::index_specific_prop_defs& properties) const {
    const auto& options = properties.get_raw_options();
    auto it = options.find(order_by_option);
    if (it == options.end()) {
        return;
    }
    const auto& name = it->second;
    auto const* c_def = schema.get_column_definition(to_bytes(name));
    if (c_def == nullptr) {
        throw exceptions::invalid_request_exception(format("Pattern index orders by column {}, which is not in the table", name));
    }
    // The index node keeps the sort value as a fixed-width integer, so only the types with an
    // order-preserving image in one can be ordered by. This is the node's rule (`is_orderable` in
    // its cql_types.rs) repeated so that a bad index is refused at CREATE rather than later; a
    // disagreement refuses an index, it cannot misorder one.
    switch (c_def->type->get_kind()) {
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

bool pattern_index::case_sensitive(const index_metadata& im) {
    auto it = im.options().find(case_sensitive_option);
    // Validated against util::boolean_values at CREATE INDEX, so it is one of the two.
    return it == im.options().end() ? default_case_sensitive : option_is(it->second, "true");
}

bool pattern_index::scylla_verifies_candidates(const index_metadata& im) {
    auto it = im.options().find(verify_candidates_option);
    return it != im.options().end() && option_is(it->second, "scylla");
}

std::optional<sstring> pattern_index::order_by(const index_metadata& im) {
    auto it = im.options().find(order_by_option);
    if (it == im.options().end()) {
        return std::nullopt;
    }
    return it->second;
}

void pattern_index::validate(const schema& schema, const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets, const gms::feature_service&, const data_dictionary::database& db) const {
    check_uses_tablets(schema, db);
    check_target(schema, targets);
    check_cdc_options(schema);
    check_index_options(properties);
    check_order_by_column(schema, properties);
    check_verify_candidates(properties);
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
