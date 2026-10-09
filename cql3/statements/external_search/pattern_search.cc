/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "pattern_search.hh"

#include "cql3/expr/expr-utils.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "exceptions/exceptions.hh"
#include "index/pattern_index.hh"
#include "index/secondary_index_manager.hh"
#include "types/types.hh"
#include "utils/assert.hh"

namespace cql3::statements::pattern_search {

namespace {

sstring one_like_message(std::string_view column) {
    return seastar::format("Pattern search queries support exactly one LIKE restriction, on the indexed column {}, and no other WHERE restrictions",
            column);
}

expr::binary_operator find_like_restriction(const restrictions::select_restrictions& select_restrictions, const secondary_index::index& index) {
    if (!select_restrictions.partition_key_restrictions_is_empty() || !restrictions::is_empty_restriction(select_restrictions.get_clustering_columns_restrictions())) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support additional WHERE restrictions");
    }
    const auto& non_pk = select_restrictions.get_non_pk_restriction();
    if (non_pk.size() != 1) {
        throw exceptions::invalid_request_exception(one_like_message(index.target_column()));
    }
    const auto& [column, restriction] = *non_pk.begin();
    if (column->name_as_text() != index.target_column()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Pattern search queries must restrict the indexed column {}", index.target_column()));
    }
    auto factors = expr::boolean_factors(restriction);
    const auto* binop = factors.size() == 1 ? expr::as_if<expr::binary_operator>(&factors.front()) : nullptr;
    if (!binop || binop->op != expr::oper_t::LIKE) {
        throw exceptions::invalid_request_exception(one_like_message(index.target_column()));
    }
    return *binop;
}

} // anonymous namespace

std::optional<secondary_index::index> index_for(data_dictionary::database db, const schema_ptr& schema,
        const restrictions::select_restrictions& restrictions, bool continues_paged_query) {
    if (restrictions.uses_secondary_indexing()) {
        auto index = restrictions.find_idx(db.find_column_family(schema).get_index_manager());
        if (index && secondary_index::secondary_index_manager::is_custom_index<secondary_index::pattern_index>(index->metadata())) {
            return index;
        }
    }
    for (const auto& [column, restriction] : restrictions.get_non_pk_restriction()) {
        if (expr::find(restriction, expr::oper_t::LIKE) && secondary_index::pattern_index::has_index_on_column(*schema, column->name_as_text())) {
            if (continues_paged_query) {
                throw external_index_select_statement::cannot_continue_paged_query(
                        seastar::format("a {} index", secondary_index::pattern_index::INDEX_TYPE_NAME), "Pattern search");
            }
            throw exceptions::invalid_request_exception(one_like_message(column->name_as_text()));
        }
    }
    return std::nullopt;
}

search_source source_for(secondary_index::index index, const schema& schema, const restrictions::select_restrictions& restrictions,
        bool has_ordering) {
    if (has_ordering) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support ORDER BY");
    }
    if (!restrictions.get_scoring_function_restrictions().empty()) {
        throw exceptions::invalid_request_exception("Pattern search queries cannot be combined with scoring functions");
    }
    auto like = find_like_restriction(restrictions, index);
    const auto* column = schema.get_column_definition(to_bytes(index.target_column()));
    throwing_assert(column);
    return search_source{
            .family = functions::search_family::pattern,
            .index = std::move(index),
            .column = column,
            .query_value = std::move(like.rhs),
    };
}

sstring query_pattern(const cql3::raw_value& value) {
    return value_cast<sstring>(utf8_type->deserialize(cql3::raw_value(value).to_bytes()));
}

} // namespace cql3::statements::pattern_search
