/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "pattern_search.hh"

#include "cql3/column_identifier.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "index/pattern_index.hh"
#include "index/secondary_index_manager.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <algorithm>


namespace cql3::statements::pattern_search {

std::optional<secondary_index::index> index_for(data_dictionary::database db, const schema_ptr& schema,
        const expr::expression& where_clause) {
    auto indexes = db.find_column_family(schema).get_index_manager().list_indexes();
    for (const auto& factor : expr::boolean_factors(where_clause)) {
        const auto* binop = expr::as_if<expr::binary_operator>(&factor);
        const auto* lhs = binop && binop->op == expr::oper_t::LIKE ? expr::as_if<expr::unresolved_identifier>(&binop->lhs) : nullptr;
        if (!lhs) {
            continue;
        }
        auto column = lhs->ident->prepare_column_identifier(*schema)->text();
        auto on_column = [&] (const secondary_index::index& index) {
            return secondary_index::pattern_index::is_pattern_index_on_column(index.metadata(), column);
        };
        if (auto it = std::ranges::find_if(indexes, on_column); it != indexes.end()) {
            return std::move(*it);
        }
    }
    return std::nullopt;
}

sstring query_pattern(const cql3::raw_value& value) {
    return value_cast<sstring>(utf8_type->deserialize(cql3::raw_value(value).to_bytes()));
}

} // namespace cql3::statements::pattern_search
