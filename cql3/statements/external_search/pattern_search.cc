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

namespace {

/// The index's columns as its option declares them, for messages.
sstring declared_order(const secondary_index::pattern_index::order& order) {
    sstring declared;
    for (const auto& column : order.columns) {
        declared += seastar::format("{}{} {}", declared.empty() ? "" : ", ", column.name, column.descending ? "DESC" : "ASC");
    }
    return declared;
}

} // anonymous namespace

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

ordering_fit fit_ordering(const schema& schema, const raw::select_statement::parameters& parameters,
        const secondary_index::pattern_index::order& order) {
    using kind = secondary_index::pattern_index::order::kind;
    const auto& orderings = parameters.orderings();
    if (order.what != kind::columns) {
        if (orderings.empty()) {
            return {};
        }
        return {.mismatch = "Pattern search queries on an index without order_by return rows in table order, as a filtered LIKE does, and "
                            "cannot be ordered: order by columns an index declares in its 'order_by' option"};
    }
    auto declared = declared_order(order);
    if (orderings.empty()) {
        return {.mismatch = seastar::format("Pattern search queries on an index ordered by {} need an ORDER BY on those columns, or the first of "
                                            "them: without one, a LIKE returns rows in table order, which this index does not keep",
                        declared)};
    }
    if (orderings.size() > order.columns.size()) {
        return {.mismatch = seastar::format("Pattern search queries can only be ordered by the index's columns ({}), not by more", declared)};
    }
    ordering_fit fit;
    std::optional<bool> reversed;
    for (size_t i = 0; i < orderings.size(); ++i) {
        const auto& [raw_column, ordering] = orderings[i];
        if (!raw_column) {
            return {.mismatch = "Pattern search queries cannot be ordered by a scoring function"};
        }
        auto column = raw_column->prepare_column_identifier(schema);
        if (column->text() != order.columns[i].name) {
            return {.mismatch = seastar::format("Pattern search queries can only be ordered by the index's columns ({}), in that order, not by {}",
                            declared, column->to_string())};
        }
        const auto* direction = std::get_if<raw::select_statement::ordering>(&ordering);
        if (!direction) {
            return {.mismatch = "Pattern search queries can only be ordered ASC or DESC"};
        }
        const bool descending = *direction == raw::select_statement::ordering::descending;
        const bool flipped = descending != order.columns[i].descending;
        if (reversed && *reversed != flipped) {
            return {.mismatch = seastar::format(
                            "Pattern search queries must order by the index's columns in their declared directions ({}) or all reversed", declared)};
        }
        reversed = flipped;
        if (i == 0) {
            fit.first_descending = descending;
        }
    }
    return fit;
}

sstring query_pattern(const cql3::raw_value& value) {
    return value_cast<sstring>(utf8_type->deserialize(cql3::raw_value(value).to_bytes()));
}

} // namespace cql3::statements::pattern_search
