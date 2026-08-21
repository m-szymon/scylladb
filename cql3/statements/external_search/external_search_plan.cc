/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_search_plan.hh"

#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_select_statement.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "index/vector_index.hh"
#include "types/types.hh"
#include "utils/log.hh"

#include <seastar/core/on_internal_error.hh>

#include <algorithm>
#include <ranges>
#include <cmath>

namespace cql3::statements {

static logging::logger plan_log("external_search_plan");

namespace {

struct named_reading {
    const functions::function_name& function;
    call_reading reading;
};

std::optional<call_reading> reading_of(const expr::function_call& fc) {
    const named_reading readings[] = {
            {functions::ANN_FUNCTION_NAME, {external_search_kind::ann, search_value::hit, "ANN"}},
            {functions::ANN_SCORE_FUNCTION_NAME, {external_search_kind::ann, search_value::score, "ANN_SCORE"}},
            {functions::ANN_RANK_FUNCTION_NAME, {external_search_kind::ann, search_value::rank, "ANN_RANK"}},
            {functions::BM25_FUNCTION_NAME, {external_search_kind::bm25, search_value::hit, "BM25"}},
            {functions::BM25_SCORE_FUNCTION_NAME, {external_search_kind::bm25, search_value::score, "BM25_SCORE"}},
            {functions::BM25_RANK_FUNCTION_NAME, {external_search_kind::bm25, search_value::rank, "BM25_RANK"}},
            {functions::BM25_HIGHLIGHT_FUNCTION_NAME, {external_search_kind::bm25, search_value::fragment, "BM25_HIGHLIGHT"}},
    };
    for (const auto& [function, reading] : readings) {
        if (expr::is_native_function_call(fc, function)) {
            return reading;
        }
    }
    return std::nullopt;
}

/// How a search of the given kind names the value it is asked about, for the messages that reject a
/// call disagreeing with the one the rows are ranked by.
std::string_view query_value_name(external_search_kind kind) {
    return kind == external_search_kind::ann ? "query vector" : "search term";
}

/// What the clause naming a search the statement does not run is told.
sstring clause_mismatch_message(const call_reading& reading, search_clause clause, bool kind_is_run) {
    if (clause == search_clause::restrictions) {
        return seastar::format("{}() in WHERE names a search that the ORDER BY clause does not run", reading.name);
    }
    if (kind_is_run) {
        return reading.kind == external_search_kind::ann
                ? seastar::format("{}() in SELECT must reference the same column as the ANN ordering", reading.name)
                : seastar::format("{}() in SELECT must reference the same column as BM25() in WHERE and ORDER BY", reading.name);
    }
    return reading.kind == external_search_kind::ann
            ? seastar::format("{}() is not supported in the SELECT clause without a matching ANN ordering", reading.name)
            : seastar::format("{}() is not supported in the SELECT clause without matching ORDER BY and WHERE clauses", reading.name);
}

/// What a call disagreeing with the search it names about what to ask is told.
sstring agreement_message(const call_reading& reading, std::string_view value_name) {
    return reading.kind == external_search_kind::ann
            ? seastar::format("{}() in SELECT must use the same {} as the ANN ordering", reading.name, value_name)
            : seastar::format("{}() in SELECT must use the same {} as BM25() in WHERE and ORDER BY", reading.name, value_name);
}

/// The similarity a rescored vector search's score is computed with on the coordinator.  It reads
/// the fetched vector column and the query vector, so it needs nothing injected per row - which is
/// also what lets it be evaluated in the position a nested occurrence asks for.
expr::expression make_similarity_expression(const secondary_index::index& index, const column_definition* column,
        const expr::expression& query_vector, data_dictionary::database db, const schema_ptr& schema) {
    auto similarity = secondary_index::vector_index::get_cql_similarity_function_name(index.metadata().options());
    auto name = functions::function_name::native_function(sstring(similarity));

    std::vector<expr::expression> args{expr::column_value(column), query_vector};
    std::vector<shared_ptr<assignment_testable>> provided_args{
            expr::as_assignment_testable(args[0], expr::type_of(args[0])),
            expr::as_assignment_testable(args[1], expr::type_of(args[1])),
    };

    return expr::function_call{
            .func = functions::instance().get(db, schema->ks_name(), name, provided_args, schema->ks_name(), schema->cf_name(), nullptr),
            .args = std::move(args),
    };
}

/// The index that will answer a search of this kind on this column.
secondary_index::index resolve_index(external_search_kind kind, data_dictionary::database db, const schema_ptr& schema,
        const column_definition& column) {
    auto cf = db.find_column_family(schema);
    auto indexes = cf.get_index_manager().list_indexes();

    if (kind == external_search_kind::ann) {
        auto it = std::ranges::find_if(indexes, [&column] (const auto& index) {
            return secondary_index::vector_index::is_vector_index_on_column(index.metadata(), column.name_as_text());
        });
        if (it == indexes.end()) {
            throw exceptions::invalid_request_exception("ANN ordering by vector requires the column to be indexed using 'vector_index'");
        }
        return *it;
    }

    auto it = std::ranges::find_if(indexes, [&column] (const auto& index) { return index.supports_bm25_expression(column); });
    if (it == indexes.end()) {
        throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
    }
    return *it;
}

} // anonymous namespace

external_search_plan::external_search_plan(data_dictionary::database db, schema_ptr schema, prepare_context& ctx,
        expr::temporary_allocator& temporaries_allocator)
    : _db(db)
    , _schema(std::move(schema))
    , _ctx(ctx)
    , _temporaries_allocator(temporaries_allocator) {
}

search_source& external_search_plan::claim(const expr::function_call& fc, const call_reading& reading, search_clause clause) {
    auto [column, query_value] = external_search::extract_call_arguments(fc, reading.name);

    auto it = std::ranges::find_if(_sources, [&] (const search_source& source) {
        return source.kind == reading.kind && source.column == column;
    });

    if (it == _sources.end()) {
        if (clause != search_clause::ordering) {
            // Only the ORDER BY clause asks for a search; the others report one it named. Whether
            // the statement runs no search of this kind or runs one on another column is worth
            // telling apart: the first is a missing clause, the second a disagreement between two.
            const bool kind_is_run = std::ranges::any_of(
                    _sources, [&] (const search_source& source) { return source.kind == reading.kind; });
            throw exceptions::invalid_request_exception(clause_mismatch_message(reading, clause, kind_is_run));
        }
        auto index = resolve_index(reading.kind, _db, _schema, *column);
        _sources.push_back(search_source{
                .kind = reading.kind,
                .index = index,
                .column = column,
                .query_value = query_value,
                .is_rescoring_enabled = reading.kind == external_search_kind::ann
                        && secondary_index::vector_index::is_rescoring_enabled(index.metadata().options()),
        });
        return _sources.back();
    }

    if (clause == search_clause::restrictions) {
        // A relation is validated by the search that owns it, which compares the query value there
        // and keeps the comparison execution cannot settle here. All this has to establish is that
        // there is a search for it to belong to.
        return *it;
    }

    // Two calls naming one search have to agree on what it asks, or they are not one search.
    auto message = agreement_message(reading, query_value_name(reading.kind));
    if (auto deferred = external_search::check_query_value(query_value, it->query_value, message)) {
        // Lifted out of the expression it was written in, whose lowered form is a leaf, so nothing
        // else will register its bind markers.
        expr::fill_prepare_context(*deferred, _ctx);
        it->deferred.push_back({std::move(*deferred), std::move(message)});
    }
    return *it;
}

expr::expression external_search_plan::deliver(const call_reading& reading, const expr::expression& call, search_source& source, bool& unnamed) {
    // A slot standing for a whole call carries it, so an unaliased selector is still named after
    // what the user wrote; the ones inside a pair carry nothing, since neither of them is the call.
    auto slot = [&] (std::optional<size_t>& index, data_type type, std::optional<expr::expression> replaced) {
        if (!index) {
            index = _temporaries_allocator.allocate();
        }
        return expr::expression(expr::temporary{.index = *index, .type = std::move(type), .replaced_expr = std::move(replaced)});
    };

    auto score = [&] (std::optional<expr::expression> replaced) -> expr::expression {
        if (!source.is_rescoring_enabled) {
            return slot(source.score_slot, float_type, std::move(replaced));
        }
        // A rescoring index answers with a similarity computed from a quantized vector, and the
        // coordinator recomputes it from the stored one. That reads the fetched column and the query
        // vector, so it needs no slot - and nothing else would do, since it is also what the rows
        // are reordered by.  It reads back as the similarity function, not as the call.
        unnamed = true;
        return make_similarity_expression(source.index, source.column, source.query_value, _db, _schema);
    };

    auto rank = [&] (std::optional<expr::expression> replaced) -> expr::expression {
        if (!source.is_rescoring_enabled) {
            return slot(source.rank_slot, int32_type, std::move(replaced));
        }
        // The rows are no longer in the order the index ranked them, so its rank would be a wrong
        // answer; the rank in the recomputed order needs every row's score at once, which this
        // path does not hold. 0 is outside the range a rank occupies, so it reads as "not
        // reported" rather than as the null that means "this search did not find the row".
        unnamed = true;
        return expr::constant(cql3::raw_value::make_value(int32_type->decompose(int32_t(0))), int32_type);
    };

    switch (reading.value) {
    case search_value::score:
        return score(call);
    case search_value::rank:
        return rank(call);
    case search_value::fragment:
        // The excerpt is generated from the row's own text, so that column has to be read from every
        // row even when the query does not select it.
        source.fragment_column = source.column;
        return slot(source.fragment_slot, utf8_type, call);
    case search_value::hit:
        // A pair of slots reads back as neither of the calls that could have named it.
        unnamed = true;
        return expr::expression(expr::tuple_constructor{
                .elements = {score(std::nullopt), rank(std::nullopt)},
                .type = functions::search_hit_type(),
        });
    }
    on_internal_error(plan_log, "unhandled search value");
}

expr::expression external_search_plan::lower(const expr::expression& e, search_clause clause, bool& lowered_any, bool& unnamed) {
    return expr::search_and_replace(e, [&] (const expr::expression& candidate) -> std::optional<expr::expression> {
        const auto* fc = expr::as_if<expr::function_call>(&candidate);
        if (!fc) {
            return std::nullopt;
        }
        const auto reading = reading_of(*fc);
        if (!reading) {
            return std::nullopt;
        }
        lowered_any = true;
        auto& source = claim(*fc, *reading, clause);
        return deliver(*reading, candidate, source, unnamed);
    });
}

void external_search_plan::bind_ordering(const expr::expression& prepared_ordering) {
    // A bare call is the common case and the one the external systems are built for: the rows come
    // back in the order the search ranked them, so nothing has to be computed or sorted here, and
    // the search's answers cost no slot unless the query also reports them.
    if (const auto* fc = expr::as_if<expr::function_call>(&prepared_ordering)) {
        if (const auto reading = reading_of(*fc)) {
            if (reading->value == search_value::fragment) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() cannot rank rows: it is an excerpt of one, not a measure of it", reading->name));
            }
            auto& source = claim(*fc, *reading, search_clause::ordering);
            if (source.is_rescoring_enabled) {
                // Except here, where the index's order is not the requested one.
                _ordering_expr = make_similarity_expression(source.index, source.column, source.query_value, _db, _schema);
            }
            return;
        }
    }

    bool lowered_any = false;
    bool unnamed = false;
    auto ordering = lower(prepared_ordering, search_clause::ordering, lowered_any, unnamed);
    if (!lowered_any) {
        // A function call in ORDER BY that names no search. The regular-ordering path skips a
        // scoring ordering, so reject it here rather than let the clause be silently ignored.
        throw exceptions::invalid_request_exception(
                "An ORDER BY expression must name at least one search, through ANN() or BM25()");
    }
    if (expr::type_of(ordering) != float_type) {
        throw exceptions::invalid_request_exception(seastar::format(
                "An ORDER BY expression over searches must be a score, but {} is {}",
                prepared_ordering, expr::type_of(ordering)->as_cql3_type()));
    }
    _ordering_expr = std::move(ordering);
}

void external_search_plan::bind_selectors(std::vector<selection::prepared_selector>& prepared_selectors) {
    for (auto& ps : prepared_selectors) {
        // What the user wrote, before any of it is lowered: a lowered call formats as neither the
        // call nor anything a client would recognize, so an unaliased selector keeps this as its
        // name whenever what it lowered to cannot carry it.
        const auto written = ps.expr;
        bool lowered_any = false;
        bool unnamed = false;

        ps.expr = lower(ps.expr, search_clause::selectors, lowered_any, unnamed);

        if (unnamed && !ps.alias) {
            ps.alias = ::make_shared<column_identifier>(fmt::format("{:result_set_metadata}", written), true);
        }
    }
}

void external_search_plan::bind_restrictions(const restrictions::statement_restrictions& restrictions) {
    for (const auto& binop : restrictions.get_scoring_function_restrictions()) {
        // statement_restrictions only diverts a relation whose left-hand side is a call to an
        // external function, so the cast cannot fail.
        const auto& fc = expr::as<expr::function_call>(binop.lhs);
        const auto reading = reading_of(fc);
        if (!reading) {
            on_internal_error(plan_log, seastar::format("no search claimed the external function call {}", fc));
        }
        if (_sources.empty()) {
            throw exceptions::invalid_request_exception(
                    "A scoring function in the WHERE clause requires a matching ORDER BY clause");
        }
        claim(fc, *reading, search_clause::restrictions);
    }
}

::shared_ptr<select_statement> external_search_plan::make_statement(external_statement_args args) const {
    return external_search_select_statement::prepare(_db, _sources, std::move(args));
}

select_statement::ordering_comparator_type descending_score_ordering_comparator(
        const expr::expression& score_expr, uint32_t column_index) {
    // Every score is a float; nothing in the types says so, so check it here rather than let the
    // comparator read another type's bytes as one.
    if (expr::type_of(score_expr) != float_type) {
        on_internal_error(plan_log,
                seastar::format("rows cannot be ranked by {}, which is {} rather than a score", score_expr, expr::type_of(score_expr)->name()));
    }
    return [column_index] (const raw::select_statement::result_row_type& r1, const raw::select_statement::result_row_type& r2) {
        auto& c1 = r1[column_index];
        auto& c2 = r2[column_index];
        auto f1 = c1 ? value_cast<float>(float_type->deserialize(*c1)) : std::numeric_limits<float>::quiet_NaN();
        auto f2 = c2 ? value_cast<float>(float_type->deserialize(*c2)) : std::numeric_limits<float>::quiet_NaN();
        if (std::isfinite(f1) && std::isfinite(f2)) {
            return f1 > f2;
        }
        // A row with no usable score sorts last, whichever way the other compares.
        return std::isfinite(f1);
    };
}

} // namespace cql3::statements
