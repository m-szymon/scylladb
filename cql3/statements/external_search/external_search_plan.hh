/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/expr/temporary_allocator.hh"
#include "cql3/selection/selection.hh"
#include "cql3/statements/select_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"

#include <optional>
#include <vector>

namespace cql3::restrictions {
class statement_restrictions;
}

namespace cql3::statements {

/// The external search systems a query can be served by.  Which one a call belongs to is decided by
/// the function it is called by, and settles what the query value means and how the search is run.
enum class external_search_kind {
    ann,
    bm25,
};

/// Where in the statement a call was written.  The three clauses ask different things of a search -
/// only the ORDER BY clause asks for one at all - so a call that names no search this statement runs
/// is refused differently in each.
enum class search_clause {
    ordering,
    selectors,
    restrictions,
};

/// Which of a search's answers a call reports.
enum class search_value {
    /// The pair the search answers with, both halves at once.
    hit,
    score,
    rank,
    /// An excerpt of the searched text, which only a full-text search has.
    fragment,
};

/// What one call to an external function names: which search system, which of its answers, and the
/// name a message about the call names it by.
struct call_reading {
    external_search_kind kind;
    search_value value;
    std::string_view name;
};

/// A query value that only execution can compare against the one the search was resolved from, a
/// bind marker standing where at least one of the two values will be.
struct deferred_query_value {
    expr::expression value;
    /// What to say when it turns out to differ.  Settled here, where what the user wrote is still
    /// in hand, rather than reconstructed at execution from a call that is no longer there.
    sstring disagreement_message;
};

/// One search an external index will be asked to run, and the slots its answers reach a row in.
///
/// A source is identified by what it asks - its kind and the column it searches - so every call
/// naming the same search shares it, and a query writing four of them still costs one request.
/// Calls naming one source must agree on the query value; where either side is a bind marker that
/// can only be settled at execution, which is what `deferred` is for.
struct search_source {
    external_search_kind kind;
    secondary_index::index index;
    /// The column searched: the vector an ANN search is near to, the text a BM25 search matches.
    const column_definition* column;
    /// The query vector, or the search term.
    expr::expression query_value;
    /// ANN only: the index recomputes the similarity on the coordinator and reorders by it.
    bool is_rescoring_enabled = false;

    /// Where each of the search's answers about a row is delivered, allocated on the first call
    /// asking for it.  A source with none of them allocated is one the query only ranks by.
    std::optional<size_t> score_slot;
    std::optional<size_t> rank_slot;
    /// BM25 only: an excerpt of the searched text, fetched by a second request.
    std::optional<size_t> fragment_slot;
    /// The column an excerpt is generated from - always `column`, recorded when one is asked for
    /// because its text then has to be read from every row.
    const column_definition* fragment_column = nullptr;

    std::vector<deferred_query_value> deferred;
    /// Every source has one; a statement asking for nothing about a search only ranks by it.
    std::vector<deferred_query_value>& deferrals() {
        return deferred;
    }

    bool reports_anything() const {
        return score_slot || rank_slot || fragment_slot;
    }
};

/// Arguments every external-search SELECT statement is built from, bundled so that
/// select_statement::prepare() need not know what kind of search - or how many - it is building for.
struct external_statement_args {
    schema_ptr schema;
    uint32_t bound_terms;
    lw_shared_ptr<const raw::select_statement::parameters> parameters;
    ::shared_ptr<selection::selection> selection;
    ::shared_ptr<const restrictions::statement_restrictions> restrictions;
    ::shared_ptr<std::vector<size_t>> group_by_cell_indices;
    bool is_reversed;
    select_statement::ordering_comparator_type ordering_comparator;
    std::optional<expr::expression> limit;
    std::optional<expr::expression> per_partition_limit;
    cql_stats& stats;
    std::unique_ptr<cql3::attributes> attrs;
};

/// The searches one statement is served by, and how the calls naming them are lowered.
///
/// A call to an external function does not compute its value from its arguments: only the search
/// system answering the query can. Preparation therefore has to find every such call - in ORDER BY,
/// in SELECT, in WHERE - work out which search it names, and lower it to where that search's answer
/// will be. This is where that happens, for however many searches the statement names.
///
/// The ORDER BY clause is what names the searches: it is the one clause a query is served by rather
/// than merely reporting, so a search a SELECT occurrence names must already be there. The clauses
/// are bound in separate calls only because they become available at different points of
/// select_statement::prepare(); collecting, matching and validating are shared.
class external_search_plan {
    data_dictionary::database _db;
    schema_ptr _schema;
    prepare_context& _ctx;
    expr::temporary_allocator& _temporaries_allocator;
    std::vector<search_source> _sources;
    std::optional<expr::expression> _ordering_expr;

public:
    external_search_plan(data_dictionary::database db, schema_ptr schema, prepare_context& ctx,
            expr::temporary_allocator& temporaries_allocator);

    /// Resolves the searches the ORDER BY expression names and lowers every call in it, leaving an
    /// expression over their answers for the rows to be ranked by.  Rejects an expression naming no
    /// search at all, since the regular-ordering path skips a scoring ordering and it would
    /// otherwise be silently ignored.
    void bind_ordering(const expr::expression& prepared_ordering);

    /// Lowers every external call in the SELECT clause, nested occurrences included, to where that
    /// search's answer is delivered.  A call naming a search the ORDER BY did not is rejected: a
    /// SELECT clause reports a search, it does not ask for one.
    void bind_selectors(std::vector<selection::prepared_selector>& prepared_selectors);

    /// Validates the relations statement_restrictions held out of the filtering machinery for the
    /// search that owns them.  A relation no search claims is rejected: nothing would interpret it,
    /// and it would be silently dropped rather than applied.
    void bind_restrictions(const restrictions::statement_restrictions& restrictions);

    /// True when the statement names no search, i.e. this is an ordinary query.
    bool empty() const {
        return _sources.empty();
    }

    /// The expression the rows must be ranked by, when the searches' own answer order is not the
    /// requested order - a fusion of several searches, anything computed on top of one, or one
    /// whose rows the coordinator reorders.  Added as a hidden trailing selector by
    /// select_statement::prepare(), which is where the comparator can read it.  Empty when the one
    /// search's own order is the answer.
    const std::optional<expr::expression>& ordering_expr() const {
        return _ordering_expr;
    }

    const std::vector<search_source>& sources() const {
        return _sources;
    }

    ::shared_ptr<select_statement> make_statement(external_statement_args args) const;

private:
    /// The source `fc` names, resolving a new one when the clause is allowed to ask for a search
    /// and no source asks what this call does.  Records a disagreement over the query value that
    /// only execution can settle.
    search_source& claim(const expr::function_call& fc, const call_reading& reading, search_clause clause);

    /// The expression the named answer is delivered by, allocating its slot on first use.  Sets
    /// `unnamed` when what it returns cannot be read back as the call it replaced, so that the
    /// selector holding it needs a name of its own.
    expr::expression deliver(const call_reading& reading, const expr::expression& call, search_source& source, bool& unnamed);

    /// Lowers every external call in `e`, nested occurrences included.
    expr::expression lower(const expr::expression& e, search_clause clause, bool& lowered_any, bool& unnamed);
};

/// A comparator ranking result rows by a score read from `column_index`, descending, with rows whose
/// score is not a usable number last.  Used with the hidden selector `ordering_expr()` asks for.
select_statement::ordering_comparator_type descending_score_ordering_comparator(
        const expr::expression& score_expr, uint32_t column_index);

} // namespace cql3::statements
