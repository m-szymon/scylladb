# Hybrid Search — Developer Notes

A hybrid query is one served by more than one external search at once: a vector search and a
full-text search over the same rows, ranked by a score fused from what each of them said.

```cql
SELECT id, BM25_HIGHLIGHT(body, 'wombat')
  FROM ks.t
  ORDER BY RRF(ANN(embedding, [0.1, 0.2]), BM25(body, 'wombat'))
  LIMIT 10;
```

This is a **proof of concept**. It runs end to end against the Vector Store mock; the parts that are
placeholders are named as such below.

## Why a search answers with a pair

Scores of different searches are not on one scale. A BM25 relevance of 3.5 and a cosine similarity
of 0.9 cannot be added, and whichever of the two happens to be numerically larger decides the order
on its own. Ranks are comparable, which is why `ANN()` and `BM25()` answer with `(score, rank)`
rather than with the score alone — a caller given only the scores could not rank for itself, having
been handed the rows a limit already cut.

`RRF(hit, hit, ...)` is reciprocal-rank fusion over the pairs: `sum(1 / (k + rank))` with `k = 60`.
Because it is declared over the pair, `RRF(ANN(...), BM25(...))` type-checks as written, with no
context rule and no implicit conversion. A fusion function that wants to weigh scores as well as
ranks gets both halves from the same argument.

`ANN_SCORE()`, `ANN_RANK()`, `BM25_SCORE()` and `BM25_RANK()` name the halves. All of them describe
the same search — the arguments say which — so a query writing several still costs one request.

## How a query is planned

`external_search_plan` owns the searches a statement is served by. A `search_source` is one question
put to one index: a kind and a column. Every call to an external function is found by capability
(`functions::function::is_external()`), matched to a source by what it asks, and lowered to where
that source's answer will be — a temporary slot a provider fills per row, or, for a rescored vector
similarity, an expression the coordinator computes for itself.

The ORDER BY clause is what asks for a search; SELECT and WHERE report one it named. A bare call in
ORDER BY still means "the rows come back in the order this search ranked them", which costs no slot
and no sorting. Anything else becomes the score the rows are ranked by, added as a hidden trailing
selector and sorted on — the mechanism the rescored-similarity ordering already used.

## How a query is run

`external_search_select_statement` runs them all. It asks every index at once, unions the candidate
keys (a row is worth reading if any search named it), reads those rows once, and reports what each
search said about each of them, with nulls where a search did not find one.

Answers are matched to rows **by primary key**. The single-search path used to walk a cursor forward
in step with the rows, which worked because the base-table read was merged in that search's own key
order; with two searches the rows can only be read in one order, and that is at most one of theirs.

The rows are ranked after they are read, and only then cut to the limit — which is why each search
is asked for more candidates than the limit.

## Placeholders and gaps

- **The over-fetch factor** is `hybrid_candidate_factor = 4`. The right factor depends on the query,
  and choosing it should be the query's to make.
- **A rescored ANN reports a rank of `0`.** The rows are reordered by a similarity the coordinator
  recomputes, so the index's rank is the rank of an order they are no longer in; the rank in the
  recomputed order needs every row's score at once, which the lowering path does not hold. Giving it
  a real value means moving the recomputation to where the whole result is in hand — the same walk
  that matches the answers to rows.
- **A hybrid query takes no WHERE clause.** What a query may restrict by is a property of the search
  doing the work: a vector index prefilters, a full-text index does not. Agreeing that across
  indexes is unsettled.
- **`ORDER BY 0.7 * ANN_SCORE(...) + 0.3 * BM25_SCORE(...)` does not parse.** Everything below the
  grammar is ready for it — the ordering is lowered and typed as an expression, and rejected unless
  it is a float — but `orderByClause` parses a function call rather than an expression.
- **Paging** is unsupported for external-index queries generally, and unchanged here.
- **Latency** is attributed to the first search's index, and a hybrid query is named "Hybrid Search"
  in the paging warning.
