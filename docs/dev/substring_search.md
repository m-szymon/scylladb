# Substring Search — Developer Notes

For user-facing documentation see [Substring Search](../features/substring-search.rst),
the [Substring Index section](../cql/secondary-indexes.rst#create-substring-index-statement)
and [Substring search queries](../cql/dml/select.rst#substring-queries).

This document covers the implementation and the decisions behind it.

## What it is

`substring_index` is the third external (Vector Store backed) index kind, next to `vector_index`
and `fulltext_index`. The index node indexes every substring of the column between `min_gram` and
`max_gram` characters, answers `POST /api/v1/indexes/{ks}/{index}/contains` with the primary keys of
the rows whose value contains a keyword, and ScyllaDB reads those rows from the base table. Nothing
is scored, so the reply carries primary keys only and the rows come back in no particular order.

## Why a separate index kind

The full-text index tokenizes into words, stores frequencies and positions, and ranks by BM25.
None of that is wanted for containment on short names, and the CQL entry point is `LIKE`, not
`BM25()`. Sharing the class would mean every option and code path had two modes. What the two
share is everything around the engine (CDC ingestion, the index-node protocol, the
`external_index` base class, the `external_index_select_statement` execution), and that is shared.

## Query routing and prepare

Unlike BM25 and ANN, which are function calls the WHERE analysis holds out as "scoring
restrictions", a `LIKE` is an ordinary column restriction. It is routed through the regular
"does an index support this restriction" path:

- `index::supports_like_expression(column, rhs)` (`index/secondary_index_manager.cc`) answers yes
  only for a substring index on that column, and, for a literal pattern, only when
  `parse_contains_pattern()` accepts it. A pattern only execution will know (a bind marker) is
  accepted and checked at execution.
- `is_predicate_supported_by()` (`cql3/restrictions/statement_restrictions.cc`) calls it for
  `oper_t::LIKE` instead of the operator-only `supports_expression()`.
- A supported `LIKE` therefore makes the query "use secondary indexing" and the substring index
  is the chosen one. `raw::select_statement::prepare` recognises the chosen index as a substring
  index and dispatches to `substring_indexed_table_select_statement` before the view-indexed
  statement. `check_needs_filtering` and the filtering-column retrieval are skipped, as for BM25
  and ANN, because no post-filtering happens.

The consequence that matters: a `LIKE` the index does not serve (a prefix, a wildcard inside the
keyword, a keyword shorter than `min_gram`, a column without a substring index) makes
`supports_like_expression` answer no, so nothing changes for it: it still requires
`ALLOW FILTERING` and scans the table. Only the `%keyword%` shape is ever routed.

`parse_contains_pattern()` (`cql3/statements/external_search/substring_pattern.cc`) is the single
definition of that shape. It counts the keyword in UTF-8 code points, so `min_gram` means
characters for CJK text too. The `\` escape is rejected along with `%` and `_` because
`like_matcher` treats it specially.

## Execution

`execute_search` evaluates the pattern if it was a bind marker, parses it (throwing
`invalid_request_exception` for a pattern the index does not serve, since the index was already
chosen at prepare and a scan is no longer an option), posts the keyword and `LIMIT` to `/contains`,
and reads the returned keys from the base table with the shared `query_base_table`. No score
provider is installed, so on a table with clustering keys the rows come back in the token-merged
order of the read, not in the index's order. This is the same "no particular order" a filtered
`LIKE` has.

## Options

`min_gram` (default 1), `max_gram` (default 3) and `case_sensitive` (default true) mirror the
Cassandra SASI vocabulary. The defaults are duplicated on the index node, which applies them to an
index created without the option; the two must be kept in step. `min_gram <= max_gram` is checked
across options after the per-option validators, using the defaults for an absent one.

Targets are restricted to regular columns for now: a `LIKE` on a primary-key or static column runs
through the key-restriction analysis, which the index does not take part in.

## Testing

- `test/boost/external_search_test.cc`: `parse_contains_pattern` cases.
- `test/cqlpy/test_substring_index.py`: schema and option validation, and prepare-time query
  validation including the fall-back-to-filtering cases.
- `test/cqlpy/test_substring_search_with_mock.py`: routing to the mocked `/contains` endpoint,
  bind markers, error mapping, paging warning.
