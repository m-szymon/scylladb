# Pattern Search — Developer Notes

For user-facing documentation see [Pattern Search](../features/pattern-search.rst),
the [Pattern Index section](../cql/secondary-indexes.rst#create-pattern-index-statement)
and [Indexed LIKE queries](../cql/dml/select.rst#pattern-queries).

This document covers the implementation and the decisions behind it.

## What it is

`pattern_index` is the third external (Vector Store backed) index kind, next to `vector_index`
and `fulltext_index`. The index node answers `POST /api/v1/indexes/{ks}/{index}/like` with the
primary keys of the rows whose value matches a `LIKE` pattern, and ScyllaDB reads those rows from
the base table. Nothing is scored, so the reply carries primary keys only, in no particular order;
ScyllaDB returns the rows in table order.

## Why a separate index kind

The full-text index tokenizes into words, stores frequencies and positions, and ranks by BM25.
None of that is wanted for pattern matching, and the CQL entry point is `LIKE`, not
`BM25()`. Sharing the class would mean every option and code path had two modes. The two share
everything around the engine instead: CDC ingestion, the index-node protocol and query execution.

## Query routing and prepare

Unlike BM25 and ANN, which are function calls introduced by ORDER BY, a `LIKE` is an ordinary
column restriction:

- A pattern index supports `LIKE` on its target column, whatever the pattern, and no other
  operator, so the secondary index selection accepts a `LIKE` alone without `ALLOW FILTERING`. A
  query on the column without a `LIKE`, such as an equality, is filtered as if there were no index.
- A `LIKE` on that column becomes a pattern search, whichever index the selection picked, and runs
  like an ANN or BM25 search rather than through the view-indexed path. It is found in the WHERE
  clause before the restrictions are prepared, which are then prepared as if `ALLOW FILTERING`
  were given, as for ANN and BM25. A query the index cannot serve, such as one with a second
  restriction on the column, is therefore rejected with the pattern search's error, rather than
  asked for an `ALLOW FILTERING` that would not help. Next to the `LIKE`, any other restriction
  is rejected, an equality included.
  No post-filtering happens, so the filtering checks are skipped, as for BM25 and ANN.
- A pattern search is not combined with other searches: a `LIKE` on a pattern-indexed column next
  to an ANN or BM25 search is rejected.

ScyllaDB does not look into the pattern. Which patterns the index node serves, and how, is up to
it, so widening that set needs no change here. It must treat `%`, `_` and `\` exactly as
a filtered `LIKE` does, so that a case-sensitive index matches a value exactly when a filtered
`LIKE` would. The index is updated from CDC and is eventually consistent, and the rows read from
the base table are not checked against the pattern again, so a stale entry can return a row that
no longer matches.

Creating the index is the opt-in: a `LIKE` on the column is routed even with `ALLOW FILTERING`, and
a failed or refused request fails the query. A `LIKE` the index cannot serve, such as one without a
`LIMIT` or with another restriction, is rejected. The index defines `LIKE` on its column for as
long as it exists, and `DROP INDEX` restores filtered `LIKE`. Since the index changes the results
of existing `LIKE` queries on its column, creating a pattern index returns a CQL warning naming the
column and stating these rules.

## Execution

A pattern index is in one of two orders, read from its `order_by` option by
`pattern_index::order_of` (the option's syntax is `parse_order`, which the index node repeats):
table order (no option), or the declared columns then table order. "Table order" here is the
index's own: partition after partition, each one's rows in clustering order, with the partitions
by token (`'partition_order': 'token'`, the order of a scan) or by a fixed scramble of the token
(`'scattered'`, the default). The option is read by the index node only; ScyllaDB validates its
value and never depends on which it is, since it returns the node's keys in the node's order.
The default is the scramble because of where the rows are read from: in token order the first
page of every frequent pattern is the lowest tokens, one tablet, and its replicas' shards read
the rows of every such page (AWS run 0818814c: one shard at 100%, the node at 49%).
`pattern_search::index_for` picks, among the pattern indexes on the `LIKE`'s column, one whose
order fits the query (`fit_ordering`): a query without `ORDER BY` wants table order; one with
`ORDER BY` wants an index ordered by those columns, a prefix of them, all as declared or all
reversed. With none that fits, the first is taken and its statement says why the query does not
fit.

## Ordered search

Every pattern index is served by `ordered_pattern_select_statement`, chosen in
`select_statement::prepare`. It derives from `external_index_select_statement` for the base-table
read only and replaces `do_execute`:

- `ORDER BY` must fit the index's order (`fit_ordering`, above); an index in table order takes none.
  The `WHERE` clause may add a range on the first ordering column. The coordinator's own ordering
  checks are skipped for an external search, so the statement validates both itself. The range goes
  to the index node as typed values (`min_sort_value`, `max_sort_value`), which the node encodes;
  ScyllaDB never sees the node's sort keys. The direction asked for the first column goes as
  `order`; the node walks its order or the exact reverse.
- The node walks its matches in that order and reports `next_cursor`, an opaque string that names
  the last row's place in the order and its primary key. It travels in the driver's paging state
  (`paging_state::get_index_cursor()`, an optional field of the IDL), so a page resumes there and
  rows tied on the ordering values are neither skipped nor repeated. A paging state without a cursor
  was made by another statement (a filtered `LIKE` paged before the index existed) and is refused.
- A paged query needs no LIMIT and asks for at most 1000 keys a page; an unpaged one needs a LIMIT of
  at most 1000. The check is made at execution, because only then is it known whether the query is
  paged.
- A page can come back short, or empty, with a cursor: a request to the node has a budget of
  time, and returns what it has settled when it ends; and a key the table no longer has leaves a
  page a row short. Such a page is topped up from the cursor. A paged query does so for half of
  the statement's timeout (`top_up_timeout_share`) and then hands the page to the client as it
  is, with a paging state: the rows of every round are read before the next is asked for, so the
  page is ready whenever the topping up stops. An unpaged query is topped up until its LIMIT is
  met, and fails when the timeout passes first: its client cannot ask for the rest, so a short
  answer would pass for a whole one.

At `CREATE INDEX` (`pattern_index::check_order`), an index that keeps an order is refused on a table
whose partitioner is not Murmur3 or whose primary key has a column of a type the index node cannot
order (`is_table_order_key_type`, the node's `is_key_type_supported`): the node computes table
order itself, from the token and the key's values.

## Options

The only option is `case_sensitive` (default true, matching `LIKE`). With `false`, the index node
lowercases both the values and the patterns. ScyllaDB sends the pattern as written and does no case
folding itself. How the index node matches patterns is its own concern, so none of it is exposed
as an option.

Only regular columns can be targets. ScyllaDB keeps every WHERE condition on a partition or
clustering key column, `LIKE` included, as a key restriction, and a pattern search rejects key
restrictions, so a `LIKE` on a key column could never reach the index. The Vector Store fetches
each row by its full primary key, which ScyllaDB refuses when the only selected column is static.

## Testing

- `test/cqlpy/test_pattern_index.py`: schema and option validation, and prepare-time query
  validation, including the columns a `LIKE` is still filtered on.
- `test/cqlpy/test_pattern_search_with_mock.py`: routing to the mocked `/like` endpoint, patterns
  and bind markers sent as written, error mapping, paging warning.
