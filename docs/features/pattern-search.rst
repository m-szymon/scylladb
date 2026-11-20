==========================
Pattern Search in ScyllaDB
==========================

.. note::

    This feature is currently available only in `ScyllaDB Cloud <https://cloud.docs.scylladb.com/>`_.

What Is Pattern Search
----------------------

Pattern Search finds the rows whose text column matches a ``LIKE``
pattern, such as ``'%abc%'``, ``'abc%'`` or ``'a%b'``, but served by an
index rather than by a scan of the table. The index node does not match
approximately: a value matches when the whole of it matches the pattern. The
pattern has the same syntax as in a filtered ``LIKE``, with the
same ``%``, ``_`` and ``\`` rules, but the query does not behave like a filtered
one in other respects; see
:ref:`Indexed vs. Filtered LIKE <pattern-search-indexed-vs-filtered>`.

It suits values that are searched by any part of them rather than by whole
words, such as product codes, identifiers and tags. For example, with the
table below::

    code
    ------------
    AB-1024-XL
    AB-2048-S
    ab-4096-M
    XAB-1024
    CD-1024-AB

* ``code LIKE 'AB-%'`` returns AB-1024-XL and AB-2048-S, and also ab-4096-M on
  an index created with ``'case_sensitive': 'false'``. It does not return
  XAB-1024 or CD-1024-AB, which contain ``AB`` but do not start with ``AB-``.
* ``code LIKE '%024%'`` returns AB-1024-XL, XAB-1024 and CD-1024-AB. It matches
  inside ``1024``, which a word-based search would not find.
* ``code LIKE '%-_'`` returns AB-2048-S and ab-4096-M, the codes that end with
  a one-character suffix.

A row either matches the pattern or it does not, and no row is ranked above
another: CD-1024-AB, with ``AB`` at its end, is as much a result of
``code LIKE '%AB%'`` as AB-1024-XL. When more rows match than the ``LIMIT``,
which of them are returned is not specified. A query can also return fewer rows
than its ``LIMIT``, even when more rows match, if a row was deleted after the
index found it.

Pattern Search does not tokenize, stem or rank. For word-based search ranked
by relevance, use :doc:`Full-Text Search </features/fulltext-search>`.

Creating a Pattern Index
------------------------

A ``pattern_index`` on a column serves the ``LIKE`` restrictions on that
column. Such a query does not need ``ALLOW FILTERING``; if it is given, it is
ignored and the index is still used. To create the index::

    CREATE CUSTOM INDEX ON ks.parts (code) USING 'pattern_index';

or, to match regardless of letter case::

    CREATE CUSTOM INDEX ON ks.parts (code) USING 'pattern_index'
        WITH OPTIONS = {'case_sensitive': 'false'};

For the option and its default, see
:ref:`Pattern Index <create-pattern-index-statement>`.

.. warning::

   Creating a pattern index changes the meaning of every ``LIKE`` on its
   column, for every application that queries the column. Once the index
   exists, a ``LIKE`` on the column is served by the index, even with
   ``ALLOW FILTERING``:

   * Queries that worked by filtering start failing. The ``LIKE`` must be the
     only restriction in the ``WHERE`` clause, and an unpaged query needs a
     ``LIMIT`` of at most 1000, so for example a ``LIKE`` together with a
     restriction on the partition key is rejected. An index with ``order_by``
     columns serves only queries ordered by them; see `Ordering and Paging`_.
   * Queries that still succeed can return different rows, without any error.
     The index is eventually consistent, so a recent write may be missed.
     With ``'case_sensitive': 'false'``, matching ignores letter case, so a
     ``LIKE`` returns rows that a filtered ``LIKE`` does not.
   * When the index node is unavailable, every ``LIKE`` on the column fails.

   ``CREATE INDEX`` returns a warning that summarizes these changes. Dropping the
   index restores filtered ``LIKE`` (see `Dropping a Pattern Index`_). For all
   the differences, see
   :ref:`Indexed vs. Filtered LIKE <pattern-search-indexed-vs-filtered>`.

**Requirements:**

* The indexed column must be of type ``text``, ``varchar``, or ``ascii``.
* The indexed column must be a regular column. Primary-key and static columns
  cannot be indexed.
* The table must use tablets (not vnodes).
* CDC must be enabled on the table with a TTL of at least 86400 seconds
  (24 hours) and either ``delta = 'full'`` or postimage enabled, because the
  index is updated from the CDC log. CDC is enabled automatically when creating
  a pattern index, so you do not need to configure it manually.

Cell-level TTL, set with the standard ``USING TTL`` syntax on ``INSERT`` or
``UPDATE``, makes the value unreadable at its expiration deadline but does not
generate a CDC event, so the pattern index is not updated and keeps a stale
entry for that value. A ``LIKE`` query can then still return the row, with the
expired value read as ``null``. If the index must reflect expiration, use the
:ref:`Per-row TTL <cql-per-row-ttl>` feature instead.

Querying with LIKE
------------------

On a column with a pattern index, ``LIKE`` queries look like this::

    SELECT id, code FROM ks.parts
        WHERE code LIKE '%024%'
        LIMIT 10;

    SELECT id, code FROM ks.parts
        WHERE code LIKE 'AB-%'
        LIMIT 10;

    SELECT id, code FROM ks.parts
        WHERE code LIKE ?
        LIMIT 10;

Creating the index is a deliberate choice to serve every ``LIKE`` on the
column from the index, even with ``ALLOW FILTERING``. The full rules are in
:ref:`Indexed LIKE queries <pattern-queries>`.

.. _pattern-search-indexed-vs-filtered:

Indexed vs. Filtered LIKE
~~~~~~~~~~~~~~~~~~~~~~~~~

The indexed column of the table below describes an index without ``order_by``,
which keeps table order. One with ``order_by`` columns is ordered by them; see
`Ordering and Paging`_.

Without a pattern index, ``LIKE`` is a filter: ScyllaDB reads the rows and
checks each value against the pattern, as described in
:ref:`LIKE Operator <like-operator>`. On a column with a pattern index, the
same ``LIKE`` is served by the index instead, and the query differs from a
filtered one as follows:

+----------------------+------------------------------------------+------------------------------------------------------+
|                      | Filtered ``LIKE``                        | Indexed ``LIKE``                                     |
+======================+==========================================+======================================================+
| Where it works       | Every deployment, on any ``text``,       | ScyllaDB Cloud, on a regular column with a           |
|                      | ``varchar`` or ``ascii`` column.         | ``pattern_index``. Every ``LIKE`` on that column is  |
|                      |                                          | served by the index.                                 |
+----------------------+------------------------------------------+------------------------------------------------------+
| Matching             | Always case-sensitive.                   | Case-sensitive, unless the index was created with    |
|                      |                                          | ``'case_sensitive': 'false'``: then letter case is   |
|                      |                                          | ignored.                                             |
+----------------------+------------------------------------------+------------------------------------------------------+
| Patterns             | Any pattern.                             | The same syntax. Which patterns are supported        |
|                      |                                          | depends on the index node.                           |
+----------------------+------------------------------------------+------------------------------------------------------+
| ``ALLOW FILTERING``  | Required.                                | Not needed. If given, it is ignored and the index is |
|                      |                                          | still used.                                          |
+----------------------+------------------------------------------+------------------------------------------------------+
| ``LIMIT``            | Optional, any value.                     | Optional when paged; at most 1000 when not.          |
+----------------------+------------------------------------------+------------------------------------------------------+
| Other restrictions   | Allowed, as in any query with            | None: the ``LIKE`` must be the only ``WHERE``        |
| and clauses          | ``ALLOW FILTERING``.                     | restriction, and ``ORDER BY``, ``GROUP BY``,         |
|                      |                                          | ``PER PARTITION LIMIT`` and aggregation are          |
|                      |                                          | rejected.                                            |
+----------------------+------------------------------------------+------------------------------------------------------+
| Paging               | Paged like any other query.              | Paged like any other query.                          |
+----------------------+------------------------------------------+------------------------------------------------------+
| Order                | The order of a scan: by token, then by   | The same, and ``LIMIT n`` returns the first ``n``    |
|                      | clustering key.                          | rows of it.                                          |
+----------------------+------------------------------------------+------------------------------------------------------+
| Consistency          | The rows are read, and matched, at the   | Eventually consistent, and the rows are not checked  |
|                      | query's consistency level.               | against the pattern again: shortly after a write, a  |
|                      |                                          | query may miss a new value, or return a row whose    |
|                      |                                          | current value no longer matches the pattern.         |
+----------------------+------------------------------------------+------------------------------------------------------+
| Failure              | Fails as any read does.                  | Also fails when the index node is unavailable or     |
|                      |                                          | does not support the pattern.                        |
+----------------------+------------------------------------------+------------------------------------------------------+

Ordering and Paging
-------------------

A ``LIKE`` without ``ORDER BY`` returns its rows in table order, as a filtered
``LIKE`` does, and that is the order an index without ``order_by`` keeps. A
search box that shows the newest matches first, a page at a time, needs an
index ordered by another column. The ``order_by`` option declares up to four
columns, each optionally ``ASC`` (the default) or ``DESC``; each must be an
integer, ``date``, ``time`` or ``timestamp`` column::

    CREATE CUSTOM INDEX ON ks.users (nickname) USING 'pattern_index'
        WITH OPTIONS = {'order_by': 'registered_at DESC'};

    SELECT user_id, nickname FROM ks.users
        WHERE nickname LIKE '%将军%'
        ORDER BY registered_at DESC;

    SELECT user_id, nickname FROM ks.users
        WHERE nickname LIKE '%将军%' AND registered_at < '2026-01-01'
        ORDER BY registered_at DESC;

* The index keeps its rows by the declared columns, then in table order. It
  serves a query that orders by a prefix of those columns, all in their
  declared directions or all reversed, as a table serves ordering by its
  clustering columns. With ``'order_by': 'a DESC, b'``, ``ORDER BY a DESC``,
  ``ORDER BY a DESC, b ASC`` and ``ORDER BY a ASC, b DESC`` are served, and
  ``ORDER BY a DESC, b DESC`` is rejected.
* A ``LIKE`` without ``ORDER BY`` is not served by such an index, since it does
  not keep table order; it is rejected, unless another pattern index on the
  column serves it. Several pattern indexes on one column are allowed, and each
  query uses the one whose order fits it.
* A range on the first ordering column (``<``, ``<=``, ``>``, ``>=``) narrows
  the search.
* A row whose ordering column is null is not in the index, as a materialized
  view leaves out a row whose key column is null.
* The query is paged by the index, the way the drivers page every query: a
  later page resumes where the previous one ended and costs about what the first
  one did, and rows sharing ordering values are neither skipped nor repeated. A
  paged query needs no ``LIMIT``; a query that is not paged needs one of at most
  1000.
* A page may hold fewer rows than the page size, or none, while more follow.
  Read until the driver reports no more pages.

To keep a later page as cheap as the first, the index node keeps its internal
segments narrow along its order (``segment_rows``) and rewrites the wide ones
an index created on an already loaded table, or one taking writes out of its
order, starts with (``rewrite``). Both are on by default. The
options are described in :ref:`Pattern Index <create-pattern-index-statement>`.

Dropping a Pattern Index
------------------------

Dropping the index restores filtered ``LIKE`` on its column::

    DROP INDEX ks.parts_code_idx;

From then on, a ``LIKE`` on the column is a filter again: it requires
``ALLOW FILTERING``, matches case-sensitively, and follows the rules of
:ref:`LIKE Operator <like-operator>`. Dropping the index is the way back when
the indexed ``LIKE`` does not suit an application that queries the column.
