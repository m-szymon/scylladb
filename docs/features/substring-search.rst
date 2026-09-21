=================================
Substring Search in ScyllaDB
=================================

What Is Substring Search
-------------------------

Substring Search finds the rows whose text column **contains** a given
keyword, the way the SQL and CQL filter ``LIKE '%keyword%'`` does, but served
by an index rather than by a scan of the table. It is exact: a row is returned
if and only if the keyword occurs in the value, contiguously and in order.

It is the right tool when users look for one specific thing by part of its
name and the values are short: display names, user names, product codes, tags.
Display names in any script work the same way, since the index works on
characters, not on words. For example, with the table below::

    nickname     | username
    -------------+---------------
    宇将军        | user0001
    李将军        | user0002
    将军来了      | jiangjunlaile
    将领          | user0004
    南宫月        | user0007
    宫南          | user0009
    NG玩家        | NGgamer
    国王NG        | kingNG

* ``nickname LIKE '%将军%'`` returns 宇将军, 李将军 and 将军来了, but not 将领.
* ``nickname LIKE '%南宫%'`` returns 南宫月, but not 宫南.
* ``username LIKE '%ng%'`` returns NGgamer and kingNG on an index created with
  ``'case_sensitive': 'false'``.

Substring Search does not tokenize, stem or rank. For word-based search ranked
by relevance, use :doc:`Full-Text Search </features/fulltext-search>`.

Creating a Substring Index
---------------------------

Before you can run substring queries without ``ALLOW FILTERING``, create a
``substring_index`` on the column::

    CREATE CUSTOM INDEX ON ks.users (nickname) USING 'substring_index';

    CREATE CUSTOM INDEX ON ks.users (username) USING 'substring_index'
        WITH OPTIONS = {'case_sensitive': 'false'};

For the options and their defaults, see
:ref:`Substring Index <create-substring-index-statement>`.

**Requirements:**

* The indexed column must be of type ``text``, ``varchar``, or ``ascii``.
* The indexed column must be a regular column. Primary-key and static columns
  cannot be indexed.
* The table must use tablets (not vnodes).
* CDC must be enabled on the table with a TTL of at least 86400 seconds
  (24 hours) and either ``delta = 'full'`` or postimage enabled. CDC is enabled
  automatically when creating a substring index, so you do not need to
  configure it manually.

Cell-level TTL, set with the standard ``USING TTL`` syntax on ``INSERT`` or
``UPDATE``, makes the value unreadable at its expiration deadline but does not
generate a CDC event, so the substring index is not updated and keeps a stale
entry for that value. If the index must reflect expiration, use the
:ref:`Per-row TTL <cql-per-row-ttl>` feature instead.

Querying with LIKE
------------------

A ``LIKE`` whose pattern is exactly ``'%keyword%'`` on an indexed column is
answered by the index::

    SELECT user_id, nickname FROM ks.users
        WHERE nickname LIKE '%将军%'
        LIMIT 20;

    SELECT user_id, nickname FROM ks.users
        WHERE nickname LIKE ?
        LIMIT 20;

The full rules, including which patterns are served and what happens to the
others, are in :ref:`Substring search queries <substring-queries>`.

How It Works
------------

Every substring of a value between ``min_gram`` and ``max_gram`` characters
long is an indexed term, so a keyword of at most ``max_gram`` characters is a
single exact lookup. A longer keyword is answered by intersecting the rows that
contain each of its ``max_gram``-long pieces and then checking the value
itself, since the pieces alone could also occur apart from each other. Both
paths return exactly the rows a filtered ``LIKE`` would.

The index lives on the same index nodes as the vector and full-text indexes
and is updated from the table's CDC log, so it is eventually consistent: a
newly written or changed name becomes searchable within a few seconds.

Limitations
-----------

* Only the ``'%keyword%'`` pattern is served. Prefix, suffix and wildcard
  patterns still need ``ALLOW FILTERING``.
* The keyword must be at least ``min_gram`` characters long.
* ``LIMIT`` is required and capped at 1000; results are not ordered and not
  paged.
* One ``LIKE`` per query, and no other ``WHERE`` restriction, ``ORDER BY``,
  ``GROUP BY`` or aggregation.

Authorization
-------------

Reading a table through its substring index is covered by the same
``TEXT_SEARCH_INDEXING`` permission as reading it through a full-text index,
as an alternative to ``SELECT``.
