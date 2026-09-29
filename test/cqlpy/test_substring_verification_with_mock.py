# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for where a substring-indexed query's matches are checked.
#
# For a keyword longer than the index's max_gram the index node has candidates
# -- rows holding every gram of the keyword -- rather than matches, and something
# has to check them against the value. A case-sensitive index leaves that to
# Scylla: it asks the node not to verify (`"verify": false`), the node flags the
# page (`"verified": false`), and Scylla applies the pattern to the value it
# reads with the row anyway. A page that comes back short is topped up from the
# node's cursor a bounded number of times, then handed over short, which a
# paging client takes in its stride.
#
# The index node is a mock, so what is checked is the CQL side: which requests
# say what, which rows survive, and how paging and the LIMIT account for the
# rows dropped.
###############################################################################

import json

import pytest
from cassandra.query import SimpleStatement
from test.pylib.skip_types import skip_env

from .util import new_test_table, unique_name

# id -> nickname. Rows 1 and 4 hold every gram of "hello" (min_gram 2, max_gram 3) without
# containing it, which is what a candidate the coordinator has to drop looks like.
NICKNAMES = {
    0: "hello",
    1: "hxllo hel",
    2: "hello world",
    3: "say hello",
    4: "hell lo",
}


def contains_response(ids, next_cursor=None, verified=None):
    body = {"primary_keys": {"id": ids}}
    if next_cursor is not None:
        body["next_cursor"] = next_cursor
    if verified is not None:
        body["verified"] = verified
    return json.dumps(body)


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Substring Search needs tablets enabled")


def make_table(cql, test_keyspace, options):
    table = test_keyspace + "." + unique_name()
    idx = unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, nickname text, registered_at timestamp)")
    cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}(nickname) USING 'substring_index' WITH OPTIONS = {options}")
    for i, nickname in NICKNAMES.items():
        cql.execute(f"INSERT INTO {table} (id, nickname, registered_at) VALUES ({i}, '{nickname}', {1000 + i})")
    return table


@pytest.fixture(scope="module")
def sensitive_table(cql, test_keyspace):
    """A case-sensitive ordered index: the coordinator can repeat the node's test, so it does."""
    table = make_table(cql, test_keyspace, "{'min_gram': '2', 'order_by': 'registered_at'}")
    yield table
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="module")
def unordered_table(cql, test_keyspace):
    """A case-sensitive index with no sort column: a page the check left short could not resume,
    since such an index reports no cursor, so the node keeps checking its own candidates."""
    table = make_table(cql, test_keyspace, "{'min_gram': '2'}")
    yield table
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="module")
def insensitive_table(cql, test_keyspace):
    """A case-insensitive index lowercases on the node with tables the coordinator does not
    share, so the node keeps checking its own candidates."""
    table = make_table(cql, test_keyspace, "{'min_gram': '2', 'order_by': 'registered_at', 'case_sensitive': 'false'}")
    yield table
    cql.execute(f"DROP TABLE {table}")


def test_a_long_keyword_is_checked_by_the_coordinator(cql, sensitive_table, vector_store_mock):
    """Five characters against max_gram 3: the request declines verification, the node answers
    with candidates, and the ones that do not hold the keyword are dropped -- in the node's order,
    and without the value showing up in the result."""
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([4, 3, 2, 1, 0], verified=False))

    rows = list(cql.execute(f"SELECT id FROM {sensitive_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert [row.id for row in rows] == [3, 2, 0]
    assert rows[0]._fields == ("id",)
    requests = vector_store_mock.contains_requests
    assert len(requests) == 1
    assert requests[0].verify is False


def test_a_short_keyword_is_left_to_the_index(cql, sensitive_table, vector_store_mock):
    """Within max_gram the grams are exact, so the request says nothing about verification and
    the page is taken as it is."""
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([1, 0]))

    rows = list(cql.execute(f"SELECT id FROM {sensitive_table} WHERE nickname LIKE '%he%' LIMIT 10"))

    assert [row.id for row in rows] == [1, 0]
    assert vector_store_mock.contains_requests[0].verify is None


def test_a_page_the_node_verified_is_taken_as_it_is(cql, sensitive_table, vector_store_mock):
    """The node's word is final when it says the page is verified -- an older node that never
    heard of the flag says nothing, which means the same -- so nothing is re-checked here."""
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([1, 0]))

    rows = list(cql.execute(f"SELECT id FROM {sensitive_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert [row.id for row in rows] == [1, 0]


def test_a_case_insensitive_index_keeps_checking_its_own_candidates(cql, insensitive_table, vector_store_mock):
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([1, 0]))

    rows = list(cql.execute(f"SELECT id FROM {insensitive_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert [row.id for row in rows] == [1, 0]
    assert vector_store_mock.contains_requests[0].verify is None


def test_a_prefix_and_a_suffix_are_checked_where_they_anchor(cql, sensitive_table, vector_store_mock):
    """Three characters plus the node's anchor mark is past max_gram 3, so these are checked here,
    each against its own end of the value."""
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([3, 2, 0], verified=False))
    rows = list(cql.execute(f"SELECT id FROM {sensitive_table} WHERE nickname LIKE 'hel%' LIMIT 10"))
    assert [row.id for row in rows] == [2, 0]
    assert vector_store_mock.contains_requests[0].verify is False

    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([4, 3, 1, 0], verified=False))
    rows = list(cql.execute(f"SELECT id FROM {sensitive_table} WHERE nickname LIKE '%llo' LIMIT 10"))
    assert [row.id for row in rows] == [3, 0]


def test_a_bound_pattern_is_checked_by_its_own_length(cql, sensitive_table, vector_store_mock):
    """A bind marker's pattern is known at execution only; the decision follows the bound value."""
    stmt = cql.prepare(f"SELECT id FROM {sensitive_table} WHERE nickname LIKE ? LIMIT 10")

    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([1, 0], verified=False))
    assert [row.id for row in cql.execute(stmt, ["%hello%"])] == [0]
    assert vector_store_mock.contains_requests[0].verify is False

    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([1, 0]))
    assert [row.id for row in cql.execute(stmt, ["%he%"])] == [1, 0]
    assert vector_store_mock.contains_requests[0].verify is None


def test_a_short_page_is_topped_up_from_the_cursor(cql, sensitive_table, vector_store_mock):
    """Filtering left the first node page one row short, so the coordinator asks for that one row
    from the node's cursor before answering; the page's own cursor is the last node page's, and
    the LIMIT counts the rows returned, not the candidates read."""
    vector_store_mock.reset()
    vector_store_mock.set_contains_responses([
        (200, contains_response([1, 0], next_cursor="after-0", verified=False)),
        (200, contains_response([2], next_cursor="after-2", verified=False)),
        (200, contains_response([3], verified=False)),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {sensitive_table} WHERE nickname LIKE '%hello%' ORDER BY registered_at DESC LIMIT 10",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [0, 2, 3]

    requests = vector_store_mock.contains_requests
    assert [(r.cursor, json.loads(r.body)["limit"]) for r in requests] == [
        (None, 2),
        ("after-0", 1),
        ("after-2", 2),
    ]


def test_top_ups_are_bounded_and_a_short_page_is_still_a_page(cql, sensitive_table, vector_store_mock):
    """A keyword whose candidates keep failing is not chased forever: after the bounded top-ups
    the page goes out as it is, empty here, with a cursor, and the client's next page continues."""
    vector_store_mock.reset()
    vector_store_mock.set_contains_responses([
        (200, contains_response([4], next_cursor="c1", verified=False)),
        (200, contains_response([4], next_cursor="c2", verified=False)),
        (200, contains_response([4], next_cursor="c3", verified=False)),
        (200, contains_response([4], next_cursor="c4", verified=False)),
        (200, contains_response([0], verified=False)),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {sensitive_table} WHERE nickname LIKE '%hello%' ORDER BY registered_at DESC LIMIT 10",
        fetch_size=1,
    )
    assert [row.id for row in cql.execute(statement)] == [0]
    requests = vector_store_mock.contains_requests
    assert len(requests) == 5
    assert requests[4].cursor == "c4"


def test_an_unordered_index_keeps_checking_its_own_candidates(cql, unordered_table, vector_store_mock):
    """Dropping a candidate here would lose a row for good on an index with no cursor to resume
    from, so the request leaves verification to the node."""
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([2, 0]))

    rows = list(cql.execute(f"SELECT id FROM {unordered_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert sorted(row.id for row in rows) == [0, 2]
    assert vector_store_mock.contains_requests[0].verify is None


def test_an_unpaged_query_is_topped_up_until_its_limit_is_met(cql, sensitive_table, vector_store_mock):
    """An unpaged client reads one reply and ignores any cursor, so a short reply would be a lost
    row. The top-ups are not bounded then: they run until the LIMIT is met or the node runs out."""
    vector_store_mock.reset()
    vector_store_mock.set_contains_responses([
        (200, contains_response([4], next_cursor="c1", verified=False)),
        (200, contains_response([4], next_cursor="c2", verified=False)),
        (200, contains_response([4], next_cursor="c3", verified=False)),
        (200, contains_response([4], next_cursor="c4", verified=False)),
        (200, contains_response([3], next_cursor="c5", verified=False)),
        (200, contains_response([0], verified=False)),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {sensitive_table} WHERE nickname LIKE '%hello%' ORDER BY registered_at DESC LIMIT 2",
        fetch_size=None,
    )
    assert [row.id for row in cql.execute(statement)] == [3, 0]
    assert len(vector_store_mock.contains_requests) == 6
