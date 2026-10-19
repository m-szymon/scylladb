# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for where a pattern-indexed query's matches are checked.
#
# For a long keyword the index node has candidates -- rows holding every
# indexed substring of the keyword -- rather than matches, and something has to
# check them against the value. By default the index node does. An ordered,
# case-sensitive index created with 'verify_candidates': 'scylla' leaves that
# to ScyllaDB instead: every request for it declines verification
# (`"verify": false`), and when the node flags a page as candidates
# (`"verified": false`) ScyllaDB applies the whole LIKE pattern to the value it
# reads with the row anyway. A page that comes back short is topped up from the
# node's cursor a bounded number of times, then handed over short, which a
# paging client takes in its stride.
#
# The index node is the Vector Store mock from vector_store_mock.py, so what is
# checked is the CQL side: which requests say what, which rows survive, and how
# paging and the LIMIT account for the rows dropped. Which option combinations
# are accepted is tested in test_pattern_index.py.
###############################################################################

import json

import pytest
from cassandra.query import SimpleStatement
from test.pylib.skip_types import skip_env

from .util import unique_name

# id -> nickname. Rows 1 and 4 hold every two- and three-letter substring of "hello" without
# containing it, which is what a candidate ScyllaDB has to drop looks like.
NICKNAMES = {
    0: "hello",
    1: "hxllo hel",
    2: "hello world",
    3: "say hello",
    4: "hell lo",
}


def like_response(ids, next_cursor=None, verified=None):
    body = {"primary_keys": {"id": ids}}
    if next_cursor is not None:
        body["next_cursor"] = next_cursor
    if verified is not None:
        body["verified"] = verified
    return json.dumps(body)


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Pattern Search needs tablets enabled by default")


def make_table(cql, test_keyspace, options):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, nickname text, registered_at timestamp)")
    cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'pattern_index' WITH OPTIONS = {options}")
    for i, nickname in NICKNAMES.items():
        cql.execute(f"INSERT INTO {table} (id, nickname, registered_at) VALUES ({i}, '{nickname}', {1000 + i})")
    return table


@pytest.fixture(scope="module")
def scylla_checks_table(cql, test_keyspace):
    """An ordered, case-sensitive index that leaves the check to ScyllaDB."""
    table = make_table(cql, test_keyspace, "{'order_by': 'registered_at', 'verify_candidates': 'scylla'}")
    yield table
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="module")
def default_table(cql, test_keyspace):
    """An ordered index created with no word on verification: the index node checks."""
    table = make_table(cql, test_keyspace, "{'order_by': 'registered_at'}")
    yield table
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="module")
def index_checks_table(cql, test_keyspace):
    """An ordered index that says outright that the index node checks."""
    table = make_table(cql, test_keyspace, "{'order_by': 'registered_at', 'verify_candidates': 'index'}")
    yield table
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="module")
def unordered_table(cql, test_keyspace):
    """An index with no sort column, which answers in one unordered page."""
    table = make_table(cql, test_keyspace, "{}")
    yield table
    cql.execute(f"DROP TABLE {table}")


# --- the index node checks --------------------------------------------------------


@pytest.mark.parametrize("fixture", ["default_table", "index_checks_table"])
def test_by_default_the_index_node_checks(cql, request, vector_store_mock, fixture):
    """The request says nothing about verification, and the page is taken as it is: row 1 does not
    hold "hello", but the node named it, so it is returned."""
    table = request.getfixturevalue(fixture)
    vector_store_mock.set_next_like_response(200, like_response([2, 1, 0]))

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert [row.id for row in rows] == [2, 1, 0]
    requests = vector_store_mock.like_requests
    assert len(requests) == 1
    assert "verify" not in json.loads(requests[0].body)


def test_an_unordered_index_leaves_the_check_to_the_index_node(cql, unordered_table, vector_store_mock):
    vector_store_mock.set_next_like_response(200, like_response([2, 0]))

    rows = list(cql.execute(f"SELECT id FROM {unordered_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert sorted(row.id for row in rows) == [0, 2]
    assert "verify" not in json.loads(vector_store_mock.like_requests[0].body)


# --- ScyllaDB checks --------------------------------------------------------------


def test_scylla_checks_the_candidates_the_node_did_not(cql, scylla_checks_table, vector_store_mock):
    """The request declines verification, the node answers with candidates, and the ones that do
    not hold the keyword are dropped -- in the node's order, and without the value showing up in
    the result."""
    vector_store_mock.set_next_like_response(200, like_response([4, 3, 2, 1, 0], verified=False))

    rows = list(cql.execute(f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert [row.id for row in rows] == [3, 2, 0]
    assert rows[0]._fields == ("id",)
    requests = vector_store_mock.like_requests
    assert len(requests) == 1
    assert requests[0].verify is False


def test_the_checked_value_may_also_be_selected(cql, scylla_checks_table, vector_store_mock):
    vector_store_mock.set_next_like_response(200, like_response([4, 0], verified=False))

    rows = list(cql.execute(f"SELECT nickname FROM {scylla_checks_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert [row.nickname for row in rows] == ["hello"]


@pytest.mark.parametrize("pattern", ["%he%", "%hello%", "hel%", "%llo world"])
def test_every_request_declines_verification(cql, scylla_checks_table, vector_store_mock, pattern):
    """ScyllaDB does not know which keywords the node can answer exactly, so it declines verification
    whatever the pattern, and leaves it to the node to say whether it checked."""
    vector_store_mock.set_next_like_response(200, like_response([]))

    list(cql.execute(f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '{pattern}' LIMIT 10"))

    assert vector_store_mock.like_requests[0].verify is False


@pytest.mark.parametrize("verified", [None, True], ids=["absent", "true"])
def test_a_page_the_node_verified_is_taken_as_it_is(cql, scylla_checks_table, vector_store_mock, verified):
    """The node's word is final when it says the page is verified -- a node that says nothing means
    the same -- so nothing is re-checked here, and row 1 is returned although it does not match."""
    vector_store_mock.set_next_like_response(200, like_response([1, 0], verified=verified))

    rows = list(cql.execute(f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '%hello%' LIMIT 10"))

    assert [row.id for row in rows] == [1, 0]


@pytest.mark.parametrize("pattern, candidates, matches", [
    ("hel%", [3, 2, 0], [2, 0]),
    ("%llo", [4, 3, 1, 0], [3, 0]),
    ("h%o", [0, 1, 2, 3], [0]),
    ("h_llo%", [4, 1, 0], [1, 0]),
    ("%HELLO%", [3, 2, 0], []),
], ids=["prefix", "suffix", "inner_wildcard", "underscore", "letter_case"])
def test_the_whole_pattern_is_applied_as_like_does(cql, scylla_checks_table, vector_store_mock, pattern, candidates, matches):
    """The check is CQL's own LIKE on the whole pattern: anchored where the pattern anchors, with
    `_` for one character, and letter case counting."""
    vector_store_mock.set_next_like_response(200, like_response(candidates, verified=False))

    rows = list(cql.execute(f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '{pattern}' LIMIT 10"))

    assert [row.id for row in rows] == matches
    assert vector_store_mock.like_requests[0].pattern == pattern


def test_a_bound_pattern_is_checked_as_bound(cql, scylla_checks_table, vector_store_mock):
    """A bind marker's pattern is known at execution only; the check uses the bound value."""
    stmt = cql.prepare(f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE ? LIMIT 10")

    vector_store_mock.set_next_like_response(200, like_response([1, 0], verified=False))
    assert [row.id for row in cql.execute(stmt, ["%hello%"])] == [0]
    assert vector_store_mock.like_requests[-1].pattern == "%hello%"

    vector_store_mock.set_next_like_response(200, like_response([1, 0], verified=False))
    assert [row.id for row in cql.execute(stmt, ["%he%"])] == [1, 0]
    assert vector_store_mock.like_requests[-1].pattern == "%he%"


# --- topping up a short page -------------------------------------------------------


def test_a_short_page_is_topped_up_from_the_cursor(cql, scylla_checks_table, vector_store_mock):
    """Filtering left the first node page one row short, so ScyllaDB asks for that one row from the
    node's cursor before answering; the page's own cursor is the last node page's, and the LIMIT
    counts the rows returned, not the candidates read."""
    vector_store_mock.set_like_responses([
        (200, like_response([1, 0], next_cursor="after-0", verified=False)),
        (200, like_response([2], next_cursor="after-2", verified=False)),
        (200, like_response([3], verified=False)),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '%hello%' ORDER BY registered_at DESC LIMIT 10",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [0, 2, 3]

    requests = vector_store_mock.like_requests
    assert [(r.cursor, r.limit) for r in requests] == [
        (None, 2),
        ("after-0", 1),
        ("after-2", 2),
    ]
    assert all(r.verify is False for r in requests)


def test_top_ups_are_bounded_and_a_short_page_is_still_a_page(cql, scylla_checks_table, vector_store_mock):
    """A keyword whose candidates keep failing is not chased forever: after three top-ups the page
    goes out as it is, empty here, with a cursor, and the client's next page continues."""
    vector_store_mock.set_like_responses([
        (200, like_response([4], next_cursor="c1", verified=False)),
        (200, like_response([4], next_cursor="c2", verified=False)),
        (200, like_response([4], next_cursor="c3", verified=False)),
        (200, like_response([4], next_cursor="c4", verified=False)),
        (200, like_response([0], verified=False)),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '%hello%' ORDER BY registered_at DESC LIMIT 10",
        fetch_size=1,
    )
    assert [row.id for row in cql.execute(statement)] == [0]
    requests = vector_store_mock.like_requests
    assert len(requests) == 5
    assert requests[4].cursor == "c4"


def test_an_unpaged_query_is_topped_up_until_its_limit_is_met(cql, scylla_checks_table, vector_store_mock):
    """An unpaged client reads one reply and ignores any cursor, so a short reply would be a lost
    row. The top-ups are not bounded then: they run until the LIMIT is met or the node runs out."""
    vector_store_mock.set_like_responses([
        (200, like_response([4], next_cursor="c1", verified=False)),
        (200, like_response([4], next_cursor="c2", verified=False)),
        (200, like_response([4], next_cursor="c3", verified=False)),
        (200, like_response([4], next_cursor="c4", verified=False)),
        (200, like_response([3], next_cursor="c5", verified=False)),
        (200, like_response([0], verified=False)),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '%hello%' ORDER BY registered_at DESC LIMIT 2",
        fetch_size=None,
    )
    assert [row.id for row in cql.execute(statement)] == [3, 0]
    assert len(vector_store_mock.like_requests) == 6


def test_a_short_page_without_a_cursor_is_the_last_one(cql, scylla_checks_table, vector_store_mock):
    """With no cursor there is nothing to top up from: the short page is the whole rest."""
    vector_store_mock.set_next_like_response(200, like_response([4, 0], verified=False))

    statement = SimpleStatement(
        f"SELECT id FROM {scylla_checks_table} WHERE nickname LIKE '%hello%' ORDER BY registered_at DESC LIMIT 10",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [0]
    assert len(vector_store_mock.like_requests) == 1
