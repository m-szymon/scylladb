# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for ORDER BY on a substring-indexed query.
#
# A substring index created with an 'order_by' option answers in sort-column
# order, and a query against it may say ORDER BY <that column> ASC or DESC. The
# ordering is done by the index node, so what is checked here is the CQL surface
# -- which clauses are accepted, which are refused, which direction is sent to
# the index node, and that the order the index node chose survives the
# base-table read -- rather than the ordering itself, which is the index node's
# own test.
#
# The cursor a page hands back is the index node's own: an opaque string that
# Scylla carries into the paging state and back into the next request unchanged.
###############################################################################

import json

import pytest
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement
from test.pylib.skip_types import skip_env

from .util import new_test_table, unique_name

NUM_ROWS = 5


def contains_response(ids, next_cursor=None):
    body = {"primary_keys": {"id": ids}}
    if next_cursor is not None:
        body["next_cursor"] = next_cursor
    return json.dumps(body)


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Substring Search needs tablets enabled")


@pytest.fixture(scope="module")
def ordered_table(cql, test_keyspace):
    """A substring index that was given a column to order by."""
    table = test_keyspace + "." + unique_name()
    idx = unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, nickname text, registered_at timestamp)")
    cql.execute(
        f"CREATE CUSTOM INDEX {idx} ON {table}(nickname) USING 'substring_index' "
        f"WITH OPTIONS = {{'min_gram': '2', 'order_by': 'registered_at'}}"
    )
    for i in range(NUM_ROWS):
        cql.execute(f"INSERT INTO {table} (id, nickname, registered_at) VALUES ({i}, 'hello', {1000 + i})")
    yield table, idx
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="module")
def unordered_table(cql, test_keyspace):
    """A substring index with no sort column: it can serve no ORDER BY at all."""
    table = test_keyspace + "." + unique_name()
    idx = unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, nickname text, registered_at timestamp)")
    cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}(nickname) USING 'substring_index' WITH OPTIONS = {{'min_gram': '2'}}")
    for i in range(NUM_ROWS):
        cql.execute(f"INSERT INTO {table} (id, nickname, registered_at) VALUES ({i}, 'hello', {1000 + i})")
    yield table, idx
    cql.execute(f"DROP TABLE {table}")


# --- the index option ---------------------------------------------------------


def test_order_by_must_name_a_column_of_the_table(cql, test_keyspace):
    """A sort column that does not exist is refused at creation. Accepting it would leave the
    index answering in its own order while the query claims another."""
    schema = "id int primary key, nickname text, registered_at timestamp"
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="not in the table"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
                f"WITH OPTIONS = {{'order_by': 'no_such_column'}}"
            )


def test_order_by_must_name_an_orderable_column(cql, test_keyspace):
    """The sort value is carried as a fixed-width integer, so a text column has no ordering the
    index can use. The index node applies the same rule, and the two must agree."""
    schema = "id int primary key, nickname text, registered_at timestamp"
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="only integer and time columns"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
                f"WITH OPTIONS = {{'order_by': 'nickname'}}"
            )


def test_order_by_option_must_not_be_empty(cql, test_keyspace):
    schema = "id int primary key, nickname text, registered_at timestamp"
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="must name a column"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' WITH OPTIONS = {{'order_by': ''}}"
            )


# --- the ORDER BY clause ------------------------------------------------------


def test_order_by_the_index_column_is_accepted(cql, ordered_table, vector_store_mock):
    """The rows come back in the order the index node named, not in the base table's."""
    table, _ = ordered_table
    vector_store_mock.set_next_contains_response(200, contains_response([3, 1, 4]))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")
    )

    assert [row.id for row in rows] == [3, 1, 4]


def test_descending_order_is_sent_to_the_index_node(cql, ordered_table, vector_store_mock):
    """ORDER BY ... DESC is passed on as 'order': 'desc'. The node would default to it anyway, but
    a query that names a direction should not rely on the default staying what it is."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([4, 3]))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")
    )

    assert [row.id for row in rows] == [4, 3]
    assert [request.order for request in vector_store_mock.contains_requests] == ["desc"]


def test_ascending_order_is_accepted_and_sent_to_the_index_node(cql, ordered_table, vector_store_mock):
    """ORDER BY ... ASC is passed on as 'order': 'asc', and the rows are returned as the index node
    ordered them: Scylla does not re-sort a page it did not order."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([0, 1, 2]))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at ASC LIMIT {NUM_ROWS}")
    )

    assert [row.id for row in rows] == [0, 1, 2]
    assert [request.order for request in vector_store_mock.contains_requests] == ["asc"]


def test_a_query_without_order_by_names_no_direction(cql, ordered_table, vector_store_mock):
    """Without an ORDER BY there is no direction to send, so the field is left out and the index
    node applies its own default. A range on the ordered column does not change that."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([2, 0]))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at < 1005 LIMIT {NUM_ROWS}")
    )

    assert sorted(row.id for row in rows) == [0, 2]
    requests = vector_store_mock.contains_requests
    assert len(requests) == 1
    assert requests[0].order is None
    assert "order" not in json.loads(requests[0].body)
    assert "max_sort_value" in json.loads(requests[0].body)


def test_a_range_travels_as_the_columns_own_values(cql, ordered_table, vector_store_mock):
    """A bound is sent as a value of the ordered column, in the JSON encoding of its type, with
    whether it is inclusive; the index node turns it into its sort key. ScyllaDB holds no copy of
    that encoding, so there is nothing here that could disagree with the node."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([3]))

    list(cql.execute(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at >= 1002 AND registered_at < 1005 "
        f"ORDER BY registered_at DESC LIMIT {NUM_ROWS}"))

    body = json.loads(vector_store_mock.contains_requests[0].body)
    assert "min_sort_key" not in body and "max_sort_key" not in body
    # registered_at is a timestamp: 1002 ms after the epoch, as the type's JSON spells it.
    assert body["min_sort_value"]["inclusive"] is True
    assert body["min_sort_value"]["value"].startswith("1970-01-01") and "00:00:01.002" in body["min_sort_value"]["value"]
    assert body["max_sort_value"]["inclusive"] is False
    assert "00:00:01.005" in body["max_sort_value"]["value"]


def test_two_bounds_on_one_side_collapse_to_the_tighter(cql, ordered_table, vector_store_mock):
    """`< 1005 AND < 1003` is `< 1003`; the node gets one bound a side."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([2]))

    list(cql.execute(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at < 1005 AND registered_at <= 1003 "
        f"ORDER BY registered_at DESC LIMIT {NUM_ROWS}"))

    body = json.loads(vector_store_mock.contains_requests[0].body)
    assert "min_sort_value" not in body
    assert "00:00:01.003" in body["max_sort_value"]["value"]
    assert body["max_sort_value"]["inclusive"] is True


def test_order_by_a_different_column_is_rejected(cql, ordered_table):
    """Only the column the index was created with can be ordered by: the index has no other
    column's values to order on."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match="can only be ordered by"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY id DESC LIMIT {NUM_ROWS}")


def test_multiple_orderings_are_rejected(cql, ordered_table):
    """The index has one sort column, so there is nothing a second ordering could be applied to."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match="ordering by a single column"):
        cql.execute(
            f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC, id ASC LIMIT {NUM_ROWS}"
        )


def test_order_by_without_the_index_option_is_rejected(cql, unordered_table):
    """An index with no sort column carries no value to order on, so it can serve no ORDER BY."""
    table, _ = unordered_table
    with pytest.raises(InvalidRequest, match="created with an 'order_by' option"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")


def test_an_ordered_index_still_serves_a_query_without_order_by(cql, ordered_table, vector_store_mock):
    """ORDER BY is optional. Without it the rows are still whatever the index node returned -- an
    unspecified order permits any order, including the sorted one."""
    table, _ = ordered_table
    vector_store_mock.set_next_contains_response(200, contains_response([2, 0]))

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' LIMIT {NUM_ROWS}"))

    assert sorted(row.id for row in rows) == [0, 2]


def test_limit_is_still_required_and_capped_when_ordering(cql, ordered_table):
    """Ordering does not loosen the limits: the index node is asked for a bounded page either way."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match="require a LIMIT"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC")
    with pytest.raises(InvalidRequest, match="not greater than 1000"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 1001")


def test_a_cursor_in_the_reply_ends_the_query_when_the_limit_is_spent(cql, ordered_table, vector_store_mock):
    """A cursor says only where the next page would start. With the LIMIT already spent there is no
    next page, so it is dropped and the client is told the result is complete."""
    table, _ = ordered_table
    vector_store_mock.set_next_contains_response(200, contains_response([4, 3], next_cursor="1003:3"))

    result = cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 2")

    assert [row.id for row in result] == [4, 3]
    assert len(vector_store_mock.contains_requests) == 1


def test_a_malformed_cursor_is_an_error(cql, ordered_table, vector_store_mock):
    """A cursor that is present but not a string means the two sides disagree about the protocol,
    which is worth failing on rather than silently paging from nowhere."""
    table, _ = ordered_table
    vector_store_mock.set_next_contains_response(200, json.dumps({"primary_keys": {"id": [1]}, "next_cursor": 1003}))

    with pytest.raises(Exception):
        list(
            cql.execute(
                f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}"
            )
        )


# --- paging --------------------------------------------------------------------


def test_pages_resume_from_the_cursor(cql, ordered_table, vector_store_mock):
    """Each page asks the index node to carry on from where the last one stopped, and the pages
    together are the whole result in order."""
    table, _ = ordered_table
    vector_store_mock.reset()
    # The cursors are whatever the index node chose to say; Scylla must hand them back verbatim.
    vector_store_mock.set_contains_responses([
        (200, contains_response([4, 3], next_cursor="1003:3")),
        (200, contains_response([2, 1], next_cursor="1001:1")),
        (200, contains_response([0])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3, 2, 1, 0]

    requests = vector_store_mock.contains_requests
    bodies = [json.loads(request.body) for request in requests]
    assert len(bodies) == 3
    assert "cursor" not in bodies[0]
    assert [body["cursor"] for body in bodies[1:]] == ["1003:3", "1001:1"]
    assert [request.cursor for request in requests] == [None, "1003:3", "1001:1"]
    # Every page carries the direction, so the node walks the same way on each.
    assert [request.order for request in requests] == ["desc"] * 3
    # Each request asks for a page, not for the whole LIMIT; the last one asks only for what the
    # LIMIT still owes.
    assert [body["limit"] for body in bodies] == [2, 2, 1]


def test_ascending_pages_resume_from_the_cursor(cql, ordered_table, vector_store_mock):
    """Paging is the same walk in the other direction: the cursor goes back verbatim, and every
    page asks for ascending order so the node does not turn around halfway."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_contains_responses([
        (200, contains_response([0, 1], next_cursor="1001:1")),
        (200, contains_response([2, 3], next_cursor="1003:3")),
        (200, contains_response([4])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at ASC LIMIT {NUM_ROWS}",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [0, 1, 2, 3, 4]

    requests = vector_store_mock.contains_requests
    assert [request.cursor for request in requests] == [None, "1001:1", "1003:3"]
    assert [request.order for request in requests] == ["asc"] * 3


def test_a_page_without_a_cursor_is_the_last_one(cql, ordered_table, vector_store_mock):
    """The index node omits the cursor when it has nothing left to walk, and that ends the query
    even though the LIMIT is not spent."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_contains_responses([(200, contains_response([4, 3]))])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3]
    assert len(vector_store_mock.contains_requests) == 1


def test_paging_stops_at_the_limit(cql, ordered_table, vector_store_mock):
    """The LIMIT bounds the whole query, not each page: a node that keeps offering a cursor does not
    get to return more rows than were asked for."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_contains_responses([
        (200, contains_response([4, 3], next_cursor="1003:3")),
        (200, contains_response([2], next_cursor="1002:2")),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 3",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3, 2]
    assert len(vector_store_mock.contains_requests) == 2


def test_a_range_is_repeated_on_every_page(cql, ordered_table, vector_store_mock):
    """The range is part of the query, so the second page has to be filtered by it too -- the cursor
    narrows where the walk starts, it does not replace the restriction."""
    table, _ = ordered_table
    vector_store_mock.reset()
    vector_store_mock.set_contains_responses([
        (200, contains_response([4], next_cursor="1004:4")),
        (200, contains_response([3])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at < 1005 "
        f"ORDER BY registered_at DESC LIMIT {NUM_ROWS}",
        fetch_size=1,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3]

    bodies = [json.loads(request.body) for request in vector_store_mock.contains_requests]
    assert len(bodies) == 2
    assert all("max_sort_value" in body for body in bodies)
    assert bodies[0]["max_sort_value"] == bodies[1]["max_sort_value"]


def test_an_unordered_index_does_not_page(cql, unordered_table, vector_store_mock):
    """Without a sort column the walk has no position to resume from, so the query keeps its old
    shape: the whole result in one page, and a warning saying so."""
    table, _ = unordered_table
    vector_store_mock.reset()
    vector_store_mock.set_next_contains_response(200, contains_response([0, 1, 2, 3]))

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' LIMIT {NUM_ROWS}", fetch_size=2
    )
    result = cql.execute(statement)

    assert sorted(row.id for row in result) == [0, 1, 2, 3]
    assert len(vector_store_mock.contains_requests) == 1
