# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for ORDER BY, ranges and paging on a pattern-indexed query.
#
# A pattern index created with 'order_by' columns answers in their order, then
# in table order, and serves the queries that ORDER BY a prefix of those
# columns, all in their declared directions or all reversed, as a query on a
# table may order by its clustering columns. Such a query may restrict the first
# column to a range, and is paged. A query without ORDER BY means what a
# filtered LIKE means -- table order -- and is served by an index without
# order_by (table order), never by one with columns. The ordering is done by the index node, so what is checked here is
# the CQL surface -- which clauses are accepted, which are refused, which index
# is chosen, what is sent to the `/like` endpoint of the Vector Store mock from
# vector_store_mock.py -- and that the order the index node chose survives the
# base-table read. The ordering itself is the index node's own test.
#
# The cursor a page hands back is the index node's own: an opaque string that
# ScyllaDB carries into the paging state and back into the next request
# unchanged.
#
# The table order of an index without 'order_by' is covered by
# test_pattern_search_with_mock.py.
###############################################################################

import json
import time

import pytest
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement
from http import HTTPStatus
from test.pylib.skip_types import skip_env

from .util import new_test_table, unique_name

NUM_ROWS = 5


def like_response(ids, next_cursor=None):
    body = {"primary_keys": {"id": ids}}
    if next_cursor is not None:
        body["next_cursor"] = next_cursor
    return json.dumps(body)


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Pattern Search needs tablets enabled by default")


@pytest.fixture(scope="module")
def ordered_table(cql, test_keyspace):
    """A pattern index that was given a column to order by."""
    table = test_keyspace + "." + unique_name()
    idx = unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, nickname text, registered_at timestamp, bio text)")
    cql.execute(
        f"CREATE CUSTOM INDEX {idx} ON {table}(nickname) USING 'pattern_index' "
        f"WITH OPTIONS = {{'order_by': 'registered_at'}}"
    )
    for i in range(NUM_ROWS):
        cql.execute(f"INSERT INTO {table} (id, nickname, registered_at) VALUES ({i}, 'hello', {1000 + i})")
    yield table, idx
    cql.execute(f"DROP TABLE {table}")


# --- the request ----------------------------------------------------------------


@pytest.mark.parametrize("pattern", ["%ell%", "ell%", "%ell", "h_llo"])
def test_the_pattern_is_sent_as_written(cql, ordered_table, vector_store_mock, pattern):
    """The index node decides what a pattern matches, so the ordered path sends it as it is, to
    the `/like` endpoint."""
    table, idx = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([1]))

    list(cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '{pattern}' ORDER BY registered_at DESC LIMIT {NUM_ROWS}"))

    requests = vector_store_mock.like_requests
    assert len(requests) == 1
    assert requests[0].path.endswith(f"/{idx}/like")
    assert requests[0].pattern == pattern


def test_a_pattern_the_index_node_refuses_fails_the_query(cql, ordered_table, vector_store_mock):
    """ScyllaDB does not judge the pattern; the index node refuses one it cannot serve with a 400,
    and the query fails with its message rather than falling back to a scan."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(HTTPStatus.BAD_REQUEST, "unsupported pattern")

    with pytest.raises(InvalidRequest, match="400.*unsupported pattern"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%a_b%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")


def test_a_null_pattern_is_refused(cql, ordered_table):
    table, _ = ordered_table
    stmt = cql.prepare(f"SELECT id FROM {table} WHERE nickname LIKE ? ORDER BY registered_at DESC LIMIT {NUM_ROWS}")
    with pytest.raises(InvalidRequest, match="must not be null"):
        cql.execute(stmt, [None])


# --- the ORDER BY clause ------------------------------------------------------


def test_order_by_the_index_column_is_accepted(cql, ordered_table, vector_store_mock):
    """The rows come back in the order the index node named, not in the base table's."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([3, 1, 4]))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")
    )

    assert [row.id for row in rows] == [3, 1, 4]


def test_descending_order_is_sent_to_the_index_node(cql, ordered_table, vector_store_mock):
    """ORDER BY ... DESC is passed on as 'order': 'desc'. The node would default to it anyway, but
    a query that names a direction should not rely on the default staying what it is."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([4, 3]))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")
    )

    assert [row.id for row in rows] == [4, 3]
    assert [request.order for request in vector_store_mock.like_requests] == ["desc"]


def test_ascending_order_is_accepted_and_sent_to_the_index_node(cql, ordered_table, vector_store_mock):
    """ORDER BY ... ASC is passed on as 'order': 'asc', and the rows are returned as the index node
    ordered them: ScyllaDB does not re-sort a page it did not order."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([0, 1, 2]))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at ASC LIMIT {NUM_ROWS}")
    )

    assert [row.id for row in rows] == [0, 1, 2]
    assert [request.order for request in vector_store_mock.like_requests] == ["asc"]


def test_a_query_without_order_by_is_refused(cql, ordered_table, vector_store_mock):
    """A LIKE without ORDER BY returns rows in table order, as a filtered LIKE does, and an index
    ordered by a column does not keep that order, so it does not serve the query."""
    table, _ = ordered_table
    for where in ["nickname LIKE '%ell%'", "nickname LIKE '%ell%' AND registered_at < 1005"]:
        with pytest.raises(InvalidRequest, match="need an ORDER BY"):
            cql.execute(f"SELECT id FROM {table} WHERE {where} LIMIT {NUM_ROWS}")
    assert vector_store_mock.like_requests == []


def test_order_by_a_different_column_is_rejected(cql, ordered_table):
    """Only the columns the index was created with can be ordered by: the index has no other
    column's values to order on."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match=r"can only be ordered by the index's columns \(registered_at ASC\)"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY id DESC LIMIT {NUM_ROWS}")


def test_more_orderings_than_the_index_has_columns_are_rejected(cql, ordered_table):
    """The index has one ordering column, so there is nothing a second ordering could be applied
    to; ties on it come in table order."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match="not by more"):
        cql.execute(
            f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC, id ASC LIMIT {NUM_ROWS}"
        )


@pytest.fixture(scope="module")
def two_column_table(cql, test_keyspace):
    """An index ordered by two columns with opposite directions."""
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, nickname text, a int, b int)")
    cql.execute(
        f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'pattern_index' WITH OPTIONS = {{'order_by': 'a DESC, b'}}"
    )
    for i in range(NUM_ROWS):
        cql.execute(f"INSERT INTO {table} (id, nickname, a, b) VALUES ({i}, 'hello', {i % 2}, {i})")
    yield table
    cql.execute(f"DROP TABLE {table}")


@pytest.mark.parametrize("order_by, sent", [
    ("a DESC", "desc"),
    ("a DESC, b ASC", "desc"),
    ("a ASC, b DESC", "asc"),
    ("a ASC", "asc"),
])
def test_a_prefix_of_the_index_columns_declared_or_reversed_is_served(cql, two_column_table, vector_store_mock, order_by, sent):
    """As for clustering columns: a prefix of the declared order, all as declared or all reversed.
    The node is told the direction of the first column and walks the order or its reverse."""
    vector_store_mock.set_next_like_response(200, like_response([1]))

    list(cql.execute(f"SELECT id FROM {two_column_table} WHERE nickname LIKE '%ell%' ORDER BY {order_by} LIMIT {NUM_ROWS}"))

    assert [request.order for request in vector_store_mock.like_requests] == [sent]


@pytest.mark.parametrize("order_by, message", [
    ("a DESC, b DESC", "in their declared directions"),
    ("a ASC, b ASC", "in their declared directions"),
    ("b ASC", "in that order"),
    ("b ASC, a DESC", "in that order"),
])
def test_other_orderings_of_the_index_columns_are_rejected(cql, two_column_table, order_by, message):
    with pytest.raises(InvalidRequest, match=message):
        cql.execute(f"SELECT id FROM {two_column_table} WHERE nickname LIKE '%ell%' ORDER BY {order_by} LIMIT {NUM_ROWS}")


def test_only_the_first_ordering_column_may_be_ranged(cql, two_column_table, vector_store_mock):
    vector_store_mock.set_next_like_response(200, like_response([]))
    list(cql.execute(
        f"SELECT id FROM {two_column_table} WHERE nickname LIKE '%ell%' AND a > 0 ORDER BY a DESC LIMIT {NUM_ROWS}"))
    assert vector_store_mock.like_requests[0].min_sort_value == {"value": 0, "inclusive": False}
    with pytest.raises(InvalidRequest, match="first ordered column a"):
        cql.execute(f"SELECT id FROM {two_column_table} WHERE nickname LIKE '%ell%' AND b > 0 ORDER BY a DESC LIMIT {NUM_ROWS}")


def test_the_index_whose_order_fits_the_query_serves_it(cql, test_keyspace, vector_store_mock):
    """Of several pattern indexes on one column, a query without ORDER BY goes to the one in table
    order, and one with ORDER BY to the one ordered that way."""
    with new_test_table(cql, test_keyspace, "id int primary key, nickname text, registered_at timestamp") as table:
        by_time = unique_name()
        table_order = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {by_time} ON {table}(nickname) USING 'pattern_index' WITH OPTIONS = {{'order_by': 'registered_at'}}")
        cql.execute(f"CREATE CUSTOM INDEX {table_order} ON {table}(nickname) USING 'pattern_index'")
        vector_store_mock.set_next_like_response(200, like_response([]))

        list(cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' LIMIT {NUM_ROWS}"))
        list(cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}"))

        paths = [request.path for request in vector_store_mock.like_requests]
        assert paths[0].endswith(f"/{table_order}/like")
        assert paths[1].endswith(f"/{by_time}/like")


# --- the WHERE clause -----------------------------------------------------------


@pytest.mark.parametrize("where, message", [
    ("id = 1 AND nickname LIKE '%ell%'", "do not support additional WHERE restrictions"),
    ("nickname LIKE '%ell%' AND bio = 'x'", "cannot restrict bio"),
    ("nickname LIKE '%ell%' AND registered_at = 1002", "only range restrictions"),
    ("nickname LIKE '%ell%' AND nickname LIKE '%llo%'", "only a single LIKE restriction"),
], ids=["partition_key", "other_column", "equality_on_ordered_column", "two_likes"])
def test_only_the_like_and_a_range_on_the_ordered_column_may_be_restricted(cql, ordered_table, where, message):
    """Anything the index node cannot apply is refused rather than silently left out."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match=message):
        cql.execute(f"SELECT id FROM {table} WHERE {where} ORDER BY registered_at DESC LIMIT {NUM_ROWS}")


@pytest.mark.parametrize("select, clause, message", [
    ("id", "PER PARTITION LIMIT 1", "do not support per-partition limits"),
    ("id", "GROUP BY id", "cannot be run with aggregation"),
    ("COUNT(*)", "", "cannot be run with aggregation"),
], ids=["per_partition_limit", "group_by", "count"])
def test_an_ordered_query_cannot_group_or_aggregate(cql, ordered_table, select, clause, message):
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match=message):
        cql.execute(f"SELECT {select} FROM {table} WHERE nickname LIKE '%ell%' {clause} LIMIT {NUM_ROWS}")


def test_a_range_travels_as_the_columns_own_values(cql, ordered_table, vector_store_mock):
    """A bound is sent as a value of the ordered column, in the JSON encoding of its type, with
    whether it is inclusive; the index node turns it into its sort key. ScyllaDB holds no copy of
    that encoding, so there is nothing here that could disagree with the node."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([3]))

    list(cql.execute(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at >= 1002 AND registered_at < 1005 "
        f"ORDER BY registered_at DESC LIMIT {NUM_ROWS}"))

    request = vector_store_mock.like_requests[0]
    # registered_at is a timestamp: 1002 ms after the epoch, as the type's JSON spells it.
    assert request.min_sort_value["inclusive"] is True
    assert request.min_sort_value["value"].startswith("1970-01-01") and "00:00:01.002" in request.min_sort_value["value"]
    assert request.max_sort_value["inclusive"] is False
    assert "00:00:01.005" in request.max_sort_value["value"]


def test_a_range_on_an_integer_column_travels_as_a_number(cql, test_keyspace, vector_store_mock):
    with new_test_table(cql, test_keyspace, "id int primary key, nickname text, rank int") as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'pattern_index' WITH OPTIONS = {{'order_by': 'rank'}}")
        vector_store_mock.set_next_like_response(200, like_response([]))

        list(cql.execute(
            f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND rank > 5 AND rank <= 9 ORDER BY rank DESC LIMIT {NUM_ROWS}"))

        request = vector_store_mock.like_requests[0]
        assert request.min_sort_value == {"value": 5, "inclusive": False}
        assert request.max_sort_value == {"value": 9, "inclusive": True}


def test_two_bounds_on_one_side_collapse_to_the_tighter(cql, ordered_table, vector_store_mock):
    """`< 1005 AND <= 1003` is `<= 1003`; the node gets one bound a side."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([2]))

    list(cql.execute(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at < 1005 AND registered_at <= 1003 "
        f"ORDER BY registered_at DESC LIMIT {NUM_ROWS}"))

    request = vector_store_mock.like_requests[0]
    assert request.min_sort_value is None
    assert "00:00:01.003" in request.max_sort_value["value"]
    assert request.max_sort_value["inclusive"] is True


def test_a_null_bound_is_refused(cql, ordered_table):
    table, _ = ordered_table
    stmt = cql.prepare(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at < ? ORDER BY registered_at DESC LIMIT {NUM_ROWS}")
    with pytest.raises(InvalidRequest, match="must not be null"):
        cql.execute(stmt, [None])


# --- LIMIT and page size --------------------------------------------------------


def test_an_unpaged_query_still_needs_a_bounded_limit(cql, ordered_table):
    """Without paging the whole result comes back in one reply, so the LIMIT is what bounds it:
    required, and at most 1000."""
    table, _ = ordered_table
    unpaged = lambda text: SimpleStatement(text, fetch_size=None)
    with pytest.raises(InvalidRequest, match="require a LIMIT unless they are paged"):
        cql.execute(unpaged(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC"))
    with pytest.raises(InvalidRequest, match="not greater than 1000"):
        cql.execute(unpaged(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 1001"))


def test_a_paged_query_needs_no_limit(cql, ordered_table, vector_store_mock):
    """A page resumes from the node's cursor at a flat cost, so the page bounds each round trip and
    the query may go without a LIMIT: the pages simply run until the node reports no more."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([
        (200, like_response([4, 3], next_cursor="after-3")),
        (200, like_response([2, 1], next_cursor="after-1")),
        (200, like_response([0])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC", fetch_size=2)
    assert [row.id for row in cql.execute(statement)] == [4, 3, 2, 1, 0]
    assert [r.limit for r in vector_store_mock.like_requests] == [2, 2, 2]


def test_a_paged_query_may_ask_for_more_than_1000_rows(cql, ordered_table, vector_store_mock):
    """The 1000 bounds one reply, not a paged query's total."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([4, 3]))

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 5000", fetch_size=100)
    assert [row.id for row in cql.execute(statement)] == [4, 3]


def test_a_large_page_is_cut_to_what_one_reply_may_hold(cql, ordered_table, vector_store_mock):
    """A driver's default page is thousands of rows; the node is asked for at most 1000 of them,
    which is a correct page since a page is a bound, not a promise."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([4, 3]))

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC", fetch_size=5000)
    list(cql.execute(statement))
    assert vector_store_mock.like_requests[0].limit == 1000


def test_one_prepared_statement_is_judged_by_how_it_is_executed(cql, ordered_table, vector_store_mock):
    """Paging is a property of the execution, so the same prepared statement without a LIMIT runs
    paged and is refused unpaged."""
    table, _ = ordered_table
    stmt = cql.prepare(f"SELECT id FROM {table} WHERE nickname LIKE ? ORDER BY registered_at DESC")
    vector_store_mock.set_next_like_response(200, like_response([4]))

    stmt.fetch_size = 10
    assert [row.id for row in cql.execute(stmt, ["%ell%"])] == [4]
    stmt.fetch_size = None
    with pytest.raises(InvalidRequest, match="require a LIMIT unless they are paged"):
        cql.execute(stmt, ["%ell%"])


def test_a_cursor_in_the_reply_ends_the_query_when_the_limit_is_spent(cql, ordered_table, vector_store_mock):
    """A cursor says only where the next page would start. With the LIMIT already spent there is no
    next page, so it is dropped and the client is told the result is complete."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([4, 3], next_cursor="1003:3"))

    result = cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 2")

    assert [row.id for row in result] == [4, 3]
    assert len(vector_store_mock.like_requests) == 1


def test_a_malformed_cursor_is_an_error(cql, ordered_table, vector_store_mock):
    """A cursor that is present but not a string means the two sides disagree about the protocol,
    which is worth failing on rather than silently paging from nowhere."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, json.dumps({"primary_keys": {"id": [1]}, "next_cursor": 1003}))

    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")


# --- paging --------------------------------------------------------------------


def test_pages_resume_from_the_cursor(cql, ordered_table, vector_store_mock):
    """Each page asks the index node to carry on from where the last one stopped, and the pages
    together are the whole result in order."""
    table, _ = ordered_table
    # The cursors are whatever the index node chose to say; ScyllaDB must hand them back verbatim.
    vector_store_mock.set_like_responses([
        (200, like_response([4, 3], next_cursor="1003:3")),
        (200, like_response([2, 1], next_cursor="1001:1")),
        (200, like_response([0])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3, 2, 1, 0]

    requests = vector_store_mock.like_requests
    assert len(requests) == 3
    assert "cursor" not in json.loads(requests[0].body)
    assert [request.cursor for request in requests] == [None, "1003:3", "1001:1"]
    # Every page carries the pattern and the direction, so the node walks the same way on each.
    assert [request.pattern for request in requests] == ["%ell%"] * 3
    assert [request.order for request in requests] == ["desc"] * 3
    # Each request asks for a page, not for the whole LIMIT; the last one asks only for what the
    # LIMIT still owes.
    assert [request.limit for request in requests] == [2, 2, 1]


def test_ascending_pages_resume_from_the_cursor(cql, ordered_table, vector_store_mock):
    """Paging is the same walk in the other direction: the cursor goes back verbatim, and every
    page asks for ascending order so the node does not turn around halfway."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([
        (200, like_response([0, 1], next_cursor="1001:1")),
        (200, like_response([2, 3], next_cursor="1003:3")),
        (200, like_response([4])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at ASC LIMIT {NUM_ROWS}",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [0, 1, 2, 3, 4]

    requests = vector_store_mock.like_requests
    assert [request.cursor for request in requests] == [None, "1001:1", "1003:3"]
    assert [request.order for request in requests] == ["asc"] * 3


def test_a_page_without_a_cursor_is_the_last_one(cql, ordered_table, vector_store_mock):
    """The index node omits the cursor when it has nothing left to walk, and that ends the query
    even though the LIMIT is not spent."""
    table, _ = ordered_table
    vector_store_mock.set_next_like_response(200, like_response([4, 3]))

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3]
    assert len(vector_store_mock.like_requests) == 1


def test_paging_stops_at_the_limit(cql, ordered_table, vector_store_mock):
    """The LIMIT bounds the whole query, not each page: a node that keeps offering a cursor does not
    get to return more rows than were asked for."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([
        (200, like_response([4, 3], next_cursor="1003:3")),
        (200, like_response([2], next_cursor="1002:2")),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 3",
        fetch_size=2,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3, 2]
    assert len(vector_store_mock.like_requests) == 2


def test_a_range_is_repeated_on_every_page(cql, ordered_table, vector_store_mock):
    """The range is part of the query, so the second page has to be filtered by it too -- the cursor
    narrows where the walk starts, it does not replace the restriction."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([
        (200, like_response([4], next_cursor="1004:4")),
        (200, like_response([3])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' AND registered_at < 1005 "
        f"ORDER BY registered_at DESC LIMIT {NUM_ROWS}",
        fetch_size=1,
    )
    assert [row.id for row in cql.execute(statement)] == [4, 3]

    requests = vector_store_mock.like_requests
    assert len(requests) == 2
    assert requests[0].max_sort_value is not None
    assert requests[0].max_sort_value == requests[1].max_sort_value


# --- short pages from the index node ---------------------------------------------
#
# A request to the index node has a budget. When it ends, the node returns the rows it has
# settled -- fewer than asked for, or none -- and a cursor to go on from.


def test_a_page_the_index_node_cut_short_is_topped_up_from_its_cursor(cql, ordered_table, vector_store_mock):
    """The client's page is filled over as many requests as it takes: each from the cursor of the
    one before, asking for what the page still lacks. An empty reply with a cursor is one of them."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([
        (200, like_response([4], next_cursor="a")),
        (200, like_response([], next_cursor="b")),
        (200, like_response([3, 2], next_cursor="c")),
        (200, like_response([1, 0])),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC", fetch_size=3)
    result = cql.execute(statement)
    assert [row.id for row in result.current_rows] == [4, 3, 2]
    assert result.has_more_pages
    requests = vector_store_mock.like_requests
    assert [request.cursor for request in requests] == [None, "a", "b"]
    assert [request.limit for request in requests] == [3, 2, 2]

    result.fetch_next_page()
    assert [row.id for row in result.current_rows] == [1, 0]
    assert not result.has_more_pages
    assert [request.cursor for request in vector_store_mock.like_requests] == [None, "a", "b", "c"]


def test_a_page_that_stays_empty_is_handed_over_with_more_to_come(cql, ordered_table, vector_store_mock):
    """A keyword with many candidates and no match for a long stretch: reply after reply is empty
    with a cursor. The page is topped up for half of the statement's timeout, not a fixed few
    times and not to the timeout's end, and then reaches the client empty, with a paging state:
    the client asks for the next page, and the scan gets to its end over as many as it takes."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([(200, like_response([], next_cursor="further"))])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC USING TIMEOUT 2s",
        fetch_size=3)
    started = time.monotonic()
    result = cql.execute(statement)
    elapsed = time.monotonic() - started
    assert result.current_rows == []
    assert result.has_more_pages
    assert 0.9 <= elapsed < 2.0, elapsed
    requests = vector_store_mock.like_requests
    assert len(requests) > 4
    assert [request.cursor for request in requests[:3]] == [None, "further", "further"]


def test_an_unpaged_query_is_topped_up_until_its_limit_is_met(cql, ordered_table, vector_store_mock):
    """Its one reply has to hold everything the LIMIT asks for: there is no next page to leave
    rows to."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([
        (200, like_response([4], next_cursor="a")),
        (200, like_response([], next_cursor="b")),
        (200, like_response([3, 2], next_cursor="c")),
    ])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 3", fetch_size=None)
    assert [row.id for row in cql.execute(statement)] == [4, 3, 2]
    requests = vector_store_mock.like_requests
    assert [request.cursor for request in requests] == [None, "a", "b"]
    assert [request.limit for request in requests] == [3, 2, 2]


def test_an_unpaged_query_the_index_node_cannot_fill_in_time_fails(cql, ordered_table, vector_store_mock):
    """A short answer would look like a whole one to a client that cannot ask for the rest, so the
    query fails when its timeout passes."""
    table, _ = ordered_table
    vector_store_mock.set_like_responses([(200, like_response([], next_cursor="further"))])

    statement = SimpleStatement(
        f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT 3 USING TIMEOUT 500ms",
        fetch_size=None)
    with pytest.raises(InvalidRequest, match="aborted"):
        cql.execute(statement)
    assert len(vector_store_mock.like_requests) > 4
