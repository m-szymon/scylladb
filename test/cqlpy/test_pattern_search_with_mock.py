# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for pattern search query execution.
#
# These tests use the shared Vector Store mock from vector_store_mock.py to
# verify that Scylla translates a `LIKE` on a pattern-indexed column into
# an HTTP POST to the `/like` endpoint of the Vector Store service, and reads
# the returned rows from the base table.
#
# They run on an index without 'order_by', which keeps table order and pages,
# through the same statement as an index with 'order_by' columns
# (test_pattern_ordering_with_mock.py).
###############################################################################

import json

import pytest
from cassandra.protocol import InvalidRequest
from test.pylib.skip_types import skip_env
from cassandra.query import SimpleStatement
from http import HTTPStatus

from .util import new_test_table, unique_name

NUM_ROWS = 5


def like_response(ids):
    return json.dumps({"primary_keys": {"id": ids}})


@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Pattern Search needs tablets enabled by default")


@pytest.fixture(scope="module")
def pattern_table(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    idx = unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, title text)")
    cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}(title) USING 'pattern_index'")
    for i in range(NUM_ROWS):
        cql.execute(f"INSERT INTO {table} (id, title) VALUES ({i}, 'hello')")
    yield table, idx
    cql.execute(f"DROP TABLE {table}")


@pytest.fixture(scope="function")
def pattern_setup_with_mock(pattern_table, vector_store_mock):
    table, idx = pattern_table
    vector_store_mock.set_next_like_response(200, like_response([3, 1]))
    return table, idx


def test_like_executes_through_the_index(cql, pattern_setup_with_mock):
    """A LIKE returns the rows the index node named, without ALLOW FILTERING."""
    table, _ = pattern_setup_with_mock

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT {NUM_ROWS}"))
    assert sorted(r.id for r in rows) == [1, 3]


def test_like_request_sent_to_correct_endpoint(cql, test_keyspace, vector_store_mock, pattern_setup_with_mock):
    """Scylla must POST to /api/v1/indexes/<ks>/<idx>/like with the pattern and the limit."""
    table, idx = pattern_setup_with_mock

    cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT 67")

    reqs = vector_store_mock.like_requests
    assert len(reqs) == 1
    assert reqs[0].path == f"/api/v1/indexes/{test_keyspace}/{idx}/like"
    body = json.loads(reqs[0].body)
    assert body["pattern"] == "%ell%"
    assert body["limit"] == 67


PATTERNS = ["%ell%", "ell%", "%ell", "h%o", "h_llo", "%e\\%l%", "%e\\_l%", "hello", "%", ""]


@pytest.mark.parametrize("pattern", PATTERNS)
def test_like_pattern_sent_as_written(cql, vector_store_mock, pattern_setup_with_mock, pattern):
    """The index node decides what a pattern matches, so Scylla sends every pattern as it is."""
    table, _ = pattern_setup_with_mock

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE title LIKE '{pattern}' LIMIT {NUM_ROWS}"))
    assert sorted(r.id for r in rows) == [1, 3]
    assert json.loads(vector_store_mock.like_requests[-1].body)["pattern"] == pattern


@pytest.mark.parametrize("pattern", PATTERNS)
def test_like_bind_marker_sent_as_bound(cql, vector_store_mock, pattern_setup_with_mock, pattern):
    table, _ = pattern_setup_with_mock

    stmt = cql.prepare(f"SELECT id FROM {table} WHERE title LIKE ? LIMIT {NUM_ROWS}")
    rows = list(cql.execute(stmt, [pattern]))
    assert sorted(r.id for r in rows) == [1, 3]
    assert json.loads(vector_store_mock.like_requests[-1].body)["pattern"] == pattern


def test_like_bind_marker_null_fails(cql, pattern_setup_with_mock):
    table, _ = pattern_setup_with_mock

    stmt = cql.prepare(f"SELECT id FROM {table} WHERE title LIKE ? LIMIT {NUM_ROWS}")
    with pytest.raises(InvalidRequest, match="The LIKE pattern of a pattern search query must not be null"):
        cql.execute(stmt, [None])


def test_like_limit_exceeds_max_raises_error(cql, pattern_setup_with_mock):
    """An unpaged query's LIMIT exceeding the external search maximum (1000) must be rejected;
    1000 is accepted."""
    table, _ = pattern_setup_with_mock

    with pytest.raises(InvalidRequest, match="1000"):
        cql.execute(SimpleStatement(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT 1001", fetch_size=None))
    cql.execute(SimpleStatement(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT 1000", fetch_size=None))


def test_like_http_error_propagated_as_invalid_request(cql, vector_store_mock, pattern_setup_with_mock):
    """An HTTP error from the vector store must be surfaced as an InvalidRequest, not answered by a scan."""
    table, _ = pattern_setup_with_mock
    vector_store_mock.set_next_like_response(HTTPStatus.NOT_FOUND, "index does not exist")

    with pytest.raises(InvalidRequest, match="404.*index does not exist"):
        cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT {NUM_ROWS}")


@pytest.mark.parametrize("body", ["not json", "[]", '{"scores": []}', '{"primary_keys": {"id": "no"}}'])
def test_like_malformed_reply_is_an_error(cql, vector_store_mock, pattern_setup_with_mock, body):
    table, _ = pattern_setup_with_mock
    vector_store_mock.set_next_like_response(200, body)

    with pytest.raises(InvalidRequest):
        cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT {NUM_ROWS}")


def test_like_stale_key_is_skipped(cql, vector_store_mock, pattern_setup_with_mock):
    """A key the index still holds but the base table no longer has is simply absent from the result."""
    table, _ = pattern_setup_with_mock
    vector_store_mock.set_next_like_response(200, like_response([2, 4242]))

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT {NUM_ROWS}"))
    assert [r.id for r in rows] == [2]


def test_like_no_match_returns_no_rows(cql, vector_store_mock, pattern_setup_with_mock):
    table, _ = pattern_setup_with_mock
    vector_store_mock.set_next_like_response(200, like_response([]))

    assert list(cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT {NUM_ROWS}")) == []


def test_like_reply_over_limit_is_truncated(cql, vector_store_mock, pattern_setup_with_mock):
    """Keys beyond the LIMIT in a reply from the index node are dropped."""
    table, _ = pattern_setup_with_mock
    vector_store_mock.set_next_like_response(200, like_response([3, 1, 2]))

    rows = list(cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' LIMIT 2"))
    assert sorted(r.id for r in rows) == [1, 3]


@pytest.mark.parametrize("column_type", ["text", "varchar", "ascii"])
def test_like_executes_on_column_type(cql, test_keyspace, vector_store_mock, column_type):
    """A pattern on a column of each supported type reaches the index node as it is."""
    with new_test_table(cql, test_keyspace, f"id int primary key, title {column_type}") as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        cql.execute(f"INSERT INTO {table} (id, title) VALUES (1, 'hello')")

        vector_store_mock.set_next_like_response(200, like_response([1]))
        rows = list(cql.execute(f"SELECT id, title FROM {table} WHERE title LIKE '%ell%' LIMIT 10"))
        assert [(r.id, r.title) for r in rows] == [(1, 'hello')]
        assert json.loads(vector_store_mock.like_requests[-1].body)["pattern"] == "%ell%"


def test_like_multibyte_patterns(cql, test_keyspace, vector_store_mock):
    """Multibyte patterns are sent to the index node as they are."""
    with new_test_table(cql, test_keyspace, "id int primary key, title text") as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        cql.execute(f"INSERT INTO {table} (id, title) VALUES (1, 'café')")

        for pattern in ["%café%", "%é", "caf_"]:
            vector_store_mock.set_next_like_response(200, like_response([1]))
            rows = list(cql.execute(f"SELECT id, title FROM {table} WHERE title LIKE '{pattern}' LIMIT 10"))
            assert [(r.id, r.title) for r in rows] == [(1, 'café')]
            assert json.loads(vector_store_mock.like_requests[-1].body)["pattern"] == pattern


# --- an index without order_by: table order, paged --------------------------------


@pytest.fixture(scope="module")
def table_order_table(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    idx = unique_name()
    cql.execute(f"CREATE TABLE {table} (id int primary key, title text)")
    cql.execute(f"CREATE CUSTOM INDEX {idx} ON {table}(title) USING 'pattern_index'")
    for i in range(NUM_ROWS):
        cql.execute(f"INSERT INTO {table} (id, title) VALUES ({i}, 'hello')")
    yield table, idx
    cql.execute(f"DROP TABLE {table}")


def test_an_index_without_order_by_pages_in_the_order_the_node_keeps(cql, table_order_table, vector_store_mock):
    """The index node keeps table order and pages it with a cursor; the request names no direction,
    and the rows come back in the order the node named them, page after page."""
    table, _ = table_order_table
    vector_store_mock.set_like_responses([
        (200, json.dumps({"primary_keys": {"id": [1, 0]}, "next_cursor": "after-0"})),
        (200, json.dumps({"primary_keys": {"id": [2, 4]}, "next_cursor": "after-4"})),
        (200, like_response([3])),
    ])

    statement = SimpleStatement(f"SELECT id FROM {table} WHERE title LIKE '%ell%'", fetch_size=2)
    assert [row.id for row in cql.execute(statement)] == [1, 0, 2, 4, 3]
    requests = vector_store_mock.like_requests
    assert [request.cursor for request in requests] == [None, "after-0", "after-4"]
    assert all(request.order is None for request in requests)


def test_an_index_without_order_by_cannot_be_ordered(cql, table_order_table):
    table, _ = table_order_table
    with pytest.raises(InvalidRequest, match="return rows in table order"):
        cql.execute(f"SELECT id FROM {table} WHERE title LIKE '%ell%' ORDER BY id DESC LIMIT {NUM_ROWS}")
