# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for ORDER BY on a substring-indexed query.
#
# A substring index created with an 'order_by' option answers newest-first, and a
# query against it may say ORDER BY <that column> DESC. The ordering is done by
# the index node, so what is checked here is the CQL surface -- which clauses are
# accepted, which are refused, and that the order the index node chose survives
# the base-table read -- rather than the ordering itself, which is the index
# node's own test.
###############################################################################

import json

import pytest
from cassandra.protocol import InvalidRequest
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


def test_order_by_a_different_column_is_rejected(cql, ordered_table):
    """Only the column the index was created with can be ordered by: the index has no other
    column's values to order on."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match="can only be ordered by"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY id DESC LIMIT {NUM_ROWS}")


def test_ascending_order_is_rejected(cql, ordered_table):
    """The index node walks its sort column downwards; ascending would need the other direction,
    and answering in the wrong order is worse than refusing."""
    table, _ = ordered_table
    with pytest.raises(InvalidRequest, match="only be ordered DESC"):
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at ASC LIMIT {NUM_ROWS}")


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


def test_a_cursor_in_the_reply_is_accepted(cql, ordered_table, vector_store_mock):
    """The index node reports where the next page resumes. Nothing pages on it yet -- the cursor is
    read and dropped -- but a reply carrying one must not be rejected as malformed."""
    table, _ = ordered_table
    vector_store_mock.set_next_contains_response(200, contains_response([4, 3], next_cursor=1003))

    rows = list(
        cql.execute(f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}")
    )

    assert [row.id for row in rows] == [4, 3]


def test_a_malformed_cursor_is_an_error(cql, ordered_table, vector_store_mock):
    """A cursor that is present but not a number means the two sides disagree about the protocol,
    which is worth failing on rather than silently paging from nowhere."""
    table, _ = ordered_table
    vector_store_mock.set_next_contains_response(200, json.dumps({"primary_keys": {"id": [1]}, "next_cursor": "soon"}))

    with pytest.raises(Exception):
        list(
            cql.execute(
                f"SELECT id FROM {table} WHERE nickname LIKE '%ell%' ORDER BY registered_at DESC LIMIT {NUM_ROWS}"
            )
        )
