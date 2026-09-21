# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for substring indexes
#
# This file tests the substring_index custom index class: schema and options
# validation, and the prepare-time validation of the LIKE queries it serves.
# Queries that reach the Vector Store are covered by
# test_substring_search_with_mock.py.
###############################################################################

import pytest
from test.pylib.skip_types import skip_env
from .util import new_test_table, new_test_keyspace, unique_name
from cassandra.protocol import InvalidRequest


# Substring search is not allowed in tables using vnodes, so all tests in this file need tablets
@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Substring Search needs tablets enabled")


@pytest.mark.parametrize("column_type", ["text", "varchar", "ascii"])
def test_create_substring_index_on_supported_text_column(cql, test_keyspace, column_type):
    """Substring index should accept all textual CQL columns."""
    schema = f'p int primary key, nickname {column_type}'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index'")


def test_create_substring_index_uppercase_class(cql, test_keyspace):
    """Custom index class name lookup is case-insensitive."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'SUBSTRING_INDEX'")


@pytest.mark.parametrize("column_type", ["int", "blob", "list<text>", "vector<float, 3>"])
def test_create_substring_index_on_unsupported_column_fails(cql, test_keyspace, column_type):
    """Substring index must reject non-text column types."""
    schema = f'p int primary key, v {column_type}'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Substring index is only supported on text, varchar, or ascii columns"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'substring_index'")


def test_create_substring_index_on_key_or_static_column_fails(cql, test_keyspace):
    """In its first version the substring index serves regular columns only."""
    schema = 'p text, c text, s text static, v text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        for column in ['p', 'c', 's']:
            with pytest.raises(InvalidRequest, match="Substring index is only supported on regular columns"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}({column}) USING 'substring_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'substring_index'")


def test_create_substring_index_with_valid_options(cql, test_keyspace):
    """All options accepted together; boolean values are case-insensitive."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
            f"WITH OPTIONS = {{'min_gram': '2', 'max_gram': '5', 'case_sensitive': 'FALSE'}}"
        )


@pytest.mark.parametrize("option,value,error", [
    ("min_gram", "0", "out of valid range"),
    ("min_gram", "9", "out of valid range"),
    ("max_gram", "-1", "out of valid range"),
    ("max_gram", "three", "is not an integer"),
    ("max_gram", "3.5", "is not an integer"),
    ("case_sensitive", "maybe", "Invalid value in option 'case_sensitive'"),
])
def test_create_substring_index_with_bad_option_value_fails(cql, test_keyspace, option, value, error):
    """Each option validates its value."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=error):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
                f"WITH OPTIONS = {{'{option}': '{value}'}}"
            )


def test_create_substring_index_min_gram_above_max_gram_fails(cql, test_keyspace):
    """The two gram lengths must be consistent, whether both or only one is given."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=r"min_gram \(4\) must not be greater than max_gram \(3\)"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
                f"WITH OPTIONS = {{'min_gram': '4', 'max_gram': '3'}}"
            )
        # Only min_gram given: compared against the default max_gram of 3.
        with pytest.raises(InvalidRequest, match=r"min_gram \(4\) must not be greater than max_gram \(3\)"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
                f"WITH OPTIONS = {{'min_gram': '4'}}"
            )
        # Equal is fine.
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
            f"WITH OPTIONS = {{'min_gram': '3', 'max_gram': '3'}}"
        )


def test_create_substring_index_with_unsupported_option_fails(cql, test_keyspace):
    """Unknown WITH OPTIONS keys should be rejected."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Unsupported option analyzer for substring index"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index' "
                f"WITH OPTIONS = {{'analyzer': 'standard'}}"
            )


def test_no_view_for_substring_index(cql, test_keyspace):
    """A substring index lives on the index node, not in a materialized view."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(nickname) USING 'substring_index'")
        views = list(cql.execute(f"SELECT view_name FROM system_schema.views WHERE keyspace_name = '{test_keyspace}'"))
        assert not any(v.view_name == f"{index_name}_index" for v in views)


def test_describe_substring_index(cql, test_keyspace):
    """DESCRIBE reproduces the class and the options."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(nickname) USING 'substring_index' "
            f"WITH OPTIONS = {{'min_gram': '2', 'case_sensitive': 'false'}}"
        )
        desc = cql.execute(f"DESCRIBE INDEX {test_keyspace}.{index_name}").one().create_statement
        assert "USING 'substring_index'" in desc
        assert "'min_gram': '2'" in desc
        assert "'case_sensitive': 'false'" in desc


def test_substring_index_in_system_schema(cql, test_keyspace):
    """The index is recorded with its class and options, the way the Vector Store reads them."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(nickname) USING 'substring_index' "
            f"WITH OPTIONS = {{'max_gram': '4'}}"
        )
        table_name = table.split('.')[1]
        row = cql.execute(
            f"SELECT kind, options FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' "
            f"AND table_name = '{table_name}' AND index_name = '{index_name}'").one()
        assert row.kind == 'CUSTOM'
        assert row.options['class_name'] == 'substring_index'
        assert row.options['target'] == 'nickname'
        assert row.options['max_gram'] == '4'


def test_create_substring_index_requires_tablets(cql, this_dc):
    """Substring index creation must fail when the keyspace does not use tablets."""
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}") as ks:
        with new_test_table(cql, ks, 'p int primary key, nickname text') as table:
            with pytest.raises(InvalidRequest, match="Creating a substring index requires the base table's keyspace to use tablets"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index'")


def test_create_substring_index_cdc_low_ttl_fails(cql, test_keyspace):
    """Substring index creation must fail when CDC TTL is below the 24-hour minimum."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema, " WITH cdc = {'enabled': true, 'ttl': 1}") as table:
        with pytest.raises(InvalidRequest, match="CDC's TTL must be at least"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index'")


def test_create_substring_index_cdc_bad_delta_mode_fails(cql, test_keyspace):
    """Substring index creation must fail when CDC delta mode is not 'full' and postimage is off."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema, " WITH cdc = {'enabled': true, 'delta': 'keys'}") as table:
        with pytest.raises(InvalidRequest, match="delta mode must be set to 'full' or postimage must be enabled"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index'")


def test_cannot_disable_cdc_with_substring_index(cql, test_keyspace):
    """ALTER TABLE to disable CDC must fail when a substring index exists on the table."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index'")
        with pytest.raises(InvalidRequest, match="Cannot disable CDC when Substring Search is enabled"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")


def test_alter_cdc_low_ttl_with_substring_index_fails(cql, test_keyspace):
    """ALTER TABLE to set CDC TTL below the 24-hour minimum must fail when a substring index exists."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(nickname) USING 'substring_index'")
        with pytest.raises(InvalidRequest, match="CDC's TTL must be at least"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': true, 'ttl': 1}}")


def test_drop_substring_index(cql, test_keyspace):
    """DROP INDEX on a substring index should succeed, and CDC can then be disabled again."""
    schema = 'p int primary key, nickname text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(nickname) USING 'substring_index'")
        cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")
