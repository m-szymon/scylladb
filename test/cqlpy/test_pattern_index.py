# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

###############################################################################
# Tests for pattern indexes
#
# This file tests the pattern_index custom index class: schema and options
# validation, and the prepare-time validation of the LIKE queries it serves.
# Queries that reach the Vector Store are covered by
# test_pattern_search_with_mock.py.
###############################################################################

import pytest
from test.pylib.skip_types import skip_env
from .util import new_test_table, new_test_keyspace, unique_name
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement


# Pattern search is not allowed in tables using vnodes, so all tests in this file need tablets
@pytest.fixture(scope="module", autouse=True)
def all_tests_are_tablets_and_scylla_only(scylla_only, has_tablets):
    if not has_tablets:
        skip_env("Pattern Search needs tablets enabled by default")


def test_create_pattern_index_on_supported_text_column(cql, test_keyspace):
    """Pattern index should accept all textual CQL columns."""
    column_types = ["text", "varchar", "ascii"]
    schema = 'p int primary key, ' + ', '.join(f'v_{t} {t}' for t in column_types)
    with new_test_table(cql, test_keyspace, schema) as table:
        for t in column_types:
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(v_{t}) USING 'pattern_index'")


def test_create_pattern_index_uppercase_class(cql, test_keyspace):
    """Custom index class name lookup is case-insensitive."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'PATTERN_INDEX'")


def test_create_pattern_index_on_unsupported_column_fails(cql, test_keyspace):
    """Pattern index must reject non-text column types."""
    column_types = {'v_int': 'int', 'v_blob': 'blob', 'v_list': 'list<text>', 'v_vector': 'vector<float, 3>'}
    schema = 'p int primary key, ' + ', '.join(f'{c} {t}' for c, t in column_types.items())
    with new_test_table(cql, test_keyspace, schema) as table:
        for column in column_types:
            with pytest.raises(InvalidRequest, match="Pattern index is only supported on text, varchar, or ascii columns"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}({column}) USING 'pattern_index'")


def test_create_pattern_index_on_key_or_static_column_fails(cql, test_keyspace):
    """Only regular columns can be indexed: key and static columns are rejected."""
    schema = 'p1 text, p2 text, c text, s text static, v text, PRIMARY KEY ((p1, p2), c)'
    # The clustering column c is DESC on purpose. Internally, the type of a DESC
    # column is reversed(text), which the index's type check doesn't accept as
    # text. If the index checked the type before checking that the column is
    # regular, c would be refused as "only supported on text, varchar, or ascii
    # columns", although it is a text column. The index checks that the column
    # is regular first, so c is refused as "only supported on regular columns".
    with new_test_table(cql, test_keyspace, schema, "WITH CLUSTERING ORDER BY (c DESC)") as table:
        for column in ['p1', 'c', 's']:
            with pytest.raises(InvalidRequest, match="Pattern index is only supported on regular columns"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}({column}) USING 'pattern_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(v) USING 'pattern_index'")


def test_create_pattern_index_with_valid_options(cql, test_keyspace):
    """case_sensitive takes a boolean in any letter case."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for value in ['true', 'false', 'TRUE', 'False']:
            index_name = unique_name()
            cql.execute(
                f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' "
                f"WITH OPTIONS = {{'case_sensitive': '{value}'}}"
            )
            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")


def test_create_pattern_index_with_bad_option_value_fails(cql, test_keyspace):
    """case_sensitive must be a boolean."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="Invalid value in option 'case_sensitive'"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' "
                f"WITH OPTIONS = {{'case_sensitive': 'maybe'}}"
            )


def test_create_pattern_index_with_unsupported_option_fails(cql, test_keyspace):
    """Unknown WITH OPTIONS keys should be rejected. Option names are case-sensitive,
    so 'CASE_SENSITIVE' is unknown too."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for option in ['bad_option', 'CASE_SENSITIVE']:
            with pytest.raises(InvalidRequest, match=f"Unsupported option {option} for pattern index"):
                cql.execute(
                    f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' "
                    f"WITH OPTIONS = {{'{option}': 'true'}}"
                )


def test_no_view_for_pattern_index(cql, test_keyspace):
    """A pattern index lives on the index node, not in a materialized view."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        views = list(cql.execute(
            f"SELECT view_name FROM system_schema.views "
            f"WHERE keyspace_name = '{test_keyspace}' AND base_table_name = '{table.split('.')[1]}' ALLOW FILTERING"))
        assert views == []


def test_describe_pattern_index(cql, test_keyspace):
    """DESCRIBE reproduces the class and the options."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' "
            f"WITH OPTIONS = {{'case_sensitive': 'false'}}"
        )
        desc = cql.execute(f"DESCRIBE INDEX {test_keyspace}.{index_name}").one().create_statement
        assert "USING 'pattern_index'" in desc
        assert "'case_sensitive': 'false'" in desc


def test_pattern_index_in_system_schema(cql, test_keyspace):
    """The index is recorded with its class and options, the way the Vector Store reads them."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(
            f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' "
            f"WITH OPTIONS = {{'case_sensitive': 'false'}}"
        )
        table_name = table.split('.')[1]
        row = cql.execute(
            f"SELECT kind, options FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' "
            f"AND table_name = '{table_name}' AND index_name = '{index_name}'").one()
        assert row.kind == 'CUSTOM'
        assert row.options['class_name'] == 'pattern_index'
        assert row.options['target'] == 'title'
        assert row.options['case_sensitive'] == 'false'


def test_create_pattern_index_requires_tablets(cql, this_dc):
    """Pattern index creation must fail when the keyspace does not use tablets."""
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}") as ks:
        with new_test_table(cql, ks, 'p int primary key, title text') as table:
            with pytest.raises(InvalidRequest, match="Creating a pattern index requires the base table's keyspace to use tablets"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")


def test_create_pattern_index_cdc_low_ttl_fails(cql, test_keyspace):
    """Pattern index creation must fail when CDC TTL is below the 24-hour minimum."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema, " WITH cdc = {'enabled': true, 'ttl': 1}") as table:
        with pytest.raises(InvalidRequest, match="CDC's TTL must be at least"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")


def test_create_pattern_index_cdc_bad_delta_mode_fails(cql, test_keyspace):
    """Pattern index creation must fail when CDC delta mode is not 'full' and postimage is off."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema, " WITH cdc = {'enabled': true, 'delta': 'keys'}") as table:
        with pytest.raises(InvalidRequest, match="delta mode must be set to 'full' or postimage must be enabled"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")


def test_cannot_disable_cdc_with_pattern_index(cql, test_keyspace):
    """ALTER TABLE to disable CDC must fail when a pattern index exists on the table."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="Cannot disable CDC when Pattern Search is enabled"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")


def test_alter_cdc_low_ttl_with_pattern_index_fails(cql, test_keyspace):
    """ALTER TABLE to set CDC TTL below the 24-hour minimum must fail when a pattern index exists."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="CDC's TTL must be at least"):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': true, 'ttl': 1}}")


def test_drop_pattern_index(cql, test_keyspace):
    """DROP INDEX on a pattern index should succeed, and CDC can then be disabled again."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index'")
        cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")
        cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': false}}")


def test_drop_column_with_pattern_index_fails(cql, test_keyspace):
    """A column with a pattern index cannot be dropped until the index is
    dropped. Reproduces SCYLLADB-4875."""
    schema = 'p int primary key, title text, other text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match=f"Cannot drop column title from base table {table} because it has dependent indexes \\({index_name}\\)"):
            cql.execute(f"ALTER TABLE {table} DROP title")
        cql.execute(f"ALTER TABLE {table} DROP other")
        cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")
        cql.execute(f"ALTER TABLE {table} DROP title")


###############################################################################
# Prepare-time validation of LIKE queries on a pattern-indexed column. Nothing
# below reaches the Vector Store: the queries are prepared, not executed, or are
# rejected before the index node is asked. Some tests also show that creating
# a pattern index changes what an existing LIKE query does.
###############################################################################


@pytest.fixture(scope="module")
def pattern_table(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int primary key, title text, other text)")
    cql.execute(f"INSERT INTO {table} (p, title, other) VALUES (1, 'hello world', 'x')")
    cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
    yield table
    cql.execute(f"DROP TABLE {table}")


def test_like_prepares_without_allow_filtering(cql, pattern_table):
    """Any LIKE on the indexed column is served by the index, so no ALLOW FILTERING is needed."""
    for pattern in ["%ell%", "ell%", "%ell", "h%o", "h_llo", "%e\\%l%", "%e\\_l%", "hello", "%", ""]:
        cql.prepare(f"SELECT * FROM {pattern_table} WHERE title LIKE '{pattern}' LIMIT 10")


def test_like_bind_marker_prepares_without_allow_filtering(cql, pattern_table):
    cql.prepare(f"SELECT * FROM {pattern_table} WHERE title LIKE ? LIMIT 10")


def test_like_requires_limit(cql, pattern_table):
    """A LIKE on the indexed column is served by the index even with ALLOW FILTERING,
    so it needs a LIMIT."""
    for allow_filtering in ["", "ALLOW FILTERING"]:
        with pytest.raises(InvalidRequest, match="must have a limit specified"):
            cql.execute(f"SELECT * FROM {pattern_table} WHERE title LIKE 'ell%' {allow_filtering}")


def test_like_on_column_without_pattern_index_uses_filtering(cql, pattern_table):
    """If a column "title" is indexed, and we try LIKE on a different column
    "other", this query can't use the index and so uses filtering and is only
    allowed with ALLOW FILTERING."""
    with pytest.raises(InvalidRequest, match="ALLOW FILTERING"):
        cql.execute(f"SELECT * FROM {pattern_table} WHERE other LIKE '%x%' LIMIT 10")
    list(cql.execute(f"SELECT * FROM {pattern_table} WHERE other LIKE '%x%' LIMIT 10 ALLOW FILTERING"))


def test_equality_operator_on_indexed_column_uses_filtering(cql, pattern_table):
    """The index serves only LIKE, so an equality on the indexed column is
    filtered as it would be without the index."""
    with pytest.raises(InvalidRequest, match="ALLOW FILTERING"):
        cql.execute(f"SELECT * FROM {pattern_table} WHERE title = 'hello world'")
    assert len(list(cql.execute(f"SELECT * FROM {pattern_table} WHERE title = 'hello world' ALLOW FILTERING"))) == 1


def test_like_on_fulltext_indexed_column_uses_filtering(cql, test_keyspace):
    """A fulltext index does not serve LIKE, so a LIKE on its column uses filtering."""
    schema = 'p int primary key, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        with pytest.raises(InvalidRequest, match="ALLOW FILTERING"):
            cql.execute(f"SELECT * FROM {table} WHERE content LIKE '%hello%' LIMIT 10")


@pytest.mark.parametrize("where, message", [
    ("p = 1 AND title LIKE '%ell%'", "do not support additional WHERE restrictions"),
    ("token(p) > 0 AND title LIKE '%ell%'", "do not support additional WHERE restrictions"),
    ("title LIKE '%ell%' AND other = 'x'", "support exactly one LIKE restriction"),
    ("title LIKE 'h%' AND title LIKE '%d'", "support exactly one LIKE restriction"),
], ids=["partition_key", "token", "other_column", "two_likes"])
def test_like_rejects_additional_restrictions(cql, pattern_table, where, message):
    """The index answers exactly one LIKE; anything else in WHERE is rejected, not filtered."""
    for allow_filtering in ["", "ALLOW FILTERING"]:
        with pytest.raises(InvalidRequest, match=message):
            cql.execute(f"SELECT * FROM {pattern_table} WHERE {where} LIMIT 10 {allow_filtering}")


@pytest.mark.parametrize("allow_filtering", ["", "ALLOW FILTERING"])
def test_like_with_another_restriction_on_its_column_is_not_filtered(cql, pattern_table, allow_filtering):
    """Index selection passes over the pattern index here, but the LIKE is not left to filtering,
    and the query is rejected as a pattern search rather than asked for ALLOW FILTERING."""
    with pytest.raises(InvalidRequest, match="support exactly one LIKE restriction"):
        cql.execute(f"SELECT * FROM {pattern_table} WHERE title LIKE 'h%' AND title > 'a' LIMIT 10 {allow_filtering}")


@pytest.mark.parametrize("where", [
    "c1 = 1 AND title LIKE '%ell%'",
    "c1 > 1 AND title LIKE '%ell%'",
    "(c1, c2) > (1, 2) AND title LIKE '%ell%'",
], ids=["eq", "range", "tuple"])
def test_like_rejects_clustering_restrictions(cql, test_keyspace, where):
    """A restriction on a clustering column is rejected like one on the partition key."""
    schema = 'p int, c1 int, c2 int, title text, PRIMARY KEY (p, c1, c2)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="do not support additional WHERE restrictions"):
            cql.execute(f"SELECT * FROM {table} WHERE {where} LIMIT 10")


def test_like_rejects_bm25_combination(cql, test_keyspace):
    """Pattern and full-text searches cannot be combined."""
    schema = 'p int primary key, title text, content text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(content) USING 'fulltext_index'")
        with pytest.raises(InvalidRequest, match="Pattern search queries cannot be combined with ANN or BM25"):
            cql.execute(f"SELECT * FROM {table} WHERE title LIKE '%ell%' AND BM25(content, 'hello') > 0 "
                        f"ORDER BY BM25(content, 'hello') LIMIT 10")


def test_like_rejects_ann_combination(cql, test_keyspace):
    """Pattern and vector searches cannot be combined."""
    schema = 'p int primary key, title text, vec vector<float, 2>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(vec) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match="Pattern search queries cannot be combined with ANN or BM25"):
            cql.execute(f"SELECT * FROM {table} WHERE title LIKE '%ell%' ORDER BY vec ANN OF [1.0, 2.0] LIMIT 10")


def test_like_rejects_scoring_function(cql, test_keyspace):
    """Pattern search cannot be combined with a search introduced by WHERE either."""
    schema = 'p int primary key, title text, vec vector<float, 2>'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(vec) USING 'vector_index'")
        with pytest.raises(InvalidRequest, match="Pattern search queries cannot be combined with ANN or BM25"):
            cql.execute(f'SELECT * FROM {table} WHERE title LIKE \'%ell%\' AND "ann"(vec, [1.0, 2.0]) > 0 LIMIT 10')


@pytest.mark.parametrize("clause, message", [
    ("ORDER BY p", "do not support ORDER BY"),
    ("PER PARTITION LIMIT 1", "do not support per-partition limits"),
    ("GROUP BY p", "cannot be run with aggregation"),
], ids=["order_by", "per_partition_limit", "group_by"])
def test_like_rejects_ordering_grouping_and_per_partition_limit(cql, pattern_table, clause, message):
    """The rows come back in no particular order and cannot be grouped."""
    with pytest.raises(InvalidRequest, match=message):
        cql.execute(f"SELECT * FROM {pattern_table} WHERE title LIKE '%ell%' {clause} LIMIT 10")


def test_like_rejects_aggregation(cql, pattern_table):
    with pytest.raises(InvalidRequest, match="aggregation"):
        cql.execute(f"SELECT COUNT(*) FROM {pattern_table} WHERE title LIKE '%ell%' LIMIT 10")


def test_creating_pattern_index_breaks_filtered_like(cql, test_keyspace):
    """Without a pattern index, LIKE is a filter, allowed with ALLOW FILTERING.
    Once the index exists, every LIKE on its column is served by the index, even
    with ALLOW FILTERING, so a query without a LIMIT, or with another restriction,
    stops working."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for p, title in [(1, 'hello'), (2, 'help'), (3, 'world')]:
            cql.execute(f"INSERT INTO {table} (p, title) VALUES ({p}, '{title}')")
        without_limit = f"SELECT p FROM {table} WHERE title LIKE 'h%' ALLOW FILTERING"
        with_key = f"SELECT p FROM {table} WHERE p = 1 AND title LIKE 'h%' LIMIT 10 ALLOW FILTERING"
        assert sorted(r.p for r in cql.execute(without_limit)) == [1, 2]
        assert [r.p for r in cql.execute(with_key)] == [1]

        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="Pattern search queries must have a limit specified"):
            cql.execute(without_limit)
        with pytest.raises(InvalidRequest, match="Pattern search queries do not support additional WHERE restrictions"):
            cql.execute(with_key)


def test_dropping_pattern_index_restores_filtered_like(cql, test_keyspace):
    """Dropping the pattern index makes LIKE on its column a filter again."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for p, title in [(1, 'hello'), (2, 'help'), (3, 'world')]:
            cql.execute(f"INSERT INTO {table} (p, title) VALUES ({p}, '{title}')")
        without_limit = f"SELECT p FROM {table} WHERE title LIKE 'h%' ALLOW FILTERING"
        index_name = unique_name()
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="Pattern search queries must have a limit specified"):
            cql.execute(without_limit)

        cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")
        assert sorted(r.p for r in cql.execute(without_limit)) == [1, 2]


def test_create_pattern_index_warns_about_like(cql, test_keyspace):
    """Creating a pattern index warns that every LIKE on its column will be served
    by the index."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        result = cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        warnings = result.response_future.warnings
        assert warnings is not None
        pattern_warnings = [w for w in warnings if "pattern index" in w]
        assert len(pattern_warnings) == 1
        assert "Every LIKE on column title will be served by this pattern index" in pattern_warnings[0]


def test_create_existing_pattern_index_if_not_exists_does_not_warn(cql, test_keyspace):
    """CREATE INDEX IF NOT EXISTS on an existing pattern index changes nothing, so it does not warn."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        create = f"CREATE CUSTOM INDEX IF NOT EXISTS {index_name} ON {table}(title) USING 'pattern_index'"
        cql.execute(create)
        warnings = cql.execute(create).response_future.warnings or []
        assert not [w for w in warnings if "pattern index" in w]


def test_paging_filtered_like_then_create_pattern_index_unprepared(cql, test_keyspace):
    """A filtered LIKE paged before a pattern index was created cannot be continued
    by the index, so the next page is refused. The query has a LIMIT, so the
    index could serve it if it were retried from the beginning."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for p in range(5):
            cql.execute(f"INSERT INTO {table} (p, title) VALUES ({p}, 'hello')")
        stmt = SimpleStatement(f"SELECT p FROM {table} WHERE title LIKE 'h%' LIMIT 100 ALLOW FILTERING", fetch_size=1)
        r = cql.execute(stmt)
        assert r.has_more_pages
        assert r.paging_state is not None

        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="Cannot continue paged query: this query is served by a pattern index"):
            cql.execute(stmt, paging_state=r.paging_state)


def test_paging_filtered_like_then_create_pattern_index_prepared(cql, test_keyspace):
    """As above, with a prepared statement, which the driver prepares again after
    the index changes the schema."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for p in range(5):
            cql.execute(f"INSERT INTO {table} (p, title) VALUES ({p}, 'hello')")
        stmt = cql.prepare(f"SELECT p FROM {table} WHERE title LIKE 'h%' LIMIT 100 ALLOW FILTERING")
        stmt.fetch_size = 1
        r = cql.execute(stmt)
        assert r.has_more_pages
        assert r.paging_state is not None

        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index'")
        with pytest.raises(InvalidRequest, match="Cannot continue paged query: this query is served by a pattern index"):
            cql.execute(stmt, paging_state=r.paging_state)


###############################################################################
# The options of an ordered pattern index and of the index node's segments:
# order_by, min_gram and max_gram, segment_rows, rewrite and verify_candidates.
# Queries on an ordered index are covered by test_pattern_ordering_with_mock.py
# and test_pattern_verification_with_mock.py.
###############################################################################


def test_create_pattern_index_with_all_options(cql, test_keyspace):
    """Every option is accepted together and stored as given, for the index node to read."""
    schema = 'p int primary key, title text, registered_at timestamp'
    options = {
        'case_sensitive': 'true', 'min_gram': '2', 'max_gram': '5', 'order_by': 'registered_at',
        'segment_rows': '1000', 'rewrite': 'false', 'verify_candidates': 'scylla',
    }
    with new_test_table(cql, test_keyspace, schema) as table:
        index_name = unique_name()
        options_cql = ', '.join(f"'{k}': '{v}'" for k, v in options.items())
        cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{{options_cql}}}")
        stored = cql.execute(
            f"SELECT options FROM system_schema.indexes WHERE keyspace_name = '{test_keyspace}' "
            f"AND table_name = '{table.split('.')[1]}' AND index_name = '{index_name}'").one().options
        for k, v in options.items():
            assert stored[k] == v


@pytest.mark.parametrize("option, value", [
    ("min_gram", "1"), ("min_gram", "3"), ("max_gram", "8"),
    ("segment_rows", "0"), ("segment_rows", "200000"),
    ("rewrite", "true"), ("rewrite", "FALSE"),
    ("verify_candidates", "index"),
])
def test_create_pattern_index_with_valid_option_value(cql, test_keyspace, option, value):
    """segment_rows takes 0 for no cap; verify_candidates = 'index' needs nothing else."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'{option}': '{value}'}}")


@pytest.mark.parametrize("option, value, error", [
    ("min_gram", "0", "out of valid range"),
    ("min_gram", "9", "out of valid range"),
    ("max_gram", "-1", "out of valid range"),
    ("max_gram", "three", "is not an integer"),
    ("max_gram", "3.5", "is not an integer"),
    ("segment_rows", "-1", "must be a number of rows"),
    ("segment_rows", "x", "must be a number of rows"),
    ("segment_rows", "", "must be a number of rows"),
    ("segment_rows", "10rows", "must be a number of rows"),
    ("rewrite", "maybe", "Invalid value in option 'rewrite'"),
    ("verify_candidates", "both", "Invalid value in option 'verify_candidates'"),
    ("order_by", "", "must name a column"),
])
def test_create_pattern_index_with_bad_option_value_fails_for_new_options(cql, test_keyspace, option, value, error):
    """Each option validates its value."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=error):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'{option}': '{value}'}}")


def test_create_pattern_index_min_gram_above_max_gram_fails(cql, test_keyspace):
    """The two gram lengths must be consistent, whether both or only one is given."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match=r"min_gram \(4\) must not be greater than max_gram \(3\)"):
            cql.execute(
                f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' "
                f"WITH OPTIONS = {{'min_gram': '4', 'max_gram': '3'}}"
            )
        # Only min_gram given: compared against the default max_gram of 3.
        with pytest.raises(InvalidRequest, match=r"min_gram \(4\) must not be greater than max_gram \(3\)"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'min_gram': '4'}}")
        # Equal is fine.
        cql.execute(
            f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' "
            f"WITH OPTIONS = {{'min_gram': '3', 'max_gram': '3'}}"
        )


def test_create_pattern_index_with_poc_options_fails(cql, test_keyspace):
    """The proof of concept's placeholder options are gone, and are unknown like any other."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        for option in ['poc_option_1', 'poc_option_2', 'poc_option_3', 'poc_option_4']:
            with pytest.raises(InvalidRequest, match=f"Unsupported option {option} for pattern index"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'{option}': 'x'}}")


def test_order_by_accepts_integer_and_time_columns(cql, test_keyspace):
    """The index node keeps the sort value as a fixed-width integer, so these are the types it can order."""
    column_types = ["tinyint", "smallint", "int", "bigint", "timestamp", "date", "time"]
    schema = 'p int primary key, title text, ' + ', '.join(f'v_{t} {t}' for t in column_types)
    with new_test_table(cql, test_keyspace, schema) as table:
        for t in column_types:
            index_name = unique_name()
            cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'order_by': 'v_{t}'}}")
            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")


def test_order_by_rejects_other_column_types(cql, test_keyspace):
    """A column with no order-preserving fixed-width integer image is refused at creation; the index
    node applies the same rule, and the two must agree."""
    column_types = ["text", "boolean", "float", "double", "decimal", "varint", "uuid", "timeuuid", "blob"]
    schema = 'p int primary key, title text, ' + ', '.join(f'v_{t} {t}' for t in column_types)
    with new_test_table(cql, test_keyspace, schema) as table:
        for t in column_types:
            with pytest.raises(InvalidRequest, match="only integer and time columns can be ordered by"):
                cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'order_by': 'v_{t}'}}")
        # The indexed column itself is text, so it cannot be its own sort column either.
        with pytest.raises(InvalidRequest, match="only integer and time columns can be ordered by"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'order_by': 'title'}}")


def test_order_by_must_name_a_column_of_the_table(cql, test_keyspace):
    """A sort column that does not exist is refused at creation. Accepting it would leave the
    index answering in its own order while the query claims another."""
    schema = 'p int primary key, title text'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="no_such_column, which is not in the table"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'order_by': 'no_such_column'}}")


def test_order_by_accepts_key_columns_and_rejects_a_static_column(cql, test_keyspace):
    """The sort value may come from the primary key or a regular column, but not from a static one."""
    schema = 'p int, c bigint, s int static, title text, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        for column in ['p', 'c']:
            index_name = unique_name()
            cql.execute(f"CREATE CUSTOM INDEX {index_name} ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'order_by': '{column}'}}")
            cql.execute(f"DROP INDEX {test_keyspace}.{index_name}")
        with pytest.raises(InvalidRequest, match="(?i)cannot order by column s, which is a static column"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = {{'order_by': 's'}}")


def test_verify_candidates_scylla_needs_a_case_sensitive_ordered_index(cql, test_keyspace):
    """ScyllaDB's LIKE is case-sensitive, and a page it leaves short resumes from a cursor only an
    ordered index reports, so asking ScyllaDB to check anything else is refused when the index is
    created rather than silently ignored."""
    schema = 'p int primary key, title text, registered_at timestamp'
    with new_test_table(cql, test_keyspace, schema) as table:
        with pytest.raises(InvalidRequest, match="needs a case-sensitive index"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = "
                        "{'order_by': 'registered_at', 'case_sensitive': 'false', 'verify_candidates': 'scylla'}")
        with pytest.raises(InvalidRequest, match="needs an 'order_by' option"):
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = "
                        "{'verify_candidates': 'scylla'}")
        cql.execute(f"CREATE CUSTOM INDEX ON {table}(title) USING 'pattern_index' WITH OPTIONS = "
                    "{'order_by': 'registered_at', 'case_sensitive': 'true', 'verify_candidates': 'scylla'}")
