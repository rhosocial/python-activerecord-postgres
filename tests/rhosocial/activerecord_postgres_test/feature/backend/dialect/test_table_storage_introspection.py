# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_table_storage_introspection.py
"""The per-table storage trio (SQL generation and parsing only).

A table's footprint is three numbers PostgreSQL keeps apart: the total, the
heap and the indexes. Core widened ``IntrospectionTableInfo`` with the two
narrow slots for exactly that, and this engine answers it -- so the listing
projects the trio, a caller can ask for one relation's split through a node,
and the parser fills the slots.

Nothing here opens a connection: a statement is a rendered tree and a parser is
pure Python, so every case is a construction, render or parse assertion. The
live legs (a real server's numbers) belong to the introspection feature tests.
"""

from types import SimpleNamespace

from rhosocial.activerecord.backend.expression.introspection import TableListQueryStatement
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.introspection import (
    PostgresIndexesSizeFunction,
    PostgresRelationSizeFunction,
    PostgresTableStorageQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.introspection.introspector import (
    PostgreSQLIntrospectorMixin,
)


def _dialect(version=(17, 0, 0)):
    return PostgresDialect(version=version)


class _FakeBackend:
    """The two things a parser reads: the dialect and the connection config."""

    def __init__(self, dialect, database="app"):
        self.dialect = dialect
        self.config = SimpleNamespace(database=database)


class _ParsingIntrospector(PostgreSQLIntrospectorMixin):
    """An introspector with no executor: parsing never asks for one."""

    def __init__(self, backend):
        self._backend = backend


class TestSizeFunctionNodes:
    """The two calls name their function and bind their relation."""

    def test_relation_size_renders_the_call(self):
        sql, params = PostgresRelationSizeFunction(_dialect(), relid=16384).to_sql()
        assert sql == "PG_RELATION_SIZE(%s)"
        assert params == (16384,)

    def test_indexes_size_renders_the_call(self):
        sql, params = PostgresIndexesSizeFunction(_dialect(), relid=16384).to_sql()
        assert sql == "PG_INDEXES_SIZE(%s)"
        assert params == (16384,)

    def test_the_oid_travels_as_a_parameter_not_as_text(self):
        # The relation is named by data: a bound OID cannot become SQL text,
        # whatever number the caller holds, and the statement carries no
        # literal at all in its place.
        sql, params = PostgresRelationSizeFunction(_dialect(), relid=99_999_999).to_sql()
        assert sql == "PG_RELATION_SIZE(%s)"
        assert params == (99_999_999,)


class TestTableStorageQueryNode:
    """The one-row statement reports the split under the slot names."""

    def test_both_slots_are_asked_for_in_one_statement(self):
        sql, params = PostgresTableStorageQueryStatement(_dialect(), relid=16384).to_sql()
        assert sql == (
            'SELECT PG_RELATION_SIZE(%s) AS "data_size_bytes", '
            'PG_INDEXES_SIZE(%s) AS "index_size_bytes"'
        )
        # The OID binds once per call: the same relation, two measurements.
        assert params == (16384, 16384)

    def test_one_statement_rather_than_two(self):
        sql, _ = PostgresTableStorageQueryStatement(_dialect(), relid=16384).to_sql()
        assert sql.count("SELECT") == 1
        assert ";" not in sql


class TestListingProjection:
    """The table listing asks the trio and reads the statistics view beside it."""

    def _sql(self):
        sql, params = TableListQueryStatement(_dialect(), schema="public").to_sql()
        assert params == ("public",)
        return sql

    def test_storage_trio_is_projected_from_the_relations_own_oid(self):
        sql = self._sql()
        assert 'PG_TOTAL_RELATION_SIZE(c.oid) AS "size_bytes"' in sql
        assert 'PG_RELATION_SIZE(c.oid) AS "data_size_bytes"' in sql
        assert 'PG_INDEXES_SIZE(c.oid) AS "index_size_bytes"' in sql

    def test_the_live_row_estimate_rides_a_left_join(self):
        sql = self._sql()
        assert "LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid" in sql
        assert 's.n_live_tup::BIGINT AS "live_row_count"' in sql
        # The planner's estimate stays: the two are read side by side.
        assert 'c.reltuples::BIGINT AS "row_count"' in sql

    def test_the_statistics_join_does_not_reach_the_view_listing(self):
        # A view listing is a different question from a table listing, and it
        # has no use for a row estimate or a storage split: the join belongs to
        # the relation questions only.
        from rhosocial.activerecord.backend.expression.introspection import ViewListQueryStatement

        sql, _ = ViewListQueryStatement(_dialect(), schema="public").to_sql()
        assert "pg_stat_user_tables" not in sql
        assert "n_live_tup" not in sql


class TestCapabilityProbe:
    """The family is declared, because this engine can answer it."""

    def test_the_probe_answers_true(self):
        assert _dialect().supports_table_storage_introspection() is True

    def test_the_probe_is_the_core_contract(self):
        # A probe core does not declare cannot be asked about one; this one
        # answers for the engine rather than staying on core's default.
        from rhosocial.activerecord.backend.dialect.mixins import IntrospectionMixin

        assert (
            IntrospectionMixin.supports_table_storage_introspection(IntrospectionMixin()) is False
        )


class TestRowCountContract:
    """Which of the two estimates answers ``row_count`` is a named contract."""

    @staticmethod
    def _estimate(**overrides):
        row = {"row_count": 42, "live_row_count": 40}
        row.update(overrides)
        return row

    def _choose(self, row):
        return PostgreSQLIntrospectorMixin.parse_row_count(row)

    def test_the_planners_estimate_is_the_contract_that_answers(self):
        assert self._choose(self._estimate(row_count=42, live_row_count=40)) == 42

    def test_zero_is_an_estimate_not_an_absence(self):
        assert self._choose(self._estimate(row_count=0, live_row_count=0)) == 0

    def test_the_live_estimate_answers_when_the_planner_has_none(self):
        assert self._choose(self._estimate(row_count=-1, live_row_count=17)) == 17

    def test_neither_estimate_is_unknown(self):
        assert self._choose(self._estimate(row_count=-1, live_row_count=None)) is None
        assert self._choose(self._estimate(row_count=None, live_row_count=None)) is None


class TestStorageParsing:
    """The parser fills the slots and reads the contract's answer."""

    @staticmethod
    def _row(**overrides):
        row = {
            "table_name": "orders",
            "table_type": "BASE TABLE",
            "comment": None,
            "row_count": 42,
            "live_row_count": 40,
            "size_bytes": 8192,
            "data_size_bytes": 6144,
            "index_size_bytes": 2048,
        }
        row.update(overrides)
        return row

    def _parse(self, rows):
        return _ParsingIntrospector(_FakeBackend(_dialect())).parse_tables(rows, "public")

    def test_the_split_is_reported_beside_the_total(self):
        table = self._parse([self._row()])[0]
        assert table.size_bytes == 8192
        assert table.data_size_bytes == 6144
        assert table.index_size_bytes == 2048

    def test_absent_split_stays_none(self):
        # A relation whose size calls answer nothing reports unknown rather
        # than zero: zero is a size, and this engine did not measure it.
        table = self._parse(
            [self._row(size_bytes=None, data_size_bytes=None, index_size_bytes=None)]
        )[0]
        assert table.size_bytes is None
        assert table.data_size_bytes is None
        assert table.index_size_bytes is None

    def test_the_planners_estimate_wins_when_it_has_one(self):
        table = self._parse([self._row(row_count=42, live_row_count=40)])[0]
        assert table.row_count == 42

    def test_a_measured_zero_is_reported_rather_than_unknown(self):
        # An analyzed empty table reports 0 from either source; that is a
        # measured emptiness, not a missing column.
        table = self._parse([self._row(row_count=0, live_row_count=0)])[0]
        assert table.row_count == 0

    def test_the_live_estimate_answers_when_the_planner_has_none(self):
        # PostgreSQL 14+ reports -1 for a never-analyzed relation, which means
        # "no estimate" and not "no rows": the statistics view's number then
        # says more than nothing does.
        table = self._parse([self._row(row_count=-1, live_row_count=17)])[0]
        assert table.row_count == 17

    def test_no_estimate_from_either_source_is_unknown(self):
        table = self._parse([self._row(row_count=-1, live_row_count=None)])[0]
        assert table.row_count is None

    def test_absent_statistics_column_is_unknown(self):
        # A relation the stats view does not list still reports its other
        # slots; only the row count goes unknown.
        row = self._row()
        del row["live_row_count"]
        table = self._parse([row])[0]
        assert table.row_count == 42
        assert table.data_size_bytes == 6144
