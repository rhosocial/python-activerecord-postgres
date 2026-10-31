# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_introspection_parsing.py
"""Offline regression tests for the introspection answer shapes.

Three defects used to survive because every test that touched them needed a
server: the column statement never selected the size attributes the catalog
already answers, the table listing collapsed kinds the catalog distinguishes,
and the CLI's ``extensions`` answer read the dialect's knowledge registry
instead of ``pg_extension``. Nothing here opens a connection -- a parser is
pure Python and a statement is a rendered tree -- so these are construction and
parsing assertions only.
"""

import asyncio
from types import SimpleNamespace

import pytest

from rhosocial.activerecord.backend.expression.introspection import (
    ColumnInfoQueryStatement,
    DatabaseInfoQueryStatement,
    TableListQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.cli.introspect import (
    _handle_introspect_async,
    _handle_introspect_sync,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.introspection.introspector import (
    AsyncPostgreSQLIntrospector,
    PostgreSQLIntrospectorMixin,
    SyncPostgreSQLIntrospector,
)
from rhosocial.activerecord.backend.introspection.status import ExtensionInfo
from rhosocial.activerecord.backend.introspection.types import TableType


def _dialect(version=(17, 0, 0)):
    return PostgresDialect(version=version)


class _FakeBackend:
    """The two things a parser reads: the dialect and the cached version."""

    def __init__(self, dialect, cached_version=None, database="app"):
        self.dialect = dialect
        self._cached_version = cached_version
        self.config = SimpleNamespace(database=database)

    def get_cached_server_version(self):
        return self._cached_version


class _ParsingIntrospector(PostgreSQLIntrospectorMixin):
    """An introspector with no executor: parsing never asks for one."""

    def __init__(self, backend):
        self._backend = backend


class TestColumnStatement:
    """The statement must ask for the attributes the parser maps."""

    def test_selects_the_four_shape_attributes_from_information_schema(self):
        sql, params = ColumnInfoQueryStatement(
            _dialect(), table_name="measurements", schema="public"
        ).to_sql()
        assert "LEFT JOIN information_schema.columns AS ic" in sql
        for column in (
            "character_maximum_length",
            "numeric_precision",
            "numeric_scale",
            "datetime_precision",
        ):
            assert f'ic.{column} AS "{column}"' in sql
        # The join travels on the view's own key, not the column name alone.
        assert "ic.table_schema = n.nspname" in sql
        assert "ic.table_name = c.relname" in sql
        assert "ic.column_name = a.attname" in sql
        assert params == ("public", "measurements")


class TestColumnParsing:
    """The parser must carry those attributes onto the column record."""

    @staticmethod
    def _row(**overrides):
        row = {
            "column_name": "code",
            "ordinal_position": 1,
            "data_type": "character varying(20)",
            "is_nullable": False,
            "default_value": None,
            "is_primary_key": False,
            "comment": None,
            "character_maximum_length": 20,
            "numeric_precision": None,
            "numeric_scale": None,
            "datetime_precision": None,
        }
        row.update(overrides)
        return row

    def _parse(self, rows):
        parser = _ParsingIntrospector(_FakeBackend(_dialect()))
        return parser.parse_columns(rows, "measurements", "public")

    def test_character_maximum_length_is_reported(self):
        column = self._parse([self._row()])[0]
        assert column.character_maximum_length == 20
        assert column.numeric_precision is None
        assert column.numeric_scale is None

    def test_numeric_precision_and_scale_are_reported(self):
        column = self._parse(
            [
                self._row(
                    column_name="amount",
                    data_type="numeric(10,3)",
                    character_maximum_length=None,
                    numeric_precision=10,
                    numeric_scale=3,
                )
            ]
        )[0]
        assert column.numeric_precision == 10
        assert column.numeric_scale == 3

    def test_datetime_precision_is_reported(self):
        column = self._parse(
            [
                self._row(
                    column_name="created_at",
                    data_type="timestamp(3) with time zone",
                    character_maximum_length=None,
                    datetime_precision=3,
                )
            ]
        )[0]
        assert column.datetime_precision == 3

    def test_absent_attributes_stay_none(self):
        # A materialized view is not listed by information_schema.columns, so
        # the LEFT JOIN yields no shape row rather than dropping the column.
        column = self._parse([self._row(character_maximum_length=None)])[0]
        assert column.character_maximum_length is None


class TestTableTypeMapping:
    """Every kind the catalog names gets its own member."""

    LABELS = (
        ("BASE TABLE", TableType.BASE_TABLE),
        ("VIEW", TableType.VIEW),
        ("MATERIALIZED VIEW", TableType.MATERIALIZED_VIEW),
        ("FOREIGN TABLE", TableType.FOREIGN_TABLE),
        ("PARTITIONED TABLE", TableType.PARTITIONED_TABLE),
    )

    def _parse(self, rows):
        parser = _ParsingIntrospector(_FakeBackend(_dialect()))
        return parser.parse_tables(rows, "public")

    def test_each_label_maps_to_its_own_member(self):
        rows = [
            {"table_name": label.lower().replace(" ", "_"), "table_type": label}
            for label, _ in self.LABELS
        ]
        parsed = self._parse(rows)
        assert [info.table_type for info in parsed] == [member for _, member in self.LABELS]

    def test_unknown_label_falls_back_to_base_table(self):
        parsed = self._parse([{"table_name": "odd", "table_type": "SOMETHING ELSE"}])
        assert parsed[0].table_type == TableType.BASE_TABLE

    @pytest.mark.parametrize("label,relkind", [
        ("BASE TABLE", "'r'"),
        ("VIEW", "'v'"),
        ("MATERIALIZED VIEW", "'m'"),
        ("FOREIGN TABLE", "'f'"),
        ("PARTITIONED TABLE", "'p'"),
    ])
    def test_table_type_filter_narrows_to_the_matching_relkind(self, label, relkind):
        sql, _ = TableListQueryStatement(
            _dialect(), schema="public", table_type=label
        ).to_sql()
        assert f"c.relkind = {relkind}" in sql


class TestDatabaseInfoStatement:
    """``table_count`` rides along in the one database-info row."""

    def _sql(self):
        sql, params = DatabaseInfoQueryStatement(_dialect()).to_sql()
        assert params == ()
        return sql

    def test_count_is_projected_as_a_scalar_subquery(self):
        sql = self._sql()
        assert " AS \"table_count\"" in sql
        assert "SELECT COUNT(*) FROM pg_class AS c JOIN pg_namespace AS n" in sql
        # One statement: the count is a projected scalar, not a second query.
        assert ";" not in sql

    def test_count_asks_for_ordinary_and_partitioned_tables_only(self):
        # Views, materialized views and foreign tables are not tables, so the
        # count names the two relkinds that are rather than subtracting the
        # others from a wider listing.
        assert "c.relkind IN ('r', 'p')" in self._sql()

    def test_count_excludes_the_server_schemas_and_the_temp_family(self):
        sql = self._sql()
        assert "NOT (n.nspname IN ('pg_catalog', 'information_schema', 'pg_toast'))" in sql
        # ``pg_temp%%`` is what reaches psycopg: a bare % would open a placeholder.
        assert "n.nspname NOT LIKE 'pg_temp%%'" in sql


class TestDatabaseInfoParsing:
    """The version is reported, never guessed."""

    def _parse(self, backend, rows):
        return _ParsingIntrospector(backend).parse_database_info(rows)

    def test_sync_and_async_read_the_same_row(self):
        # The statement is the dialect's and the parser is the mixin's, so
        # neither path can drift into reading a different shape.
        assert (
            SyncPostgreSQLIntrospector.parse_database_info
            is AsyncPostgreSQLIntrospector.parse_database_info
        )
        assert (
            SyncPostgreSQLIntrospector.parse_database_info
            is PostgreSQLIntrospectorMixin.parse_database_info
        )

    def test_unknown_version_is_the_literal_unknown(self):
        info = self._parse(_FakeBackend(PostgresDialect()), [])
        assert info.version == "unknown"
        assert info.version_tuple == (0, 0, 0)

    def test_adapted_version_is_the_dotted_server_version(self):
        info = self._parse(_FakeBackend(_dialect((17, 4, 0))), [])
        assert info.version == "17.4.0"
        assert info.version_tuple == (17, 4, 0)

    def test_cached_version_is_used_when_the_dialect_is_unadapted(self):
        info = self._parse(_FakeBackend(PostgresDialect(), cached_version=(16, 2, 0)), [])
        assert info.version == "16.2.0"
        assert info.version_tuple == (16, 2, 0)

    def test_statement_answers_are_reported(self):
        info = self._parse(
            _FakeBackend(_dialect()),
            [{"encoding": "UTF8", "collation": "C", "size": 4096, "table_count": 7}],
        )
        assert info.name == "app"
        assert info.encoding == "UTF8"
        assert info.collation == "C"
        assert info.size_bytes == 4096
        assert info.table_count == 7

    def test_count_is_read_as_an_int_whatever_the_driver_answers(self):
        info = self._parse(_FakeBackend(_dialect()), [{"table_count": "7"}])
        assert info.table_count == 7

    @pytest.mark.parametrize("cell", [None, "unreadable", float("nan")])
    def test_unreadable_count_is_unknown_rather_than_an_error(self, cell):
        info = self._parse(_FakeBackend(_dialect()), [{"table_count": cell}])
        assert info.table_count is None

    def test_missing_count_column_is_unknown(self):
        info = self._parse(_FakeBackend(_dialect()), [{"encoding": "UTF8"}])
        assert info.table_count is None
        assert info.encoding == "UTF8"


class TestCapabilityPredicates:
    """A capability check answers; it never raises for an unknown version."""

    def test_unadapted_dialect_answers_false(self):
        dialect = PostgresDialect()
        assert dialect.supports_materialized_view() is False
        assert dialect.supports_multirange_type() is False

    def test_adapted_dialect_answers_for_the_server(self):
        old = _dialect((9, 2, 0))
        new = _dialect((15, 0, 0))
        assert old.supports_materialized_view() is False
        assert new.supports_materialized_view() is True


class _RecordingProvider:
    def __init__(self):
        self.rows = None
        self.title = None

    def display_results(self, data, title=None):
        self.rows = data
        self.title = title


class _FakeIntrospector:
    def __init__(self, extensions):
        self._extensions = extensions

    def list_extensions(self):
        return self._extensions


class _FakeCliBackend:
    def __init__(self, introspector, dialect):
        self._introspector = introspector
        self.dialect = dialect
        self._connection = True

    @property
    def introspector(self):
        return self._introspector

    def connect(self):
        pass

    def introspect_and_adapt(self):
        """The real backend reads the server version here; this one has none.

        The handler adapts before it asks anything, so a stand-in that cannot
        answer the call would fail every type -- which is why the fake answers
        it, and why the dialect is constructed with a version already set.
        """

    def disconnect(self):
        self._connection = False


def _registry_of(size=30):
    return {
        f"known_{index}": SimpleNamespace(
            installed=True, available=True, version="1.0", schema="public"
        )
        for index in range(size)
    }


class TestExtensionsAnswer:
    """The answer is pg_extension's, not the dialect's knowledge registry."""

    INSTALLED = [
        ExtensionInfo(
            name="plpgsql",
            version="1.0",
            schema="pg_catalog",
            description="PL/pgSQL procedural language",
        ),
        ExtensionInfo(name="pg_trgm", version="1.6", schema="public"),
    ]

    def test_answer_does_not_grow_with_the_registry(self):
        dialect = _dialect()
        dialect._extensions = _registry_of()
        provider = _RecordingProvider()
        backend = _FakeCliBackend(_FakeIntrospector(self.INSTALLED), dialect)

        _handle_introspect_sync(
            SimpleNamespace(type="extensions", name=None, include_system=False),
            backend,
            provider,
        )

        assert [row["name"] for row in provider.rows] == ["plpgsql", "pg_trgm"]
        assert set(provider.rows[0]) == {"name", "version", "schema", "description"}
        assert provider.rows[0]["description"] == "PL/pgSQL procedural language"
        # The registry is still there and still larger; the answer ignored it.
        assert len(dialect._extensions) == 30

    def test_async_answer_matches_the_sync_one(self):
        dialect = _dialect()
        dialect._extensions = _registry_of()

        class _AsyncIntrospector(_FakeIntrospector):
            async def list_extensions(self):
                return self._extensions

        class _AsyncCliBackend(_FakeCliBackend):
            async def connect(self):
                pass

            async def introspect_and_adapt(self):
                pass

            async def disconnect(self):
                self._connection = False

        provider = _RecordingProvider()
        backend = _AsyncCliBackend(_AsyncIntrospector(self.INSTALLED), dialect)

        asyncio.run(
            _handle_introspect_async(
                SimpleNamespace(type="extensions", name=None, include_system=False),
                backend,
                provider,
            )
        )

        assert [row["name"] for row in provider.rows] == ["plpgsql", "pg_trgm"]
