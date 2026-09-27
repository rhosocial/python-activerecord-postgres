# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_materialized_view_introspection.py
"""PostgreSQL materialized view introspection tests (SQL generation only).

Covers the introspection half of the regression matrix documented in
``.claude/plan/2026-09-26/materialized-view-coverage.md``:

- G7: materialized views are discoverable (``pg_matviews``), report whether they
  are populated, and expose the UNIQUE index prerequisite for
  ``REFRESH ... CONCURRENTLY``.
- G8: sync/async parity for the new introspector methods.
"""

import pytest

from rhosocial.activerecord.backend.introspection.types import IntrospectionScope
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.introspection import (
    PostgresMaterializedViewInfoExpression,
    PostgresMaterializedViewListExpression,
)
from rhosocial.activerecord.backend.impl.postgres.introspection.introspector import (
    AsyncPostgreSQLIntrospector,
    PostgreSQLIntrospectorMixin,
    SyncPostgreSQLIntrospector,
)


def _dialect():
    return PostgresDialect(version=(15, 0, 0))


class TestMaterializedViewIntrospectionSQL:
    """The generated catalog queries must target relkind 'm' via pg_matviews."""

    def test_list_query_uses_pg_matviews(self):
        sql, params = PostgresMaterializedViewListExpression(
            _dialect(), schema="reporting"
        ).to_sql()
        assert "pg_matviews" in sql
        assert "ispopulated" in sql
        assert "indisunique" in sql
        assert params == ("reporting",)

    def test_list_query_reports_unique_index_and_populated_flag(self):
        sql, _ = PostgresMaterializedViewListExpression(_dialect()).to_sql()
        for column in (
            "is_populated",
            "has_unique_index",
            "definition",
            "comment",
        ):
            assert f"as {column}" in sql

    def test_list_query_excludes_system_objects_by_default(self):
        sql, _ = PostgresMaterializedViewListExpression(_dialect()).to_sql()
        # doubled percent: psycopg would treat a bare '%' as a placeholder start
        assert "NOT LIKE 'pg_%%'" in sql

    def test_list_query_can_include_system_objects(self):
        sql, _ = PostgresMaterializedViewListExpression(
            _dialect()
        ).include_system(True).to_sql()
        assert "NOT LIKE 'pg_%'" not in sql

    def test_info_query_filters_by_name(self):
        sql, params = PostgresMaterializedViewInfoExpression(
            _dialect(), "sales_summary", schema="reporting"
        ).to_sql()
        assert "pg_matviews" in sql
        assert "m.matviewname = " in sql
        assert params == ("reporting", "sales_summary")

    def test_info_query_defaults_to_connection_schema(self):
        _, params = PostgresMaterializedViewInfoExpression(_dialect(), "sales_summary").to_sql()
        assert params[0] == "public"


class TestMaterializedViewParsing:
    """Row parsing must expose PostgreSQL specific MV state."""

    @pytest.fixture
    def parser(self):
        return PostgreSQLIntrospectorMixin

    def _row(self, **overrides):
        row = {
            "schema_name": "reporting",
            "view_name": "sales_summary",
            "definition": " SELECT 1;",
            "is_populated": True,
            "has_unique_index": True,
            "comment": "monthly rollup",
        }
        row.update(overrides)
        return row

    def test_parse_exposes_materialized_state(self, parser):
        info = parser._parse_materialized_view(self._row(), "public")
        assert info.name == "sales_summary"
        assert info.schema == "reporting"
        assert info.definition == " SELECT 1;"
        assert info.comment == "monthly rollup"
        assert info.is_updatable is False
        assert info.is_insertable is False
        assert info.extra["is_materialized"] is True
        assert info.extra["is_populated"] is True
        assert info.extra["has_unique_index"] is True

    def test_parse_unpopulated_view(self, parser):
        info = parser._parse_materialized_view(
            self._row(is_populated=False, has_unique_index=False), "public"
        )
        assert info.extra["is_populated"] is False
        assert info.extra["has_unique_index"] is False

    def test_parse_falls_back_to_defaults(self, parser):
        info = parser._parse_materialized_view({}, "public", "fallback_mv")
        assert info.name == "fallback_mv"
        assert info.schema == "public"
        assert info.extra["is_populated"] is False
        assert info.extra["has_unique_index"] is False

    def test_parse_list(self, parser):
        infos = parser._parse_materialized_views(
            [self._row(), self._row(view_name="sales_daily", is_populated=False)],
            "public",
        )
        assert [info.name for info in infos] == ["sales_summary", "sales_daily"]
        assert [info.extra["is_populated"] for info in infos] == [True, False]

    def test_parse_info_returns_none_when_missing(self, parser):
        assert parser._parse_materialized_view_info([], "missing_mv", "public") is None


class TestMaterializedViewIntrospectionSurface:
    """Sync/async parity and cache-key isolation."""

    @pytest.mark.parametrize(
        "method", ["list_materialized_views", "get_materialized_view_info", "materialized_view_exists"]
    )
    def test_sync_and_async_expose_same_methods(self, method):
        assert hasattr(SyncPostgreSQLIntrospector, method)
        assert hasattr(AsyncPostgreSQLIntrospector, method)

    def test_cache_keys_are_namespaced(self):
        """MV lookups must not collide with regular view lookups."""
        introspector = SyncPostgreSQLIntrospector.__new__(SyncPostgreSQLIntrospector)
        list_key = introspector._materialized_view_cache_key("public", include_system=False)
        single_key = introspector._materialized_view_cache_key(
            "public", view_name="sales_summary"
        )
        system_key = introspector._materialized_view_cache_key("public", include_system=True)
        view_key = introspector._make_cache_key(IntrospectionScope.VIEW, schema="public")
        assert list_key != single_key
        assert list_key != system_key
        assert list_key != view_key
        assert "matviews" in list_key
