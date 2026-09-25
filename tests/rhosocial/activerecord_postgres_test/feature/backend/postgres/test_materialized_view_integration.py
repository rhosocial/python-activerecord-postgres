# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_materialized_view_integration.py
"""PostgreSQL materialized view integration tests (require a live server).

Covers the server-behaviour half of the regression matrix documented in
``.claude/plan/2026-09-26/materialized-view-coverage.md``: the SQL-generation unit
tests prove the emitted text, these prove PostgreSQL accepts it and that the
object behaves as documented.
"""

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.postgres import PostgresStorageParameter
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresAlterMaterializedViewExpression,
    PostgresChangeMaterializedViewOwnerAction,
    PostgresCreateMaterializedViewExpression,
    PostgresDropMaterializedViewExpression,
    PostgresRefreshMaterializedViewExpression,
    PostgresRenameMaterializedViewAction,
    PostgresSetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewSchemaAction,
)

from conftest import DDL, DQL

pytestmark = pytest.mark.usefixtures("mv_backend")


def _query(backend, sql):
    return backend.execute(sql, options=DQL)


def _reloptions(backend, view_name, schema="public"):
    """Read pg_class.reloptions straight from the catalog."""
    result = backend.execute(
        "SELECT reloptions FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relname = %s AND n.nspname = %s",
        (view_name, schema),
        options=DQL,
    )
    rows = result.data or []
    return rows[0]["reloptions"] if rows else None


class TestCreateMaterializedView:
    def test_create_and_query(self, mv_backend, create_summary_mv):
        create_summary_mv()
        result = _query(mv_backend, "SELECT product_id FROM mv_sales_summary ORDER BY product_id")
        assert [row["product_id"] for row in result.data or []] == [1, 2]

    def test_storage_options_applied_on_server(self, mv_backend, create_summary_mv):
        """T-42: the server must accept and persist the storage parameters."""
        create_summary_mv(
            storage_options={
                PostgresStorageParameter.FILLFACTOR: 70,
                PostgresStorageParameter.AUTOVACUUM_ENABLED: "false",
            }
        )
        options = _reloptions(mv_backend, "mv_sales_summary")
        assert options is not None
        joined = " ".join(options)
        assert "fillfactor=70" in joined
        assert "autovacuum_enabled=false" in joined

    def test_if_not_exists_is_idempotent(self, mv_backend, create_summary_mv):
        """T-45."""
        create_summary_mv(if_not_exists=True)
        create_summary_mv(if_not_exists=True)
        result = _query(mv_backend, "SELECT count(*) FROM mv_sales_summary")
        assert result.data[0]["count"] == 2

    def test_with_no_data_is_unqueryable_until_refresh(self, mv_backend, daily_query):
        """T-44."""
        expression = PostgresCreateMaterializedViewExpression(
            dialect=mv_backend.dialect,
            view_name="mv_sales_daily",
            query=daily_query(),
            with_data=False,
        )
        sql, params = expression.to_sql()
        mv_backend.execute(sql, params, options=DDL)
        mv_backend.introspector.clear_cache()

        with pytest.raises(DatabaseError) as exc:
            _query(mv_backend, "SELECT count(*) FROM mv_sales_daily")
        assert "has not been populated" in str(exc.value)

        refresh = PostgresRefreshMaterializedViewExpression(
            dialect=mv_backend.dialect, name="mv_sales_daily", with_data=True
        )
        sql, params = refresh.to_sql()
        mv_backend.execute(sql, params)
        mv_backend.introspector.clear_cache()
        result = _query(mv_backend, "SELECT count(*) FROM mv_sales_daily")
        assert result.data[0]["count"] == 3


class TestRefreshMaterializedView:
    def test_refresh_picks_up_new_rows(self, mv_backend, create_summary_mv):
        create_summary_mv()
        mv_backend.execute(
            "INSERT INTO mv_sales (product_id, amount, sale_date) VALUES (3, 10.00, '2024-02-01')"
        )
        refresh = PostgresRefreshMaterializedViewExpression(
            dialect=mv_backend.dialect, name="mv_sales_summary"
        )
        sql, params = refresh.to_sql()
        mv_backend.execute(sql, params)
        result = _query(mv_backend, "SELECT count(*) FROM mv_sales_summary")
        assert result.data[0]["count"] == 3

    def test_concurrent_refresh_requires_unique_index(self, mv_backend, create_summary_mv):
        """T-43: without a UNIQUE index the server must reject CONCURRENTLY."""
        create_summary_mv()
        refresh = PostgresRefreshMaterializedViewExpression(
            dialect=mv_backend.dialect, name="mv_sales_summary", concurrently=True
        )
        sql, params = refresh.to_sql()
        with pytest.raises(DatabaseError) as exc:
            mv_backend.execute(sql, params)
        assert "unique" in str(exc.value).lower()

        mv_backend.execute(
            "CREATE UNIQUE INDEX mv_sales_summary_pk ON mv_sales_summary (product_id)",
            options=DDL,
        )
        mv_backend.execute(sql, params)
        result = _query(mv_backend, "SELECT count(*) FROM mv_sales_summary")
        assert result.data[0]["count"] == 2


class TestSchemaQualifiedMaterializedView:
    def test_lifecycle_in_non_public_schema(self, mv_backend, create_summary_mv):
        """T-46."""
        mv_backend.execute("CREATE SCHEMA IF NOT EXISTS mv_reporting", options=DDL)
        create_summary_mv(
            view_name="sales_summary",
            schema="mv_reporting",
            column_aliases=["product_id", "sale_count", "total_amount"],
        )
        result = _query(mv_backend, "SELECT product_id FROM mv_reporting.sales_summary ORDER BY product_id")
        assert [row["product_id"] for row in result.data or []] == [1, 2]

        alter = PostgresAlterMaterializedViewExpression(
            dialect=mv_backend.dialect,
            view_name="sales_summary",
            schema="mv_reporting",
            actions=[PostgresSetMaterializedViewSchemaAction(mv_backend.dialect, "public")],
        )
        sql, params = alter.to_sql()
        mv_backend.execute(sql, params, options=DDL)
        mv_backend.introspector.clear_cache()
        result = _query(mv_backend, "SELECT product_id FROM public.sales_summary")
        assert len(result.data or []) == 2


class TestAlterMaterializedView:
    def test_rename(self, mv_backend, create_summary_mv):
        """T-47."""
        create_summary_mv()
        alter = PostgresAlterMaterializedViewExpression(
            dialect=mv_backend.dialect,
            view_name="mv_sales_summary",
            actions=[PostgresRenameMaterializedViewAction(mv_backend.dialect, "mv_sales_renamed")],
        )
        sql, params = alter.to_sql()
        mv_backend.execute(sql, params, options=DDL)
        mv_backend.introspector.clear_cache()
        assert mv_backend.introspector.materialized_view_exists("mv_sales_renamed")
        assert not mv_backend.introspector.materialized_view_exists("mv_sales_summary")

    def test_set_storage_properties(self, mv_backend, create_summary_mv):
        create_summary_mv()
        alter = PostgresAlterMaterializedViewExpression(
            dialect=mv_backend.dialect,
            view_name="mv_sales_summary",
            actions=[
                PostgresSetMaterializedViewPropertiesAction(
                    mv_backend.dialect, {PostgresStorageParameter.FILLFACTOR: 85}
                )
            ],
        )
        sql, params = alter.to_sql()
        mv_backend.execute(sql, params, options=DDL)
        assert "fillfactor=85" in " ".join(_reloptions(mv_backend, "mv_sales_summary") or [])

    def test_owner_to_is_accepted(self, mv_backend, create_summary_mv):
        create_summary_mv()
        alter = PostgresAlterMaterializedViewExpression(
            dialect=mv_backend.dialect,
            view_name="mv_sales_summary",
            actions=[PostgresChangeMaterializedViewOwnerAction(mv_backend.dialect, "CURRENT_USER")],
        )
        sql, params = alter.to_sql()
        mv_backend.execute(sql, params, options=DDL)


class TestMaterializedViewIntrospection:
    def test_list_reports_populated_flag(self, mv_backend, create_summary_mv, daily_query):
        """T-48."""
        create_summary_mv()
        empty = PostgresCreateMaterializedViewExpression(
            dialect=mv_backend.dialect,
            view_name="mv_sales_daily",
            query=daily_query(),
            with_data=False,
        )
        sql, params = empty.to_sql()
        mv_backend.execute(sql, params, options=DDL)
        mv_backend.introspector.clear_cache()

        views = {view.name: view for view in mv_backend.introspector.list_materialized_views()}
        assert "mv_sales_summary" in views
        assert "mv_sales_daily" in views
        assert views["mv_sales_summary"].extra["is_populated"] is True
        assert views["mv_sales_daily"].extra["is_populated"] is False
        assert all(view.extra["is_materialized"] for view in views.values())
        assert all(view.is_updatable is False for view in views.values())

    def test_list_reports_unique_index(self, mv_backend, create_summary_mv):
        """T-49."""
        create_summary_mv()
        mv_backend.introspector.clear_cache()
        views = {view.name: view for view in mv_backend.introspector.list_materialized_views()}
        assert views["mv_sales_summary"].extra["has_unique_index"] is False

        mv_backend.execute(
            "CREATE UNIQUE INDEX mv_sales_summary_uidx ON mv_sales_summary (product_id)",
            options=DDL,
        )
        mv_backend.introspector.clear_cache()
        views = {view.name: view for view in mv_backend.introspector.list_materialized_views()}
        assert views["mv_sales_summary"].extra["has_unique_index"] is True

    def test_get_info_and_exists(self, mv_backend, create_summary_mv):
        create_summary_mv()
        mv_backend.introspector.clear_cache()
        info = mv_backend.introspector.get_materialized_view_info("mv_sales_summary")
        assert info is not None
        assert info.schema == "public"
        assert info.definition
        assert mv_backend.introspector.materialized_view_exists("mv_sales_summary")
        assert not mv_backend.introspector.materialized_view_exists("mv_missing")

    def test_regular_views_are_not_listed(self, mv_backend, create_summary_mv):
        """MV enumeration must not leak into the regular view listing."""
        create_summary_mv()
        mv_backend.execute("CREATE OR REPLACE VIEW mv_plain_view AS SELECT 1 AS one", options=DDL)
        mv_backend.introspector.clear_cache()
        view_names = {view.name for view in mv_backend.introspector.list_views()}
        assert "mv_plain_view" in view_names
        assert "mv_sales_summary" not in view_names

    def test_table_list_filters_materialized_view(self, mv_backend, create_summary_mv):
        """T-50: relkind 'm' rows are reported and filterable."""
        create_summary_mv()
        tables = mv_backend.introspector.list_tables(table_type="MATERIALIZED VIEW")
        assert "mv_sales_summary" in {table.name for table in tables}

    def test_drop_cascade(self, mv_backend, create_summary_mv):
        create_summary_mv()
        drop = PostgresDropMaterializedViewExpression(
            dialect=mv_backend.dialect, view_name="mv_sales_summary", if_exists=True, cascade=True
        )
        sql, params = drop.to_sql()
        mv_backend.execute(sql, params, options=DDL)
        mv_backend.introspector.clear_cache()
        assert not mv_backend.introspector.materialized_view_exists("mv_sales_summary")


class TestMaterializedViewAsyncParity:
    """T-52 / T-53: the async backend must behave identically."""

    async def test_create_refresh_drop(self, async_mv_backend, summary_query):
        dialect = async_mv_backend.dialect
        create = PostgresCreateMaterializedViewExpression(
            dialect=dialect,
            view_name="mv_sales_summary",
            query=summary_query(),
            column_aliases=["product_id", "sale_count", "total_amount"],
        )
        sql, params = create.to_sql()
        await async_mv_backend.execute(sql, params, options=DDL)

        result = await async_mv_backend.execute(
            "SELECT product_id FROM mv_sales_summary ORDER BY product_id", options=DQL
        )
        assert [row["product_id"] for row in result.data or []] == [1, 2]

        await async_mv_backend.execute(
            "INSERT INTO mv_sales (product_id, amount, sale_date) VALUES (3, 10.00, '2024-02-01')"
        )
        refresh = PostgresRefreshMaterializedViewExpression(
            dialect=dialect, name="mv_sales_summary"
        )
        sql, params = refresh.to_sql()
        await async_mv_backend.execute(sql, params)

        result = await async_mv_backend.execute(
            "SELECT count(*) FROM mv_sales_summary", options=DQL
        )
        assert result.data[0]["count"] == 3

        drop = PostgresDropMaterializedViewExpression(
            dialect=dialect, view_name="mv_sales_summary", if_exists=True, cascade=True
        )
        sql, params = drop.to_sql()
        await async_mv_backend.execute(sql, params, options=DDL)

    async def test_list_materialized_views(self, async_mv_backend, summary_query):
        dialect = async_mv_backend.dialect
        create = PostgresCreateMaterializedViewExpression(
            dialect=dialect,
            view_name="mv_sales_summary",
            query=summary_query(),
        )
        sql, params = create.to_sql()
        await async_mv_backend.execute(sql, params, options=DDL)
        async_mv_backend.introspector.clear_cache()

        views = await async_mv_backend.introspector.list_materialized_views()
        names = {view.name for view in views}
        assert "mv_sales_summary" in names
        summary = next(view for view in views if view.name == "mv_sales_summary")
        assert summary.extra["is_populated"] is True
        assert await async_mv_backend.introspector.materialized_view_exists("mv_sales_summary")

    async def test_teardown_leaves_no_residue(self, async_mv_backend, daily_query):
        """T-51: fixture teardown must remove every object it created."""
        dialect = async_mv_backend.dialect
        create = PostgresCreateMaterializedViewExpression(
            dialect=dialect, view_name="mv_sales_daily", query=daily_query(), with_data=False
        )
        sql, params = create.to_sql()
        await async_mv_backend.execute(sql, params, options=DDL)
        assert await async_mv_backend.introspector.materialized_view_exists("mv_sales_daily")
