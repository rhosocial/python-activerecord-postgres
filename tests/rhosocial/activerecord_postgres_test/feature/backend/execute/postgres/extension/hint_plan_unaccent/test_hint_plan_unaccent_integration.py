# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/postgres/extension/
#   hint_plan_unaccent/test_hint_plan_unaccent_integration.py
"""Integration tests for pg_hint_plan and unaccent (sync and async)."""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    Column,
    ColumnConstraint,
    ColumnDefinition,
    ColumnPrimaryKeyConstraint,
    CreateTableStatement,
    InsertStatement,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.core import Parameter
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.types import TextType
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresExplainStatement,
    PostgresQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import PostgresSerialType
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import (
    hint_index_scan,
    hint_seq_scan,
    load_pg_hint_plan,
)
from rhosocial.activerecord.backend.impl.postgres.functions.unaccent import unaccent
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DDL = ExecutionOptions(stmt_type=StatementType.DDL)
DQL = ExecutionOptions(stmt_type=StatementType.DQL)


def _create_table(backend, dialect, table):
    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        ),
        ColumnDefinition(dialect, name="name", data_type=TextType(dialect=dialect)),
    ]
    backend.execute(
        *CreateTableStatement(
            dialect=dialect, table=Table(dialect, table), columns=columns
        ).to_sql(),
        options=DDL,
    )
    rows = [[Parameter(dialect, name)] for name in ("Café", "Résumé", "naïve", "plain")]
    backend.execute(
        *InsertStatement(
            dialect=dialect,
            into=Table(dialect, table),
            columns=["name"],
            source=ValuesSource(dialect, rows),
        ).to_sql()
    )


@pytest.fixture
def hint_env(postgres_backend_single):
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_hint_plan", "unaccent")
    dialect = backend.dialect
    table = "test_hint_plan_unaccent"
    backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)
    _create_table(backend, dialect, table)
    backend.execute(f"CREATE INDEX idx_{table}_name ON {table} (name)", (), options=DDL)
    backend.execute(f"ANALYZE {table}", (), options=DDL)
    yield backend, dialect, table
    backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)


@pytest_asyncio.fixture
async def async_hint_env(async_postgres_backend_single):
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_hint_plan", "unaccent")
    dialect = backend.dialect
    table = "test_hint_plan_unaccent_async"
    await backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)
    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        ),
        ColumnDefinition(dialect, name="name", data_type=TextType(dialect=dialect)),
    ]
    await backend.execute(
        *CreateTableStatement(
            dialect=dialect, table=Table(dialect, table), columns=columns
        ).to_sql(),
        options=DDL,
    )
    await backend.execute(
        *InsertStatement(
            dialect=dialect,
            into=Table(dialect, table),
            columns=["name"],
            source=ValuesSource(
                dialect, [[Parameter(dialect, name)] for name in ("Café", "Résumé", "naïve", "plain")]
            ),
        ).to_sql()
    )
    yield backend, dialect, table
    await backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)


class TestHintPlanIntegration:
    def test_capability_detection(self, hint_env):
        _, dialect, _ = hint_env
        assert dialect.supports_pg_hint_plan()
        # The extension version tracks the PG major (1.3.10 on PG12 through
        # 1.8.1 on PG18), so only assert that a version was detected.
        assert dialect.get_extension_version("pg_hint_plan") is not None

    def test_hinted_query_runs_and_binds(self, hint_env):
        backend, dialect, table = hint_env
        backend.execute(*load_pg_hint_plan(dialect).to_sql(), options=DDL)
        query = PostgresQueryStatement(
            dialect,
            select=[Column(dialect, "id")],
            from_=Table(dialect, table),
            hints=(hint_seq_scan(dialect, Table(dialect, table)),),
        )
        sql, params = query.to_sql()
        assert len(backend.execute(sql, params, options=DQL).data) == 4

    def test_hint_changes_plan(self, hint_env):
        backend, dialect, table = hint_env
        # CREATE EXTENSION alone does not install the planner hook on images
        # without shared_preload_libraries; LOAD enables it for this session.
        backend.execute(*load_pg_hint_plan(dialect).to_sql(), options=DDL)
        query = QueryStatement(
            dialect=dialect,
            select=[Column(dialect, "id")],
            from_=Table(dialect, table),
            where=ComparisonPredicate(
                dialect, "=", Column(dialect, "name"), Parameter(dialect, "Cafe")
            ),
        )
        # Make the index the planner's default on this four-row table so the
        # SeqScan hint has an observable effect; restore afterwards.
        backend.execute("SET enable_seqscan = off", (), options=DDL)
        try:
            base, params = query.to_sql()
            indexed = backend.fetch_one("EXPLAIN " + base, params)["QUERY PLAN"]
            assert "Index Scan" in indexed

            seq_sql, seq_params = PostgresExplainStatement(
                dialect, query, hints=(hint_seq_scan(dialect, Table(dialect, table)),)
            ).to_sql()
            forced = backend.fetch_one(seq_sql, seq_params)["QUERY PLAN"]
            assert "Seq Scan" in forced

            index_sql, index_params = PostgresExplainStatement(
                dialect,
                query,
                hints=(hint_index_scan(dialect, Table(dialect, table), f"idx_{table}_name"),),
            ).to_sql()
            forced_index = backend.fetch_one(index_sql, index_params)["QUERY PLAN"]
            assert "Index Scan" in forced_index
        finally:
            backend.execute("SET enable_seqscan = on", (), options=DDL)


class TestUnaccentIntegration:
    def test_capability_and_function(self, hint_env):
        backend, dialect, _ = hint_env
        assert dialect.supports_unaccent()
        sql, params = unaccent(dialect, "Café").to_sql()
        assert backend.fetch_one("SELECT " + sql + " AS value", params)["value"] == "Cafe"

    def test_column_normalization(self, hint_env):
        backend, dialect, table = hint_env
        query = QueryStatement(
            dialect=dialect,
            select=[unaccent(dialect, Column(dialect, "name")).as_("plain")],
            from_=Table(dialect, table),
        )
        rows = backend.execute(*query.to_sql(), options=DQL).data
        assert "Cafe" in [row["plain"] for row in rows]


class TestAsyncIntegration:
    @pytest.mark.asyncio
    async def test_async_capabilities_and_queries(self, async_hint_env):
        backend, dialect, table = async_hint_env
        assert dialect.supports_pg_hint_plan()
        assert dialect.supports_unaccent()
        await backend.execute(*load_pg_hint_plan(dialect).to_sql(), options=DDL)
        query = PostgresQueryStatement(
            dialect,
            select=[Column(dialect, "id")],
            from_=Table(dialect, table),
            hints=(hint_seq_scan(dialect, Table(dialect, table)),),
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=DQL)
        assert len(result.data) == 4
        sql, params = unaccent(dialect, "Résumé").to_sql()
        row = await backend.fetch_one("SELECT " + sql + " AS value", params)
        assert row["value"] == "Resume"
