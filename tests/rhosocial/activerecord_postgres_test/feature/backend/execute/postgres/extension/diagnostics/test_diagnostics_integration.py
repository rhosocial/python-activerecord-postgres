# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/postgres/extension/
#   diagnostics/test_diagnostics_integration.py
"""Integration tests for amcheck, pageinspect, pgstattuple and pg_logicalinspect."""

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
    DropTableStatement,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.impl.postgres.expression.types import PostgresSerialType
from rhosocial.activerecord.backend.impl.postgres.functions.amcheck import (
    bt_index_check,
    verify_heapam,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pageinspect import get_raw_page, page_header
from rhosocial.activerecord.backend.impl.postgres.functions.pgstattuple import pgstattuple
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DDL = ExecutionOptions(stmt_type=StatementType.DDL)


def _create_table(backend, dialect, table):
    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        )
    ]
    backend.execute(
        *CreateTableStatement(dialect=dialect, table=Table(dialect, table), columns=columns).to_sql(),
        options=DDL,
    )
    backend.execute(f"INSERT INTO {table} DEFAULT VALUES", (), options=DDL)


@pytest.fixture
def diagnostics_env(postgres_backend_single):
    backend = postgres_backend_single
    ensure_extension_installed(backend, "amcheck", "pageinspect", "pgstattuple")
    dialect = backend.dialect
    table = "test_diagnostics_ext"
    backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)
    _create_table(backend, dialect, table)
    yield backend, dialect, table
    backend.execute(*DropTableStatement(dialect=dialect, table=Table(dialect, table)).to_sql(), options=DDL)


@pytest_asyncio.fixture
async def async_diagnostics_env(async_postgres_backend_single):
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "amcheck", "pageinspect", "pgstattuple")
    dialect = backend.dialect
    table = "test_diagnostics_ext_async"
    await backend.execute(f"DROP TABLE IF EXISTS {table}", (), options=DDL)
    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        )
    ]
    await backend.execute(
        *CreateTableStatement(dialect=dialect, table=Table(dialect, table), columns=columns).to_sql(),
        options=DDL,
    )
    await backend.execute(f"INSERT INTO {table} DEFAULT VALUES", (), options=DDL)
    yield backend, dialect, table
    await backend.execute(
        *DropTableStatement(dialect=dialect, table=Table(dialect, table)).to_sql(), options=DDL
    )


class TestDiagnosticsIntegration:
    def test_capabilities(self, diagnostics_env):
        _, dialect, _ = diagnostics_env
        assert dialect.supports_amcheck()
        assert dialect.supports_pageinspect()
        assert dialect.supports_pgstattuple()

    def test_amcheck_functions_execute(self, diagnostics_env):
        backend, dialect, table = diagnostics_env
        # The one-argument form exists in every amcheck version; the two
        # argument form and verify_heapam changed signature across majors.
        calls = [bt_index_check(dialect, f"{table}_pkey")]
        if dialect.version_at_least((18, 0, 0)):
            calls.append(verify_heapam(dialect, table))
        for call in calls:
            sql, params = call.to_sql()
            backend.execute(f"SELECT {sql} AS result", params)

    def test_pageinspect_functions_execute(self, diagnostics_env):
        backend, dialect, table = diagnostics_env
        sql, params = get_raw_page(dialect, table, "main", 0).to_sql()
        page = backend.fetch_one(f"SELECT {sql} AS page", params)["page"]
        assert page is not None
        sql, params = page_header(dialect, get_raw_page(dialect, table, "main", 0)).to_sql()
        header = backend.fetch_one(f"SELECT ({sql}).lsn AS lsn", params)
        assert header["lsn"] is not None

    def test_pgstattuple_function_executes(self, diagnostics_env):
        backend, dialect, table = diagnostics_env
        sql, params = pgstattuple(dialect, table).to_sql()
        row = backend.fetch_one(f"SELECT ({sql}).tuple_count AS count", params)
        assert row["count"] >= 1

    def test_pg_logicalinspect_capability(self, diagnostics_env):
        backend, dialect, _ = diagnostics_env
        if not dialect.is_extension_available("pg_logicalinspect"):
            pytest.skip("pg_logicalinspect is available on PG18+ only")
        ensure_extension_installed(backend, "pg_logicalinspect")
        assert dialect.supports_pg_logicalinspect()


class TestAsyncDiagnosticsIntegration:
    @pytest.mark.asyncio
    async def test_async_capabilities_and_query(self, async_diagnostics_env):
        backend, dialect, table = async_diagnostics_env
        assert dialect.supports_amcheck()
        result = await backend.fetch_one(f"SELECT count(*) AS count FROM {table}")
        assert result["count"] == 1
        await backend.execute(
            *QueryStatement(
                dialect=dialect,
                select=[Column(dialect, "id")],
                from_=Table(dialect, table),
            ).to_sql()
        )
