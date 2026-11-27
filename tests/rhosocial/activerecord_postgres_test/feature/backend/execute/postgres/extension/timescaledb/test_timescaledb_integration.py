# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/postgres/extension/
#   timescaledb/test_timescaledb_integration.py
"""Integration tests for TimescaleDB hypertable creation."""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    ColumnConstraint,
    ColumnDefinition,
    ColumnNotNullConstraint,
    CreateTableStatement,
    DropTableStatement,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.types import TimestampTzType, TextType
from rhosocial.activerecord.backend.impl.postgres.functions.timescaledb import create_hypertable
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DDL = ExecutionOptions(stmt_type=StatementType.DDL)


def _columns(dialect):
    return [
        ColumnDefinition(dialect, name="ts", data_type=TimestampTzType(dialect=dialect),
                         constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))]),
        ColumnDefinition(dialect, name="device", data_type=TextType(dialect=dialect)),
    ]


@pytest.fixture
def timescaledb_env(postgres_backend_single):
    backend = postgres_backend_single
    ensure_extension_installed(backend, "timescaledb")
    dialect = backend.dialect
    table = "test_ts_metrics"
    drop = DropTableStatement(dialect=dialect, table=Table(dialect, table), if_exists=True)
    backend.execute(*drop.to_sql(), options=DDL)
    backend.execute(
        *CreateTableStatement(dialect=dialect, table=Table(dialect, table), columns=_columns(dialect)).to_sql(),
        options=DDL,
    )
    yield backend, dialect, table
    backend.execute(*drop.to_sql(), options=DDL)


@pytest_asyncio.fixture
async def async_timescaledb_env(async_postgres_backend_single):
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "timescaledb")
    dialect = backend.dialect
    table = "test_ts_metrics_async"
    await backend.execute(
        *DropTableStatement(dialect=dialect, table=Table(dialect, table), if_exists=True).to_sql(),
        options=DDL,
    )
    await backend.execute(
        *CreateTableStatement(dialect=dialect, table=Table(dialect, table), columns=_columns(dialect)).to_sql(),
        options=DDL,
    )
    yield backend, dialect, table
    await backend.execute(
        *DropTableStatement(dialect=dialect, table=Table(dialect, table), if_exists=True).to_sql(),
        options=DDL,
    )


class TestTimescaleDBIntegration:
    def test_capability(self, timescaledb_env):
        _, dialect, _ = timescaledb_env
        assert dialect.supports_timescaledb()
        assert dialect.get_extension_version("timescaledb") is not None

    def test_create_hypertable(self, timescaledb_env):
        backend, dialect, table = timescaledb_env
        backend.execute(
            *QueryStatement(
                dialect=dialect, select=[create_hypertable(dialect, table, "ts")]
            ).to_sql(),
            options=DDL,
        )
        row = backend.fetch_one(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = %s",
            (table,),
        )
        assert row["hypertable_name"] == table


class TestAsyncTimescaleDBIntegration:
    @pytest.mark.asyncio
    async def test_async_create_hypertable(self, async_timescaledb_env):
        backend, dialect, table = async_timescaledb_env
        assert dialect.supports_timescaledb()
        await backend.execute(
            *QueryStatement(
                dialect=dialect, select=[create_hypertable(dialect, table, "ts")]
            ).to_sql(),
            options=DDL,
        )
        row = await backend.fetch_one(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = %s",
            (table,),
        )
        assert row["hypertable_name"] == table
