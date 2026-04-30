# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_walinspect_integration.py

"""Integration tests for the pg_walinspect extension.

These tests require a PostgreSQL database with the pg_walinspect extension installed.
Tests will be automatically skipped if the extension is not available.
pg_walinspect requires superuser privileges for inspecting WAL records.
Tests wrap operations in try/except and skip on permission errors.
All database operations use expression objects, not raw SQL strings.

The actual PostgreSQL function signatures are:
- pg_get_wal_records_info(start_lsn pg_lsn, end_lsn pg_lsn) — both params required
- pg_get_wal_blocks_info(start_lsn pg_lsn, end_lsn pg_lsn) — both params required

Note: pg_logical_emit_message is a built-in PostgreSQL function, not part
of pg_walinspect. It has been removed from the pg_walinspect module.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
)
from rhosocial.activerecord.backend.expression.core import FunctionCall
from rhosocial.activerecord.backend.impl.postgres.functions.pg_walinspect import (
    pg_get_wal_records_info,
    pg_get_wal_blocks_info,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


def _get_current_wal_lsn(backend, dialect):
    """Get the current WAL insert LSN as a string.

    Uses pg_current_wal_lsn() built-in function to get the current
    write-ahead log insert location.
    """
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    lsn_func = FunctionCall(dialect, "pg_current_wal_lsn").as_("current_lsn")
    query = QueryExpression(
        dialect=dialect,
        select=[lsn_func],
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=opts)
    return result.data[0]["current_lsn"]


async def _async_get_current_wal_lsn(backend, dialect):
    """Async version of _get_current_wal_lsn.

    Get the current WAL insert LSN as a string using async backend.
    """
    opts = ExecutionOptions(stmt_type=StatementType.DQL)
    lsn_func = FunctionCall(dialect, "pg_current_wal_lsn").as_("current_lsn")
    query = QueryExpression(
        dialect=dialect,
        select=[lsn_func],
    )
    sql, params = query.to_sql()
    result = await backend.execute(sql, params, options=opts)
    return result.data[0]["current_lsn"]


@pytest.fixture
def pg_walinspect_env(postgres_backend_single):
    """Independent test environment for pg_walinspect extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_walinspect")
    dialect = backend.dialect

    yield backend, dialect


class TestPgWalinspectIntegration:
    """Integration tests for pg_walinspect WAL inspection functions."""

    def test_extension_installed(self, pg_walinspect_env):
        """Test that pg_walinspect extension is installed."""
        backend, dialect = pg_walinspect_env
        # If we got here, ensure_extension_installed succeeded
        assert dialect.is_extension_installed("pg_walinspect")

    def test_sql_generation(self, pg_walinspect_env):
        """Test that function factories generate correct SQL expressions.

        This test verifies SQL generation without requiring superuser
        privileges for execution.
        """
        backend, dialect = pg_walinspect_env

        # Verify pg_get_wal_records_info generates correct SQL
        func1 = pg_get_wal_records_info(dialect, "0/16E9130", "0/16E9200")
        sql1, _ = func1.to_sql()
        assert "pg_get_wal_records_info" in sql1.lower()

        # Verify pg_get_wal_blocks_info generates correct SQL
        func2 = pg_get_wal_blocks_info(dialect, "0/16E9130", "0/16E9200")
        sql2, _ = func2.to_sql()
        assert "pg_get_wal_blocks_info" in sql2.lower()

    def test_pg_get_wal_records_info_with_lsn(self, pg_walinspect_env):
        """Test getting WAL records info with explicit LSN range.

        pg_get_wal_records_info requires both start_lsn and end_lsn parameters.
        We obtain valid LSN values from pg_current_wal_lsn() first.
        """
        backend, dialect = pg_walinspect_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Get current WAL position to use as LSN values
            current_lsn = _get_current_wal_lsn(backend, dialect)

            # Query WAL records info with the obtained LSN
            wal_func = pg_get_wal_records_info(
                dialect,
                current_lsn,
                current_lsn,
            )
            query = QueryExpression(
                dialect=dialect,
                select=[wal_func.as_("wal_records")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if ("permission denied" in error_msg
                or "must be superuser" in error_msg
                or "could not find a valid record" in error_msg):
                pytest.skip(
                    "pg_walinspect requires superuser privileges or valid WAL range"
                )
            raise


@pytest_asyncio.fixture
async def async_pg_walinspect_env(async_postgres_backend_single):
    """Independent async test environment for pg_walinspect extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_walinspect")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPgWalinspectIntegration:
    """Async integration tests for pg_walinspect WAL inspection functions."""

    @pytest.mark.asyncio
    async def test_async_extension_installed(self, async_pg_walinspect_env):
        """Test that pg_walinspect extension is installed asynchronously."""
        backend, dialect = async_pg_walinspect_env
        # If we got here, async_ensure_extension_installed succeeded
        assert dialect.is_extension_installed("pg_walinspect")

    @pytest.mark.asyncio
    async def test_async_sql_generation(self, async_pg_walinspect_env):
        """Test that function factories generate correct SQL expressions asynchronously.

        This test verifies SQL generation without requiring superuser
        privileges for execution.
        """
        backend, dialect = async_pg_walinspect_env

        # Verify pg_get_wal_records_info generates correct SQL
        func1 = pg_get_wal_records_info(dialect, "0/16E9130", "0/16E9200")
        sql1, _ = func1.to_sql()
        assert "pg_get_wal_records_info" in sql1.lower()

        # Verify pg_get_wal_blocks_info generates correct SQL
        func2 = pg_get_wal_blocks_info(dialect, "0/16E9130", "0/16E9200")
        sql2, _ = func2.to_sql()
        assert "pg_get_wal_blocks_info" in sql2.lower()

    @pytest.mark.asyncio
    async def test_async_pg_get_wal_records_info_with_lsn(self, async_pg_walinspect_env):
        """Test getting WAL records info with explicit LSN range asynchronously.

        pg_get_wal_records_info requires both start_lsn and end_lsn parameters.
        We obtain valid LSN values from pg_current_wal_lsn() first.
        """
        backend, dialect = async_pg_walinspect_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Get current WAL position to use as LSN values
            current_lsn = await _async_get_current_wal_lsn(backend, dialect)

            # Query WAL records info with the obtained LSN
            wal_func = pg_get_wal_records_info(
                dialect,
                current_lsn,
                current_lsn,
            )
            query = QueryExpression(
                dialect=dialect,
                select=[wal_func.as_("wal_records")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if ("permission denied" in error_msg
                or "must be superuser" in error_msg
                or "could not find a valid record" in error_msg):
                pytest.skip(
                    "pg_walinspect requires superuser privileges or valid WAL range"
                )
            raise
