# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_stat_statements_integration.py

"""Integration tests for PostgreSQL pg_stat_statements extension with real database.

These tests require a live PostgreSQL connection with pg_stat_statements extension installed
and test:
- Querying the pg_stat_statements view
- Resetting pg_stat_statements statistics via function factory

The pg_stat_statements extension requires shared_preload_libraries = 'pg_stat_statements'
in postgresql.conf. Additionally, full access to pg_stat_statements_reset() requires
superuser privileges. Tests wrap operations in try/except and skip if permission is denied.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    TableExpression,
    Column,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_stat_statements import (
    pg_stat_statements_reset,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def pg_stat_statements_env(postgres_backend_single):
    """Independent test environment for pg_stat_statements extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_stat_statements")
    dialect = backend.dialect

    yield backend, dialect


class TestPgStatStatementsIntegration:
    """Integration tests for pg_stat_statements extension."""

    def test_pg_stat_statements_query(self, pg_stat_statements_env):
        """Test querying the pg_stat_statements view using QueryExpression."""
        backend, dialect = pg_stat_statements_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Query the pg_stat_statements view using QueryExpression
            query = QueryExpression(
                dialect=dialect,
                select=[
                    Column(dialect, "query"),
                    Column(dialect, "calls"),
                ],
                from_=TableExpression(dialect, "pg_stat_statements"),
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            # If we got here, the query succeeded
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if ("permission denied" in error_msg
                or "must be superuser" in error_msg
                or "shared_preload_libraries" in error_msg):
                pytest.skip("pg_stat_statements requires shared_preload_libraries or superuser privileges")
            raise

    def test_pg_stat_statements_reset(self, pg_stat_statements_env):
        """Test resetting pg_stat_statements statistics via function factory."""
        backend, dialect = pg_stat_statements_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Use pg_stat_statements_reset function factory to reset statistics
            reset_func = pg_stat_statements_reset(dialect)
            query = QueryExpression(
                dialect=dialect,
                select=[reset_func],
            )
            sql, params = query.to_sql()
            backend.execute(sql, params, options=opts)
            # If we got here, the reset succeeded
        except Exception as e:
            error_msg = str(e).lower()
            if ("permission denied" in error_msg
                or "must be superuser" in error_msg
                or "shared_preload_libraries" in error_msg):
                pytest.skip("pg_stat_statements_reset requires shared_preload_libraries or superuser privileges")
            raise


@pytest_asyncio.fixture
async def async_pg_stat_statements_env(async_postgres_backend_single):
    """Independent async test environment for pg_stat_statements extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_stat_statements")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPgStatStatementsIntegration:
    """Async integration tests for pg_stat_statements extension."""

    @pytest.mark.asyncio
    async def test_async_pg_stat_statements_query(self, async_pg_stat_statements_env):
        """Test querying the pg_stat_statements view using QueryExpression asynchronously."""
        backend, dialect = async_pg_stat_statements_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Query the pg_stat_statements view using QueryExpression
            query = QueryExpression(
                dialect=dialect,
                select=[
                    Column(dialect, "query"),
                    Column(dialect, "calls"),
                ],
                from_=TableExpression(dialect, "pg_stat_statements"),
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            # If we got here, the query succeeded
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if ("permission denied" in error_msg
                or "must be superuser" in error_msg
                or "shared_preload_libraries" in error_msg):
                pytest.skip("pg_stat_statements requires shared_preload_libraries or superuser privileges")
            raise

    @pytest.mark.asyncio
    async def test_async_pg_stat_statements_reset(self, async_pg_stat_statements_env):
        """Test resetting pg_stat_statements statistics via function factory asynchronously."""
        backend, dialect = async_pg_stat_statements_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Use pg_stat_statements_reset function factory to reset statistics
            reset_func = pg_stat_statements_reset(dialect)
            query = QueryExpression(
                dialect=dialect,
                select=[reset_func],
            )
            sql, params = query.to_sql()
            await backend.execute(sql, params, options=opts)
            # If we got here, the reset succeeded
        except Exception as e:
            error_msg = str(e).lower()
            if ("permission denied" in error_msg
                or "must be superuser" in error_msg
                or "shared_preload_libraries" in error_msg):
                pytest.skip("pg_stat_statements_reset requires shared_preload_libraries or superuser privileges")
            raise
