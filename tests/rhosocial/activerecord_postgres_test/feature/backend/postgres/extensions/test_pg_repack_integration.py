# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_repack_integration.py

"""Integration tests for the pg_repack extension.

These tests require a PostgreSQL database with the pg_repack extension installed.
Tests will be automatically skipped if the extension is not available.

pg_repack is primarily a command-line tool, not a set of SQL-callable functions.
The internal SQL functions in the "repack" schema (repack_apply, repack_swap, etc.)
are not intended for direct use. The function factory provides repack_version()
for convenience, but it may not exist in all pg_repack versions.

To actually repack a table, use the pg_repack command-line utility:
    pg_repack --table=tablename dbname

All database operations use expression objects, not raw SQL strings.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    async_ensure_extension_installed,
    ensure_extension_installed,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_repack import (
    repack_version,
)
from rhosocial.activerecord.backend.expression import QueryExpression
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def pg_repack_env(postgres_backend_single):
    """Independent test environment for pg_repack extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_repack")
    dialect = backend.dialect

    yield backend, dialect


class TestPgRepackIntegration:
    """Integration tests for pg_repack extension.

    Since pg_repack is a CLI tool with no public SQL API, the integration
    test verifies the extension is installed and the function factory
    generates valid SQL expressions.
    """

    def test_extension_installed(self, pg_repack_env):
        """Test that pg_repack extension is installed."""
        backend, dialect = pg_repack_env
        # If we got here, ensure_extension_installed succeeded
        assert dialect.is_extension_installed("pg_repack")

    def test_repack_version_sql_generation(self, pg_repack_env):
        """Test that repack_version generates valid SQL expression.

        Note: repack.repack_version() may not exist in all pg_repack
        versions. This test only verifies SQL generation, not execution.
        """
        backend, dialect = pg_repack_env

        version_func = repack_version(dialect)
        sql, params = version_func.to_sql()
        assert "repack_version" in sql.lower()

    def test_repack_version_execution(self, pg_repack_env):
        """Test repack_version execution if the function exists.

        Note: repack.repack_version() may not exist in all pg_repack
        versions. The test will be skipped if the function is unavailable.
        """
        backend, dialect = pg_repack_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            version_func = repack_version(dialect)
            query = QueryExpression(
                dialect=dialect,
                select=[version_func.as_("version")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if ("does not exist" in error_msg
                or "permission denied" in error_msg
                or "must be superuser" in error_msg):
                pytest.skip(
                    "pg_repack version function not available or requires superuser"
                )
            raise


@pytest_asyncio.fixture
async def async_pg_repack_env(async_postgres_backend_single):
    """Independent test environment for pg_repack extension (async)."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_repack")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPgRepackIntegration:
    """Async integration tests for pg_repack extension.

    Since pg_repack is a CLI tool with no public SQL API, the integration
    test verifies the extension is installed and the function factory
    generates valid SQL expressions.
    """

    @pytest.mark.asyncio
    async def test_async_extension_installed(self, async_pg_repack_env):
        """Test that pg_repack extension is installed."""
        backend, dialect = async_pg_repack_env
        # If we got here, async_ensure_extension_installed succeeded
        assert dialect.is_extension_installed("pg_repack")

    @pytest.mark.asyncio
    async def test_async_repack_version_sql_generation(self, async_pg_repack_env):
        """Test that repack_version generates valid SQL expression.

        Note: repack.repack_version() may not exist in all pg_repack
        versions. This test only verifies SQL generation, not execution.
        """
        backend, dialect = async_pg_repack_env

        version_func = repack_version(dialect)
        sql, params = version_func.to_sql()
        assert "repack_version" in sql.lower()

    @pytest.mark.asyncio
    async def test_async_repack_version_execution(self, async_pg_repack_env):
        """Test repack_version execution if the function exists.

        Note: repack.repack_version() may not exist in all pg_repack
        versions. The test will be skipped if the function is unavailable.
        """
        backend, dialect = async_pg_repack_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            version_func = repack_version(dialect)
            query = QueryExpression(
                dialect=dialect,
                select=[version_func.as_("version")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if ("does not exist" in error_msg
                or "permission denied" in error_msg
                or "must be superuser" in error_msg):
                pytest.skip(
                    "pg_repack version function not available or requires superuser"
                )
            raise
