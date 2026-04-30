# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_partman_integration.py

"""Integration tests for the pg_partman extension.

These tests require a PostgreSQL database with the pg_partman extension installed.
Tests will be automatically skipped if the extension is not available.
pg_partman requires superuser privileges for most operations. Tests wrap
operations in try/except and skip on permission errors.
All database operations use expression objects, not raw SQL strings.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    async_ensure_extension_installed,
    ensure_extension_installed,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_partman import (
    create_parent,
    run_maintenance,
)


@pytest.fixture
def pg_partman_env(postgres_backend_single):
    """Independent test environment for pg_partman extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_partman")
    dialect = backend.dialect

    yield backend, dialect


class TestPgPartmanIntegration:
    """Integration tests for pg_partman partition management."""

    def test_extension_installed(self, pg_partman_env):
        """Test that pg_partman extension is installed."""
        backend, dialect = pg_partman_env
        # If we got here, ensure_extension_installed succeeded
        assert dialect.is_extension_installed("pg_partman")

    def test_create_parent_sql_generation(self, pg_partman_env):
        """Test that create_parent generates valid SQL expression.

        This tests SQL generation only, not execution, since create_parent
        requires superuser privileges and a pre-existing partitioned table.
        """
        backend, dialect = pg_partman_env

        part_func = create_parent(
            dialect,
            "public.test_partman_events",
            "event_time",
            "1 day",
        )
        sql, params = part_func.to_sql()
        assert "create_parent" in sql.lower()

    def test_run_maintenance_sql_generation(self, pg_partman_env):
        """Test that run_maintenance generates valid SQL expression."""
        backend, dialect = pg_partman_env

        maint_func = run_maintenance(dialect)
        sql, params = maint_func.to_sql()
        assert "run_maintenance" in sql.lower()


@pytest_asyncio.fixture
async def async_pg_partman_env(async_postgres_backend_single):
    """Independent test environment for pg_partman extension (async)."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_partman")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPgPartmanIntegration:
    """Async integration tests for pg_partman partition management."""

    @pytest.mark.asyncio
    async def test_async_extension_installed(self, async_pg_partman_env):
        """Test that pg_partman extension is installed."""
        backend, dialect = async_pg_partman_env
        # If we got here, async_ensure_extension_installed succeeded
        assert dialect.is_extension_installed("pg_partman")

    @pytest.mark.asyncio
    async def test_async_create_parent_sql_generation(self, async_pg_partman_env):
        """Test that create_parent generates valid SQL expression.

        This tests SQL generation only, not execution, since create_parent
        requires superuser privileges and a pre-existing partitioned table.
        """
        backend, dialect = async_pg_partman_env

        part_func = create_parent(
            dialect,
            "public.test_partman_events",
            "event_time",
            "1 day",
        )
        sql, params = part_func.to_sql()
        assert "create_parent" in sql.lower()

    @pytest.mark.asyncio
    async def test_async_run_maintenance_sql_generation(self, async_pg_partman_env):
        """Test that run_maintenance generates valid SQL expression."""
        backend, dialect = async_pg_partman_env

        maint_func = run_maintenance(dialect)
        sql, params = maint_func.to_sql()
        assert "run_maintenance" in sql.lower()
