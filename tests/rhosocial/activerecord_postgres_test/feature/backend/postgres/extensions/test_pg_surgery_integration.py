# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_surgery_integration.py

"""Integration tests for the pg_surgery extension.

These tests require a PostgreSQL database with the pg_surgery extension installed.
Tests will be automatically skipped if the extension is not available.
pg_surgery requires superuser privileges. Tests wrap operations in try/except
and skip on permission errors.
All database operations use expression objects, not raw SQL strings.

The actual pg_surgery functions are:
- heap_force_freeze(reloid regclass, tids tid[])
- heap_force_kill(reloid regclass, tids tid[])

These functions require regclass and tid[] types, which are complex to
construct with expression objects. The integration test focuses on verifying
the extension is installed rather than calling these dangerous functions.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    async_ensure_extension_installed,
    ensure_extension_installed,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_surgery import (
    heap_force_freeze,
    heap_force_kill,
)


@pytest.fixture
def pg_surgery_env(postgres_backend_single):
    """Independent test environment for pg_surgery extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_surgery")
    dialect = backend.dialect

    yield backend, dialect


class TestPgSurgeryIntegration:
    """Integration tests for pg_surgery extension.

    pg_surgery functions (heap_force_freeze, heap_force_kill) require
    regclass and tid[] parameters which are complex to construct with
    expressions. The test verifies the extension is installed and the
    function factories generate valid SQL.
    """

    def test_extension_installed(self, pg_surgery_env):
        """Test that pg_surgery extension is installed."""
        backend, dialect = pg_surgery_env
        # If we got here, ensure_extension_installed succeeded

    def test_heap_force_freeze_sql_generation(self, pg_surgery_env):
        """Test that heap_force_freeze generates valid SQL expression.

        This tests SQL generation only, not execution, since heap_force_freeze
        requires regclass and tid[] parameters and superuser privileges.
        """
        backend, dialect = pg_surgery_env

        # Verify the function factory produces a FunctionCall
        func = heap_force_freeze(dialect, "my_table", "'{(0,1)}'::tid[]")
        sql, params = func.to_sql()
        assert "heap_force_freeze" in sql.lower()

    def test_heap_force_kill_sql_generation(self, pg_surgery_env):
        """Test that heap_force_kill generates valid SQL expression.

        This tests SQL generation only, not execution, since heap_force_kill
        requires regclass and tid[] parameters and superuser privileges.
        """
        backend, dialect = pg_surgery_env

        # Verify the function factory produces a FunctionCall
        func = heap_force_kill(dialect, "my_table", "'{(0,1)}'::tid[]")
        sql, params = func.to_sql()
        assert "heap_force_kill" in sql.lower()


@pytest_asyncio.fixture
async def async_pg_surgery_env(async_postgres_backend_single):
    """Independent test environment for pg_surgery extension (async)."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_surgery")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPgSurgeryIntegration:
    """Async integration tests for pg_surgery extension.

    pg_surgery functions (heap_force_freeze, heap_force_kill) require
    regclass and tid[] parameters which are complex to construct with
    expressions. The test verifies the extension is installed and the
    function factories generate valid SQL.
    """

    @pytest.mark.asyncio
    async def test_async_extension_installed(self, async_pg_surgery_env):
        """Test that pg_surgery extension is installed."""
        backend, dialect = async_pg_surgery_env
        # If we got here, async_ensure_extension_installed succeeded

    @pytest.mark.asyncio
    async def test_async_heap_force_freeze_sql_generation(self, async_pg_surgery_env):
        """Test that heap_force_freeze generates valid SQL expression.

        This tests SQL generation only, not execution, since heap_force_freeze
        requires regclass and tid[] parameters and superuser privileges.
        """
        backend, dialect = async_pg_surgery_env

        # Verify the function factory produces a FunctionCall
        func = heap_force_freeze(dialect, "my_table", "'{(0,1)}'::tid[]")
        sql, params = func.to_sql()
        assert "heap_force_freeze" in sql.lower()

    @pytest.mark.asyncio
    async def test_async_heap_force_kill_sql_generation(self, async_pg_surgery_env):
        """Test that heap_force_kill generates valid SQL expression.

        This tests SQL generation only, not execution, since heap_force_kill
        requires regclass and tid[] parameters and superuser privileges.
        """
        backend, dialect = async_pg_surgery_env

        # Verify the function factory produces a FunctionCall
        func = heap_force_kill(dialect, "my_table", "'{(0,1)}'::tid[]")
        sql, params = func.to_sql()
        assert "heap_force_kill" in sql.lower()
