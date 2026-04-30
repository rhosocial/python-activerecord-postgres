# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pgaudit_integration.py

"""Integration tests for the pgaudit extension.

These tests require a PostgreSQL database with the pgaudit extension installed.
Tests will be automatically skipped if the extension is not available.
pgaudit configuration requires superuser privileges. Tests wrap operations
in try/except and skip on permission errors.
All database operations use expression objects, not raw SQL strings.
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
from rhosocial.activerecord.backend.impl.postgres.functions.pgaudit import (
    pgaudit_set_role,
    pgaudit_log_level,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def pgaudit_env(postgres_backend_single):
    """Independent test environment for pgaudit extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pgaudit")
    dialect = backend.dialect

    yield backend, dialect


class TestPgauditIntegration:
    """Integration tests for pgaudit audit logging configuration."""

    def test_pgaudit_set_role(self, pgaudit_env):
        """Test setting pgaudit role using pgaudit_set_role."""
        backend, dialect = pgaudit_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Set the pgaudit role for auditing
            role_func = pgaudit_set_role(dialect, "audit_role")
            query = QueryExpression(
                dialect=dialect,
                select=[role_func.as_("set_config_result")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pgaudit requires superuser privileges")
            raise

    def test_pgaudit_log_level(self, pgaudit_env):
        """Test setting pgaudit log level using pgaudit_log_level."""
        backend, dialect = pgaudit_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Set the pgaudit log level
            level_func = pgaudit_log_level(dialect, "log")
            query = QueryExpression(
                dialect=dialect,
                select=[level_func.as_("set_config_result")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pgaudit requires superuser privileges")
            raise


@pytest_asyncio.fixture
async def async_pgaudit_env(async_postgres_backend_single):
    """Independent async test environment for pgaudit extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pgaudit")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPgauditIntegration:
    """Async integration tests for pgaudit audit logging configuration."""

    @pytest.mark.asyncio
    async def test_async_pgaudit_set_role(self, async_pgaudit_env):
        """Test setting pgaudit role using pgaudit_set_role asynchronously."""
        backend, dialect = async_pgaudit_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Set the pgaudit role for auditing
            role_func = pgaudit_set_role(dialect, "audit_role")
            query = QueryExpression(
                dialect=dialect,
                select=[role_func.as_("set_config_result")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pgaudit requires superuser privileges")
            raise

    @pytest.mark.asyncio
    async def test_async_pgaudit_log_level(self, async_pgaudit_env):
        """Test setting pgaudit log level using pgaudit_log_level asynchronously."""
        backend, dialect = async_pgaudit_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Set the pgaudit log level
            level_func = pgaudit_log_level(dialect, "log")
            query = QueryExpression(
                dialect=dialect,
                select=[level_func.as_("set_config_result")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pgaudit requires superuser privileges")
            raise
