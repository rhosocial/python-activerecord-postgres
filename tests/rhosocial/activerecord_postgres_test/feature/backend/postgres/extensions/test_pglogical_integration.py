# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pglogical_integration.py

"""Integration tests for the pglogical extension.

These tests require a PostgreSQL database with the pglogical extension installed.
Tests will be automatically skipped if the extension is not available.
pglogical requires superuser privileges for node and replication management.
Tests wrap operations in try/except and skip on permission errors.
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
from rhosocial.activerecord.backend.impl.postgres.functions.pglogical import (
    pglogical_create_node,
    pglogical_show_subscription_status,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def pglogical_env(postgres_backend_single):
    """Independent test environment for pglogical extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pglogical")
    dialect = backend.dialect

    yield backend, dialect


class TestPglogicalIntegration:
    """Integration tests for pglogical logical replication."""

    def test_pglogical_create_node(self, pglogical_env):
        """Test creating a pglogical node using pglogical_create_node."""
        backend, dialect = pglogical_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Create a pglogical node
            node_func = pglogical_create_node(
                dialect,
                "test_node",
                "host=localhost dbname=test",
            )
            query = QueryExpression(
                dialect=dialect,
                select=[node_func.as_("node_result")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pglogical requires superuser privileges")
            raise

    def test_pglogical_show_subscription_status(self, pglogical_env):
        """Test showing subscription status using pglogical_show_subscription_status."""
        backend, dialect = pglogical_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Show all subscription statuses
            status_func = pglogical_show_subscription_status(dialect)
            query = QueryExpression(
                dialect=dialect,
                select=[status_func.as_("subscription_status")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pglogical requires superuser privileges")
            raise


@pytest_asyncio.fixture
async def async_pglogical_env(async_postgres_backend_single):
    """Independent async test environment for pglogical extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pglogical")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPglogicalIntegration:
    """Async integration tests for pglogical logical replication."""

    @pytest.mark.asyncio
    async def test_async_pglogical_create_node(self, async_pglogical_env):
        """Test creating a pglogical node using pglogical_create_node asynchronously."""
        backend, dialect = async_pglogical_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Create a pglogical node
            node_func = pglogical_create_node(
                dialect,
                "test_node",
                "host=localhost dbname=test",
            )
            query = QueryExpression(
                dialect=dialect,
                select=[node_func.as_("node_result")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pglogical requires superuser privileges")
            raise

    @pytest.mark.asyncio
    async def test_async_pglogical_show_subscription_status(self, async_pglogical_env):
        """Test showing subscription status using pglogical_show_subscription_status asynchronously."""
        backend, dialect = async_pglogical_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Show all subscription statuses
            status_func = pglogical_show_subscription_status(dialect)
            query = QueryExpression(
                dialect=dialect,
                select=[status_func.as_("subscription_status")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pglogical requires superuser privileges")
            raise
