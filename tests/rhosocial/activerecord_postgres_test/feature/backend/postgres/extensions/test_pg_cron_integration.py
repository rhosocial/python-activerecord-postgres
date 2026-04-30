# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_cron_integration.py

"""Integration tests for the pg_cron extension.

These tests require a PostgreSQL database with the pg_cron extension installed.
Tests will be automatically skipped if the extension is not available.
pg_cron requires superuser privileges for scheduling jobs. Tests wrap
operations in try/except and skip on permission errors.
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
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.postgres.functions.pg_cron import (
    cron_schedule,
    cron_unschedule,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def pg_cron_env(postgres_backend_single):
    """Independent test environment for pg_cron extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_cron")
    dialect = backend.dialect

    yield backend, dialect


class TestPgCronIntegration:
    """Integration tests for pg_cron job scheduling."""

    def test_cron_schedule(self, pg_cron_env):
        """Test scheduling a cron job using cron_schedule."""
        backend, dialect = pg_cron_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Schedule a simple hourly job
            schedule_func = cron_schedule(
                dialect,
                "0 * * * *",
                Literal(dialect, "SELECT 1"),
            )
            query = QueryExpression(
                dialect=dialect,
                select=[schedule_func.as_("job_id")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
            assert len(result.data) > 0
            # cron_schedule returns the job ID
            job_id = result.data[0]["job_id"]
            assert job_id is not None

            # Cleanup: unschedule the job
            unschedule_func = cron_unschedule(dialect, job_id)
            query = QueryExpression(
                dialect=dialect,
                select=[unschedule_func],
            )
            sql, params = query.to_sql()
            backend.execute(sql, params, options=opts)
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pg_cron requires superuser privileges")
            raise

    def test_cron_unschedule(self, pg_cron_env):
        """Test unscheduling a cron job using cron_unschedule."""
        backend, dialect = pg_cron_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # First, schedule a job
            schedule_func = cron_schedule(
                dialect,
                "0 * * * *",
                Literal(dialect, "SELECT 1"),
            )
            query = QueryExpression(
                dialect=dialect,
                select=[schedule_func.as_("job_id")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            job_id = result.data[0]["job_id"]

            # Then, unschedule the job
            unschedule_func = cron_unschedule(dialect, job_id)
            query = QueryExpression(
                dialect=dialect,
                select=[unschedule_func.as_("unschedule_result")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pg_cron requires superuser privileges")
            raise


@pytest_asyncio.fixture
async def async_pg_cron_env(async_postgres_backend_single):
    """Independent async test environment for pg_cron extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_cron")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncPgCronIntegration:
    """Async integration tests for pg_cron job scheduling."""

    @pytest.mark.asyncio
    async def test_async_cron_schedule(self, async_pg_cron_env):
        """Test scheduling a cron job using cron_schedule asynchronously."""
        backend, dialect = async_pg_cron_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # Schedule a simple hourly job
            schedule_func = cron_schedule(
                dialect,
                "0 * * * *",
                Literal(dialect, "SELECT 1"),
            )
            query = QueryExpression(
                dialect=dialect,
                select=[schedule_func.as_("job_id")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
            assert len(result.data) > 0
            # cron_schedule returns the job ID
            job_id = result.data[0]["job_id"]
            assert job_id is not None

            # Cleanup: unschedule the job
            unschedule_func = cron_unschedule(dialect, job_id)
            query = QueryExpression(
                dialect=dialect,
                select=[unschedule_func],
            )
            sql, params = query.to_sql()
            await backend.execute(sql, params, options=opts)
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pg_cron requires superuser privileges")
            raise

    @pytest.mark.asyncio
    async def test_async_cron_unschedule(self, async_pg_cron_env):
        """Test unscheduling a cron job using cron_unschedule asynchronously."""
        backend, dialect = async_pg_cron_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            # First, schedule a job
            schedule_func = cron_schedule(
                dialect,
                "0 * * * *",
                Literal(dialect, "SELECT 1"),
            )
            query = QueryExpression(
                dialect=dialect,
                select=[schedule_func.as_("job_id")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            job_id = result.data[0]["job_id"]

            # Then, unschedule the job
            unschedule_func = cron_unschedule(dialect, job_id)
            query = QueryExpression(
                dialect=dialect,
                select=[unschedule_func.as_("unschedule_result")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "permission denied" in error_msg or "must be superuser" in error_msg:
                pytest.skip("pg_cron requires superuser privileges")
            raise
