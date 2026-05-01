# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_cron.py
"""
Unit tests for PostgreSQL pg_cron extension functions.

Tests for:
- cron_schedule
- cron_unschedule
- cron_run
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_cron import (
    cron_schedule,
    cron_unschedule,
    cron_run,
)


class TestPostgresPgCronFunctions:
    """Tests for pg_cron extension function factories."""

    def test_cron_schedule(self):
        """cron_schedule should return FunctionCall with cron.schedule."""
        dialect = PostgresDialect((14, 0, 0))
        result = cron_schedule(dialect, "0 * * * *", "SELECT 1")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cron.schedule" in sql.lower()

    def test_cron_schedule_with_comment(self):
        """cron_schedule with comment should return FunctionCall with cron.schedule."""
        dialect = PostgresDialect((14, 0, 0))
        result = cron_schedule(dialect, "0 * * * *", "SELECT 1", comment="hourly job")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cron.schedule" in sql.lower()

    def test_cron_unschedule(self):
        """cron_unschedule should return FunctionCall with cron.unschedule."""
        dialect = PostgresDialect((14, 0, 0))
        result = cron_unschedule(dialect, job_id=1)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cron.unschedule" in sql.lower()

    def test_cron_run(self):
        """cron_run should return FunctionCall with cron.run."""
        dialect = PostgresDialect((14, 0, 0))
        result = cron_run(dialect, job_id=1)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cron.run" in sql.lower()
