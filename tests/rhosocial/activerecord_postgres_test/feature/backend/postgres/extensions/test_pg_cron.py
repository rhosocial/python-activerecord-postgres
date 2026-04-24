# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_cron.py
"""Unit tests for PostgreSQL pg_cron extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pg_cron import PostgresPgCronMixin


class TestPgCronMixin:
    """Test pg_cron extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresPgCronMixin()

    def test_format_cron_schedule(self):
        """Test cron schedule formatting."""
        result = self.mixin.format_cron_schedule('0 * * * *', 'SELECT 1')
        assert "cron.schedule" in result

    def test_format_cron_schedule_with_comment(self):
        """Test cron schedule formatting with comment."""
        result = self.mixin.format_cron_schedule('0 * * * *', 'SELECT 1', comment='hourly job')
        assert "cron.schedule" in result

    def test_format_cron_unschedule(self):
        """Test cron unschedule formatting."""
        result = self.mixin.format_cron_unschedule(job_id=1)
        assert "cron.unschedule" in result

    def test_format_cron_job_run(self):
        """Test cron job run formatting."""
        result = self.mixin.format_cron_job_run(job_id=1)
        assert "cron.run" in result
