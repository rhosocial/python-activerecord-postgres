# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_cron.py
"""
pg_cron job scheduling functionality implementation.

This module provides the PostgresPgCronMixin class that adds support for
pg_cron extension features including job scheduling and management.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass


class PostgresPgCronMixin:
    """pg_cron job scheduling functionality implementation."""

    def supports_pg_cron(self) -> bool:
        """Check if pg_cron extension is available."""
        return self.is_extension_installed("pg_cron")

    def supports_pg_cron_schedule(self) -> bool:
        """Check if pg_cron supports job scheduling."""
        return self.check_extension_feature("pg_cron", "schedule")

    def supports_pg_cron_cancel(self) -> bool:
        """Check if pg_cron supports job cancellation."""
        return self.check_extension_feature("pg_cron", "cancel")

    def format_cron_schedule(self, schedule: str, command: str, comment: Optional[str] = None) -> str:
        """Format a pg_cron job schedule.

        Args:
            schedule: Cron schedule expression (e.g., '0 * * * *')
            command: SQL command to execute
            comment: Optional job comment

        Returns:
            SQL SELECT statement for job scheduling
        """
        parts = [f"schedule := '{schedule}'", f"command := $$ {command} $$"]
        if comment:
            parts.append(f"comment := '{comment}'")

        return f"SELECT cron.schedule({', '.join(parts)})"

    def format_cron_unschedule(self, job_id: int) -> str:
        """Format a pg_cron job cancellation.

        Args:
            job_id: Job ID to cancel

        Returns:
            SQL SELECT statement for job cancellation
        """
        return f"SELECT cron.unschedule(job_id := {job_id})"

    def format_cron_job_run(self, job_id: int) -> str:
        """Format a pg_cron job run immediate execution.

        Args:
            job_id: Job ID to run immediately

        Returns:
            SQL SELECT statement for immediate job execution
        """
        return f"SELECT cron.run(job_id := {job_id})"