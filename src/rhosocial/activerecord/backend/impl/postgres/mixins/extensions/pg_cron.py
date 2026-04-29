# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_cron.py
"""
PostgreSQL pg_cron job scheduling functionality mixin.

This module provides functionality to check pg_cron extension features,
including job scheduling, cancellation, and immediate execution support.

For SQL expression generation, use the function factories in
``functions/pg_cron.py`` instead of the removed format_* methods.
"""


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

    def supports_pg_cron_run(self) -> bool:
        """Check if pg_cron supports immediate job execution."""
        return self.check_extension_feature("pg_cron", "run")
