# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_cron.py
"""pg_cron extension protocol definition.

This module defines the protocol for pg_cron job scheduling
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgCronSupport(Protocol):
    """pg_cron job scheduling extension protocol.

    Feature Source: Extension support (requires pg_cron extension)

    pg_cron provides cron-like job scheduling:
    - Schedule jobs using cron syntax
    - Run jobs in background
    - Cancel running jobs

    Extension Information:
    - Extension name: pg_cron
    - Install command: CREATE EXTENSION pg_cron;
    - Minimum version: 1.6
    - Documentation: https://github.com/citusdata/pg_cron

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'pg_cron';
    - Programmatic detection: dialect.is_extension_installed('pg_cron')
    """

    def supports_pg_cron(self) -> bool:
        """Whether pg_cron extension is available.

        Requires pg_cron extension installed.
        """
        ...

    def supports_pg_cron_schedule(self) -> bool:
        """Whether pg_cron supports job scheduling.

        Requires pg_cron extension.
        Supports scheduling jobs with cron syntax.
        """
        ...

    def supports_pg_cron_cancel(self) -> bool:
        """Whether pg_cron supports job cancellation.

        Requires pg_cron extension.
        Supports cancelling scheduled jobs.
        """
        ...