# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_cron.py
"""
PostgreSQL pg_cron Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_cron
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pg_cron extension provides a cron-based job scheduler for PostgreSQL
that runs inside the database as a background worker.

PostgreSQL Documentation: https://github.com/citusdata/pg_cron

The pg_cron extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_cron;

Supported functions:
- cron_schedule: Schedule a new cron job
- cron_unschedule: Remove a scheduled cron job
- cron_run: Run a scheduled cron job immediately

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ============== Job Scheduling ==============

def cron_schedule(
    dialect: "SQLDialectBase",
    schedule: str,
    command: str,
    comment: Optional[str] = None,
) -> core.FunctionCall:
    """Schedule a new cron job.

    Schedules a new job to be run at the specified schedule. The
    schedule uses standard cron syntax with 5 fields:
    minute (0-59), hour (0-23), day of month (1-31),
    month (1-12), day of week (0-6, where 0 is Sunday).

    Special schedule strings are also supported:
    - '* * * * *': Every minute
    - '0 * * * *': Every hour
    - '0 0 * * *': Every day at midnight
    - '0 0 * * 0': Every Sunday at midnight

    Args:
        dialect: The SQL dialect instance
        schedule: Cron schedule expression (e.g., '0 * * * *' for hourly)
        command: SQL command to execute
        comment: Optional comment describing the job

    Returns:
        FunctionCall for cron_schedule(schedule, command[, comment])

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> cron_schedule(dialect, '0 * * * *', 'DELETE FROM logs WHERE created < now() - interval ''7 days''')
        >>> cron_schedule(dialect, '30 3 * * *', 'VACUUM ANALYZE', 'Nightly vacuum')
    """
    if comment is not None:
        return core.FunctionCall(
            dialect, "cron.schedule",
            schedule if isinstance(schedule, bases.BaseExpression) else core.Literal(dialect, schedule),
            command if isinstance(command, bases.BaseExpression) else core.Literal(dialect, command),
            comment if isinstance(comment, bases.BaseExpression) else core.Literal(dialect, comment),
        )
    return core.FunctionCall(
        dialect, "cron.schedule",
        schedule if isinstance(schedule, bases.BaseExpression) else core.Literal(dialect, schedule),
        command if isinstance(command, bases.BaseExpression) else core.Literal(dialect, command),
    )


def cron_unschedule(
    dialect: "SQLDialectBase",
    job_id: int,
) -> core.FunctionCall:
    """Remove a scheduled cron job.

    Removes the cron job with the specified job ID. The job ID
    is returned by the cron_schedule function when the job is created.

    Args:
        dialect: The SQL dialect instance
        job_id: The ID of the cron job to remove (positive integer)

    Returns:
        FunctionCall for cron.unschedule(job_id)

    Example:
        >>> cron_unschedule(dialect, 1)
        >>> cron_unschedule(dialect, job_id_expr)
    """
    return core.FunctionCall(
        dialect, "cron.unschedule",
        job_id if isinstance(job_id, bases.BaseExpression) else core.Literal(dialect, job_id),
    )


def cron_run(
    dialect: "SQLDialectBase",
    job_id: int,
) -> core.FunctionCall:
    """Run a scheduled cron job immediately.

    Triggers immediate execution of the cron job with the specified
    job ID, regardless of its schedule. This is useful for testing
    or running jobs on demand.

    Args:
        dialect: The SQL dialect instance
        job_id: The ID of the cron job to run immediately (positive integer)

    Returns:
        FunctionCall for cron.run(job_id)

    Example:
        >>> cron_run(dialect, 1)
        >>> cron_run(dialect, job_id_expr)
    """
    return core.FunctionCall(
        dialect, "cron.run",
        job_id if isinstance(job_id, bases.BaseExpression) else core.Literal(dialect, job_id),
    )


__all__ = [
    # Job scheduling
    "cron_schedule",
    "cron_unschedule",
    "cron_run",
]
