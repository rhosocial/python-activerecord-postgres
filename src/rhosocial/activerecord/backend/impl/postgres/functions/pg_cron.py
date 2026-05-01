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

from typing import Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    For string inputs, generates a literal expression. For
    BaseExpression inputs, returns them unchanged.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Job Scheduling ==============

def cron_schedule(
    dialect: "SQLDialectBase",
    schedule: Union[str, "bases.BaseExpression"],
    command: Union[str, "bases.BaseExpression"],
    comment: Optional[Union[str, "bases.BaseExpression"]] = None,
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
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> cron_schedule(dialect, '0 * * * *', 'DELETE FROM logs WHERE created < now() - interval ''7 days''')
        >>> cron_schedule(dialect, '30 3 * * *', 'VACUUM ANALYZE', 'Nightly vacuum')
    """
    if comment is not None:
        return core.FunctionCall(
            dialect, "cron.schedule",
            _convert_to_expression(dialect, schedule),
            _convert_to_expression(dialect, command),
            _convert_to_expression(dialect, comment),
        )
    return core.FunctionCall(
        dialect, "cron.schedule",
        _convert_to_expression(dialect, schedule),
        _convert_to_expression(dialect, command),
    )


def cron_unschedule(
    dialect: "SQLDialectBase",
    job_id: Union[int, "bases.BaseExpression"],
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
        _convert_to_expression(dialect, job_id),
    )


def cron_run(
    dialect: "SQLDialectBase",
    job_id: Union[int, "bases.BaseExpression"],
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
        _convert_to_expression(dialect, job_id),
    )


__all__ = [
    # Job scheduling
    "cron_schedule",
    "cron_unschedule",
    "cron_run",
]
