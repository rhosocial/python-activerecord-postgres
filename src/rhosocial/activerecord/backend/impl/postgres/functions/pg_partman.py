# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_partman.py
"""
PostgreSQL pg_partman Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_partman
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pg_partman extension provides partition management for PostgreSQL. It
automates the creation and maintenance of table partitions based on time
or ID ranges.

PostgreSQL Documentation: https://github.com/pgpartman/pg_partman

The pg_partman extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_partman;

Supported functions:
- create_parent: Create a partition set for a parent table
- run_maintenance: Run partition maintenance

Note: pg_partman functions are installed in the public schema, not a
dedicated "partman" schema. The actual function signatures are:

- create_parent(p_parent_table text, p_control text, p_interval text,
    p_type text DEFAULT 'native', p_premake int DEFAULT 4, ...)
- run_maintenance(p_parent_table text DEFAULT NULL::text,
    p_run_maintenance boolean DEFAULT true, ...)

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, etc.)
"""

from typing import Union, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, int, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings, integers, and existing BaseExpression objects.

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


# ============== Partition Management Functions ==============

def create_parent(
    dialect: "SQLDialectBase",
    parent_table: Union[str, "bases.BaseExpression"],
    control: Union[str, "bases.BaseExpression"],
    interval: Union[str, "bases.BaseExpression"],
    partition_type: Union[str, "bases.BaseExpression"] = "native",
    premake: Union[int, "bases.BaseExpression"] = 4,
) -> core.FunctionCall:
    """Create a partition set for a parent table.

    Sets up partitioning for the specified parent table using pg_partman.
    This is the primary function for creating both time-based and ID-based
    partition sets.

    The actual PostgreSQL function signature is:
        create_parent(p_parent_table text, p_control text, p_interval text,
                      p_type text DEFAULT 'native', p_premake int DEFAULT 4, ...)

    Args:
        dialect: The SQL dialect instance
        parent_table: The name of the parent table to partition
                      (e.g., 'public.metrics', 'logs')
        control: The column used as the partition control key
                 (e.g., 'created_at' for time, 'id' for serial)
        interval: The partitioning interval
                  (e.g., '1 day', '1 week', '1 month', '10000' for ID-based)
        partition_type: Type of partitioning (default: 'native').
                        Use 'native' for declarative partitioning.
        premake: Number of future partitions to premake (default: 4)

    Returns:
        FunctionCall for create_parent(parent_table, control, interval,
                                       partition_type, premake)

    Example:
        >>> create_parent(dialect, 'public.metrics', 'created_at', '1 day')
        >>> create_parent(dialect, 'public.logs', 'id', '10000')
        >>> create_parent(dialect, 'public.events', 'event_time', '1 hour',
        ...               partition_type='native', premake=6)
    """
    return core.FunctionCall(
        dialect, "create_parent",
        _convert_to_expression(dialect, parent_table),
        _convert_to_expression(dialect, control),
        _convert_to_expression(dialect, interval),
        _convert_to_expression(dialect, partition_type),
        _convert_to_expression(dialect, premake),
    )


def run_maintenance(
    dialect: "SQLDialectBase",
    parent_table: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Run partition maintenance to create new partitions and drop old ones.

    Executes the pg_partman maintenance procedure, which creates new child
    partitions as needed and drops old ones based on the configured retention
    policy. This should be called regularly (e.g., via cron or pg_cron).

    The actual PostgreSQL function signature is:
        run_maintenance(p_parent_table text DEFAULT NULL::text, ...)

    If parent_table is specified, only the partition set identified by that
    table will be maintained. Otherwise, all configured partition sets
    are maintained.

    Args:
        dialect: The SQL dialect instance
        parent_table: Optional parent table name to maintain. If None, all
                      configured partition sets are maintained.

    Returns:
        FunctionCall for run_maintenance([parent_table])

    Example:
        >>> run_maintenance(dialect)
        >>> run_maintenance(dialect, 'public.metrics')
    """
    args = []
    if parent_table is not None:
        args.append(_convert_to_expression(dialect, parent_table))
    return core.FunctionCall(dialect, "run_maintenance", *args)


__all__ = [
    # Partition management functions
    "create_parent",
    "run_maintenance",
]
