# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_partman.py
"""
PostgreSQL pg_partman Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_partman
extension functions. All functions return Expression objects (FunctionCall,
BinaryExpression) that integrate with the expression-dialect architecture.

The pg_partman extension provides partition management for PostgreSQL. It
automates the creation and maintenance of table partitions based on time
or ID ranges.

PostgreSQL Documentation: https://github.com/pgpartman/pg_partman

The pg_partman extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_partman;

Supported functions:
- create_time_partition: Create time-based partitions
- create_id_partition: Create ID-based partitions
- run_maintenance: Run partition maintenance

Note: The format_auto_partition_config function is not included here as it
produces an UPDATE statement rather than a function call expression. It
remains in the PostgresPgPartmanMixin class.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
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


# ============== Time-Based Partitioning ==============

def create_time_partition(
    dialect: "SQLDialectBase",
    table_name: Union[str, "bases.BaseExpression"],
    partition_type: Union[str, "bases.BaseExpression"] = "daily",
    interval: Optional[Union[str, "bases.BaseExpression"]] = None,
    preload: bool = True,
) -> core.FunctionCall:
    """Create time-based partitions for a table.

    Sets up time-based partitioning for the specified table using pg_partman.
    Partitions are created automatically based on the specified time interval
    and partition type.

    Args:
        dialect: The SQL dialect instance
        table_name: The name of the parent table to partition
        partition_type: Type of partitioning interval (e.g., 'daily', 'hourly',
                        'weekly', 'monthly', 'yearly'). Default: 'daily'
        interval: Optional explicit time interval for partitioning
                  (e.g., '1 day', '1 week', '1 month'). If None, the
                  partition_type is used to determine the interval.
        preload: Whether to preload partition data (default: True)

    Returns:
        FunctionCall for partman.create_time_based_partition_set(
            table_name, partition_type[, interval], pre_make := preload)

    Example:
        >>> create_time_partition(dialect, 'metrics')
        >>> create_time_partition(dialect, 'logs', partition_type='hourly')
        >>> create_time_partition(dialect, 'events', partition_type='daily',
        ...                       interval='1 day', preload=False)
    """
    args = [
        _convert_to_expression(dialect, table_name),
        _convert_to_expression(dialect, partition_type),
    ]
    if interval is not None:
        args.append(_convert_to_expression(dialect, interval))
    args.append(_convert_to_expression(dialect, "true" if preload else "false"))
    return core.FunctionCall(dialect, "partman.create_time_based_partition_set", *args)


# ============== ID-Based Partitioning ==============

def create_id_partition(
    dialect: "SQLDialectBase",
    table_name: Union[str, "bases.BaseExpression"],
    interval: Union[int, str, "bases.BaseExpression"] = 10000,
    preload: bool = True,
) -> core.FunctionCall:
    """Create ID-based partitions for a table.

    Sets up ID-based (sequential integer) partitioning for the specified table
    using pg_partman. Partitions are created automatically based on the
    specified ID range interval.

    Args:
        dialect: The SQL dialect instance
        table_name: The name of the parent table to partition
        interval: Number of IDs per partition (default: 10000)
        preload: Whether to preload partition data (default: True)

    Returns:
        FunctionCall for partman.create_id_based_partition_set(
            table_name, p_interval := interval, pre_make := preload)

    Example:
        >>> create_id_partition(dialect, 'orders')
        >>> create_id_partition(dialect, 'records', interval=50000)
        >>> create_id_partition(dialect, 'logs', interval=100000, preload=False)
    """
    return core.FunctionCall(
        dialect, "partman.create_id_based_partition_set",
        _convert_to_expression(dialect, table_name),
        _convert_to_expression(dialect, interval),
        _convert_to_expression(dialect, "true" if preload else "false"),
    )


# ============== Maintenance ==============

def run_maintenance(
    dialect: "SQLDialectBase",
    config: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Run partition maintenance to create new partitions and drop old ones.

    Executes the pg_partman maintenance procedure, which creates new child
    partitions as needed and drops old ones based on the configured retention
    policy. This should be called regularly (e.g., via cron or pg_cron).

    If config is specified, only the partition set identified by that
    configuration will be maintained. Otherwise, all configured partition
    sets are maintained.

    Args:
        dialect: The SQL dialect instance
        config: Optional parent table name to maintain. If None, all
                configured partition sets are maintained.

    Returns:
        FunctionCall for partman.run_maintenance([config])

    Example:
        >>> run_maintenance(dialect)
        >>> run_maintenance(dialect, 'public.metrics')
    """
    args = []
    if config is not None:
        args.append(_convert_to_expression(dialect, config))
    return core.FunctionCall(dialect, "partman.run_maintenance", *args)


__all__ = [
    # Time-based partitioning
    "create_time_partition",
    # ID-based partitioning
    "create_id_partition",
    # Maintenance
    "run_maintenance",
]
