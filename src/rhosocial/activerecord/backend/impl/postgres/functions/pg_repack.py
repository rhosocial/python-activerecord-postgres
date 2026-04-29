# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_repack.py
"""
PostgreSQL pg_repack Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_repack
extension functions. All functions return Expression objects (FunctionCall,
BinaryExpression) that integrate with the expression-dialect architecture.

The pg_repack extension provides functions to reorganize tables and indexes
with minimal locking. It can remove bloat from tables and indexes, and
rebuild them without requiring an exclusive lock for the entire duration
of the operation.

PostgreSQL Documentation: https://reorg.github.io/pg_repack/

The pg_repack extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_repack;

Supported functions:
- repack_table: Repack a table to remove bloat
- repack_index: Repack an index to remove bloat
- move_tablespace: Move a table to a different tablespace

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

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


# ============== Reorganization Functions ==============

def repack_table(
    dialect: "SQLDialectBase",
    table_name: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Repack a table to remove bloat and reclaim space.

    Reorganizes the specified table by creating a new copy of the table
    and swapping it with the old one. This removes bloat (dead space from
    updates and deletes) and reclaims disk space. The operation holds only
    a brief exclusive lock at the end, minimizing downtime.

    Only tables with a primary key or a NOT NULL unique index can be
    repacked.

    Args:
        dialect: The SQL dialect instance
        table_name: The name of the table to repack (optionally schema-qualified)

    Returns:
        FunctionCall for repack.repack_table(table_name)

    Example:
        >>> repack_table(dialect, 'users')
        >>> repack_table(dialect, 'public.orders')
    """
    return core.FunctionCall(
        dialect, "repack.repack_table",
        _convert_to_expression(dialect, table_name),
    )


def repack_index(
    dialect: "SQLDialectBase",
    index_name: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Repack an index to remove bloat and reclaim space.

    Rebuilds the specified index by creating a new copy and swapping it
    with the old one. This removes index bloat and improves query
    performance. The operation holds only a brief lock, minimizing
    impact on concurrent operations.

    Args:
        dialect: The SQL dialect instance
        index_name: The name of the index to repack (optionally schema-qualified)

    Returns:
        FunctionCall for repack.repack_index(index_name)

    Example:
        >>> repack_index(dialect, 'users_email_idx')
        >>> repack_index(dialect, 'public.orders_created_at_idx')
    """
    return core.FunctionCall(
        dialect, "repack.repack_index",
        _convert_to_expression(dialect, index_name),
    )


def move_tablespace(
    dialect: "SQLDialectBase",
    table_name: Union[str, "bases.BaseExpression"],
    tablespace: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Move a table to a different tablespace using pg_repack.

    Moves the specified table to a different tablespace using the pg_repack
    mechanism. This is similar to ALTER TABLE ... SET TABLESPACE but uses
    pg_repack's minimal-locking approach, making it more suitable for
    production environments where downtime must be minimized.

    Only tables with a primary key or a NOT NULL unique index can be
    moved.

    Args:
        dialect: The SQL dialect instance
        table_name: The name of the table to move (optionally schema-qualified)
        tablespace: The name of the target tablespace

    Returns:
        FunctionCall for repack.repack_move_tablespace(table_name, tablespace)

    Example:
        >>> move_tablespace(dialect, 'large_table', 'ssd_tablespace')
        >>> move_tablespace(dialect, 'public.archive_data', 'hdd_tablespace')
    """
    return core.FunctionCall(
        dialect, "repack.repack_move_tablespace",
        _convert_to_expression(dialect, table_name),
        _convert_to_expression(dialect, tablespace),
    )


__all__ = [
    # Reorganization functions
    "repack_table",
    "repack_index",
    "move_tablespace",
]
