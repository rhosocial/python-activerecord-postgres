# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_walinspect.py
"""
PostgreSQL pg_walinspect Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_walinspect
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pg_walinspect extension provides functions to inspect the contents of
Write-Ahead Log (WAL) records. This is useful for debugging, auditing, and
understanding the WAL activity of a PostgreSQL server.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pgwalinspect.html

The pg_walinspect extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_walinspect;

Supported functions:
- pg_get_wal_records_info: Get information about WAL records in an LSN range
- pg_get_wal_blocks_info: Get information about WAL block references

The actual PostgreSQL function signatures are:
- pg_get_wal_records_info(start_lsn pg_lsn, end_lsn pg_lsn)
- pg_get_wal_blocks_info(start_lsn pg_lsn, end_lsn pg_lsn)

Note: pg_logical_emit_message is a built-in PostgreSQL function (not part
of pg_walinspect). It has been moved out of this module as it does not
belong to the pg_walinspect extension.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, etc.)
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, int, bool, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings, integers, booleans, and existing BaseExpression objects.

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


# ============== WAL Inspection Functions ==============

def pg_get_wal_records_info(
    dialect: "SQLDialectBase",
    start_lsn: Union[str, "bases.BaseExpression"],
    end_lsn: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get information about WAL records in a LSN range.

    Returns information about WAL records between the specified start and
    end LSN (Log Sequence Number) positions.

    The actual PostgreSQL function signature is:
        pg_get_wal_records_info(start_lsn pg_lsn, end_lsn pg_lsn)

    Both start_lsn and end_lsn are required parameters. They accept LSN
    string format like '0/16E9130'.

    Args:
        dialect: The SQL dialect instance
        start_lsn: Start LSN position (e.g., '0/16E9130')
        end_lsn: End LSN position (e.g., '0/16E9200')

    Returns:
        FunctionCall for pg_get_wal_records_info(start_lsn, end_lsn)

    Example:
        >>> pg_get_wal_records_info(dialect, '0/16E9130', '0/16E9200')
    """
    return core.FunctionCall(
        dialect, "pg_get_wal_records_info",
        _convert_to_expression(dialect, start_lsn),
        _convert_to_expression(dialect, end_lsn),
    )


def pg_get_wal_blocks_info(
    dialect: "SQLDialectBase",
    start_lsn: Union[str, "bases.BaseExpression"],
    end_lsn: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Get information about WAL block references in a LSN range.

    Returns information about block references in WAL records between the
    specified start and end LSN positions. This shows which data blocks
    (tables, indexes) are affected by WAL records.

    The actual PostgreSQL function signature is:
        pg_get_wal_blocks_info(start_lsn pg_lsn, end_lsn pg_lsn)

    Both start_lsn and end_lsn are required parameters.

    Args:
        dialect: The SQL dialect instance
        start_lsn: Start LSN position (e.g., '0/16E9130')
        end_lsn: End LSN position (e.g., '0/16E9200')

    Returns:
        FunctionCall for pg_get_wal_blocks_info(start_lsn, end_lsn)

    Example:
        >>> pg_get_wal_blocks_info(dialect, '0/16E9130', '0/16E9200')
    """
    return core.FunctionCall(
        dialect, "pg_get_wal_blocks_info",
        _convert_to_expression(dialect, start_lsn),
        _convert_to_expression(dialect, end_lsn),
    )


__all__ = [
    # WAL inspection functions
    "pg_get_wal_records_info",
    "pg_get_wal_blocks_info",
]
