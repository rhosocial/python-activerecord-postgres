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
- pg_get_wal_records_info: Get information about WAL records
- pg_get_wal_blocks_info: Get information about WAL block references
- pg_logical_emit_message: Emit a logical decoding message

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
    start_lsn: Optional[Union[str, "bases.BaseExpression"]] = None,
    end_lsn: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Get information about WAL records in a LSN range.

    Returns information about WAL records between the specified start and
    end LSN (Log Sequence Number) positions. If start_lsn is not specified,
    it defaults to the first LSN of the current WAL file. If end_lsn is
    not specified, it defaults to the current insert LSN.

    Args:
        dialect: The SQL dialect instance
        start_lsn: Start LSN position (e.g., '0/16E9130'), or None for default
        end_lsn: End LSN position (e.g., '0/16E9130'), or None for default

    Returns:
        FunctionCall for pg_get_wal_records_info(start_lsn, end_lsn)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> pg_get_wal_records_info(dialect)
        # Generates: pg_get_wal_records_info()
        >>> pg_get_wal_records_info(dialect, start_lsn='0/16E9130')
        # Generates: pg_get_wal_records_info('0/16E9130')
        >>> pg_get_wal_records_info(dialect, start_lsn='0/16E9130', end_lsn='0/16E9200')
        # Generates: pg_get_wal_records_info('0/16E9130', '0/16E9200')
    """
    args = []
    if start_lsn is not None:
        args.append(_convert_to_expression(dialect, start_lsn))
    if end_lsn is not None:
        args.append(_convert_to_expression(dialect, end_lsn))
    return core.FunctionCall(dialect, "pg_get_wal_records_info", *args)


def pg_get_wal_blocks_info(
    dialect: "SQLDialectBase",
    start_lsn: Optional[Union[str, "bases.BaseExpression"]] = None,
    end_lsn: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Get information about WAL block references in a LSN range.

    Returns information about block references in WAL records between the
    specified start and end LSN positions. This shows which data blocks
    (tables, indexes) are affected by WAL records.

    If start_lsn is not specified, it defaults to the first LSN of the
    current WAL file. If end_lsn is not specified, it defaults to the
    current insert LSN.

    Args:
        dialect: The SQL dialect instance
        start_lsn: Start LSN position (e.g., '0/16E9130'), or None for default
        end_lsn: End LSN position (e.g., '0/16E9130'), or None for default

    Returns:
        FunctionCall for pg_get_wal_blocks_info(start_lsn, end_lsn)

    Example:
        >>> pg_get_wal_blocks_info(dialect)
        # Generates: pg_get_wal_blocks_info()
        >>> pg_get_wal_blocks_info(dialect, start_lsn='0/16E9130')
        # Generates: pg_get_wal_blocks_info('0/16E9130')
        >>> pg_get_wal_blocks_info(dialect, start_lsn='0/16E9130', end_lsn='0/16E9200')
        # Generates: pg_get_wal_blocks_info('0/16E9130', '0/16E9200')
    """
    args = []
    if start_lsn is not None:
        args.append(_convert_to_expression(dialect, start_lsn))
    if end_lsn is not None:
        args.append(_convert_to_expression(dialect, end_lsn))
    return core.FunctionCall(dialect, "pg_get_wal_blocks_info", *args)


# ============== Logical Decoding Functions ==============

def pg_logical_emit_message(
    dialect: "SQLDialectBase",
    transactional: bool = False,
    prefix: Union[str, "bases.BaseExpression"] = "test",
    message: Union[str, "bases.BaseExpression"] = "",
) -> core.FunctionCall:
    """Emit a logical decoding message.

    Emits a logical decoding message that will be included in the WAL stream.
    This is useful for applications that use logical decoding to receive
    custom messages along with data changes.

    The transactional parameter determines whether the message is emitted
    as part of the current transaction (true) or immediately (false). The
    prefix is used to identify the message type, and the message is the
    content to be emitted. Both are included in the WAL and can be consumed
    by logical decoding plugins.

    Args:
        dialect: The SQL dialect instance
        transactional: Whether the message is transactional (default: False).
                       If True, the message is emitted only if the enclosing
                       transaction commits; if False, it is emitted immediately.
        prefix: The prefix string for the logical message (default: 'test').
                Identifies the message type for consumers.
        message: The message content to emit (default: '')

    Returns:
        FunctionCall for pg_logical_emit_message(transactional, prefix, message)

    Example:
        >>> pg_logical_emit_message(dialect)
        # Generates: pg_logical_emit_message(False, 'test', '')
        >>> pg_logical_emit_message(dialect, transactional=True, prefix='myapp', message='user login event')
        # Generates: pg_logical_emit_message(True, 'myapp', 'user login event')
        >>> pg_logical_emit_message(dialect, prefix='audit', message='critical action performed')
        # Generates: pg_logical_emit_message(False, 'audit', 'critical action performed')
    """
    return core.FunctionCall(
        dialect, "pg_logical_emit_message",
        _convert_to_expression(dialect, transactional),
        _convert_to_expression(dialect, prefix),
        _convert_to_expression(dialect, message),
    )


__all__ = [
    # WAL inspection functions
    "pg_get_wal_records_info",
    "pg_get_wal_blocks_info",
    # Logical decoding functions
    "pg_logical_emit_message",
]
