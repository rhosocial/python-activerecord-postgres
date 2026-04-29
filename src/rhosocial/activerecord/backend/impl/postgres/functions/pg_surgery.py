# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_surgery.py
"""
PostgreSQL pg_surgery Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_surgery
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pg_surgery extension provides functions to perform surgery on relation
pages. These functions are intended for recovery and repair of corrupted
data, and should be used with extreme caution.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pgsurgery.html

The pg_surgery extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_surgery;

Supported functions:
- pg_surgery_heap_freeze: Force-freeze tuples on a heap page
- pg_surgery_heap_page_header: Set heap tuple frozen status

Warning:
    These functions are potentially dangerous and should only be used by
    experienced administrators for data recovery. Improper use can cause
    data corruption.

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


# ============== Heap Surgery Functions ==============

def pg_surgery_heap_freeze(
    dialect: "SQLDialectBase",
    table_name: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Force-freeze tuples on a heap relation page.

    Marks all tuples on the specified heap relation as frozen, which prevents
    them from being visited by future VACUUM operations. This calls the
    pg_surgery.freeze_heap() function.

    Warning:
        This function is potentially dangerous. It should only be used by
        experienced administrators for data recovery purposes. Improper use
        can cause data corruption.

    Args:
        dialect: The SQL dialect instance
        table_name: The name of the table (optionally schema-qualified)

    Returns:
        FunctionCall for pg_surgery.freeze_heap(table_name)

    Example:
        >>> pg_surgery_heap_freeze(dialect, 'my_table')
        # Generates: pg_surgery.freeze_heap('my_table')
        >>> pg_surgery_heap_freeze(dialect, 'public.users')
        # Generates: pg_surgery.freeze_heap('public.users')
    """
    return core.FunctionCall(
        dialect, "pg_surgery.freeze_heap",
        _convert_to_expression(dialect, table_name),
    )


def pg_surgery_heap_page_header(
    dialect: "SQLDialectBase",
    table_name: Union[str, "bases.BaseExpression"],
    page_offset: Union[int, "bases.BaseExpression"],
    page_pid: Union[int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Set heap tuple frozen status on a heap relation page.

    Marks the specified tuple as frozen on the given heap relation page.
    This calls the pg_surgery.set_heap_tuple_frozen() function with the
    table name, page offset, and page PID.

    Warning:
        This function is potentially dangerous. It should only be used by
        experienced administrators for data recovery purposes. Improper use
        can cause data corruption.

    Args:
        dialect: The SQL dialect instance
        table_name: The name of the table (optionally schema-qualified)
        page_offset: The byte offset of the page
        page_pid: The page identifier

    Returns:
        FunctionCall for pg_surgery.set_heap_tuple_frozen(table_name, page_offset, page_pid)

    Example:
        >>> pg_surgery_heap_page_header(dialect, 'my_table', 0, 0)
        # Generates: pg_surgery.set_heap_tuple_frozen('my_table', 0, 0)
    """
    return core.FunctionCall(
        dialect, "pg_surgery.set_heap_tuple_frozen",
        _convert_to_expression(dialect, table_name),
        _convert_to_expression(dialect, page_offset),
        _convert_to_expression(dialect, page_pid),
    )


__all__ = [
    # Heap surgery functions
    "pg_surgery_heap_freeze",
    "pg_surgery_heap_page_header",
]
