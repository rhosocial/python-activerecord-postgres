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
- heap_force_freeze: Force-freeze tuples on a heap relation
- heap_force_kill: Force-kill tuples on a heap relation

The actual PostgreSQL function signatures are:
- heap_force_freeze(reloid regclass, tids tid[])
- heap_force_kill(reloid regclass, tids tid[])

Note: These functions require regclass and tid[] types, which are complex
to construct with expression objects alone. The function factories accept
string representations that will be passed as literals. For more advanced
usage, callers can pass BaseExpression objects directly.

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

def heap_force_freeze(
    dialect: "SQLDialectBase",
    reloid: Union[str, "bases.BaseExpression"],
    tids: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Force-freeze tuples on a heap relation.

    Marks the specified tuples as frozen on the given heap relation.
    The reloid parameter identifies the table, and tids specifies an
    array of tuple identifiers (TID) to freeze.

    The actual PostgreSQL function signature is:
        heap_force_freeze(reloid regclass, tids tid[])

    Warning:
        This function is potentially dangerous. It should only be used by
        experienced administrators for data recovery purposes. Improper use
        can cause data corruption.

    Args:
        dialect: The SQL dialect instance
        reloid: The table name (will be cast to regclass).
                Can be a string like 'my_table' or 'public.users'.
        tids: Array of tuple identifiers in tid[] format.
              Can be a string expression like "'(0,1)'::tid" or
              a BaseExpression for complex cases.

    Returns:
        FunctionCall for heap_force_freeze(reloid, tids)

    Example:
        >>> heap_force_freeze(dialect, 'my_table', "'{(0,1)}'::tid[]")
        >>> heap_force_freeze(dialect, 'public.users', tid_array_expr)
    """
    return core.FunctionCall(
        dialect, "heap_force_freeze",
        _convert_to_expression(dialect, reloid),
        _convert_to_expression(dialect, tids),
    )


def heap_force_kill(
    dialect: "SQLDialectBase",
    reloid: Union[str, "bases.BaseExpression"],
    tids: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Force-kill tuples on a heap relation.

    Marks the specified tuples as dead on the given heap relation.
    The reloid parameter identifies the table, and tids specifies an
    array of tuple identifiers (TID) to kill.

    The actual PostgreSQL function signature is:
        heap_force_kill(reloid regclass, tids tid[])

    Warning:
        This function is potentially dangerous. It should only be used by
        experienced administrators for data recovery purposes. Improper use
        can cause data corruption.

    Args:
        dialect: The SQL dialect instance
        reloid: The table name (will be cast to regclass).
                Can be a string like 'my_table' or 'public.users'.
        tids: Array of tuple identifiers in tid[] format.
              Can be a string expression like "'(0,1)'::tid" or
              a BaseExpression for complex cases.

    Returns:
        FunctionCall for heap_force_kill(reloid, tids)

    Example:
        >>> heap_force_kill(dialect, 'my_table', "'{(0,1)}'::tid[]")
        >>> heap_force_kill(dialect, 'public.users', tid_array_expr)
    """
    return core.FunctionCall(
        dialect, "heap_force_kill",
        _convert_to_expression(dialect, reloid),
        _convert_to_expression(dialect, tids),
    )


__all__ = [
    # Heap surgery functions
    "heap_force_freeze",
    "heap_force_kill",
]
