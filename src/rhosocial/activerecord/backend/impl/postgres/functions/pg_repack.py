# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_repack.py
"""
PostgreSQL pg_repack Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_repack
extension. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pg_repack extension provides functions to reorganize tables and indexes
with minimal locking. It can remove bloat from tables and indexes, and
rebuild them without requiring an exclusive lock for the entire duration
of the operation.

PostgreSQL Documentation: https://reorg.github.io/pg_repack/

The pg_repack extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_repack;

Note: pg_repack is primarily a command-line tool. The SQL functions in the
"repack" schema (repack_apply, repack_swap, etc.) are internal functions
used by the pg_repack client tool and are not intended to be called directly
by users. There is no public SQL API like "repack_table()" or
"repack_index()".

To repack a table or index, use the pg_repack command-line utility:
    pg_repack --table=tablename dbname
    pg_repack --index=indexname dbname

This module provides a convenience function to check if the pg_repack
extension is installed, which can be used in integration tests.

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


# ============== Extension Status Functions ==============

def repack_version(
    dialect: "SQLDialectBase",
) -> core.FunctionCall:
    """Get the pg_repack extension version.

    Returns the version string of the installed pg_repack extension.
    This is useful for verifying the extension is installed and
    determining the available features.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall for repack.repack_version()

    Example:
        >>> repack_version(dialect)
        # Generates: repack.repack_version()
    """
    return core.FunctionCall(dialect, "repack.repack_version")


__all__ = [
    # Extension status functions
    "repack_version",
]
