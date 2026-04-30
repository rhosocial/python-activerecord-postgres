# src/rhosocial/activerecord/backend/impl/postgres/functions/hypopg.py
"""
PostgreSQL HypoPG Extension Functions.

This module provides SQL expression generators for PostgreSQL HypoPG
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

HypoPG provides hypothetical (virtual) indexes in PostgreSQL. These indexes
do not exist on disk and have no effect on actual query execution, but they
allow you to evaluate whether an index would be useful for a query by checking
the query plan with EXPLAIN.

PostgreSQL Documentation: https://hypopg.readthedocs.io/

The hypopg extension must be installed:
    CREATE EXTENSION IF NOT EXISTS hypopg;

Supported functions:
- hypopg_create_index: Create a hypothetical index
- hypopg_reset: Remove all hypothetical indexes
- hypopg_show_indexes: List all hypothetical indexes
- hypopg_estimate_size: Estimate the size of a hypothetical index

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


# ============== Hypothetical Index Functions ==============

def hypopg_create_index(
    dialect: "SQLDialectBase",
    index_def: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Create a hypothetical index.

    Creates a hypothetical (virtual) index from an index definition string.
    The index does not exist on disk and will not affect actual query
    execution, but it will be considered by the query planner when
    using EXPLAIN.

    The index definition should be a valid CREATE INDEX statement.
    For example: 'CREATE INDEX ON my_table USING btree (my_column)'

    Args:
        dialect: The SQL dialect instance
        index_def: The CREATE INDEX statement defining the hypothetical index

    Returns:
        FunctionCall for hypopg_create_index(index_def)

    Example:
        >>> hypopg_create_index(dialect, 'CREATE INDEX ON users USING btree (email)')
        >>> hypopg_create_index(dialect, 'CREATE INDEX ON orders USING btree (created_at)')
    """
    return core.FunctionCall(
        dialect, "hypopg_create_index",
        _convert_to_expression(dialect, index_def),
    )


def hypopg_reset(
    dialect: "SQLDialectBase",
) -> core.FunctionCall:
    """Remove all hypothetical indexes.

    Removes all hypothetical indexes that have been created in the current
    session. This is useful for cleaning up after testing different index
    configurations.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall for hypopg_reset()

    Example:
        >>> hypopg_reset(dialect)
    """
    return core.FunctionCall(dialect, "hypopg_reset")


def hypopg_show_indexes(
    dialect: "SQLDialectBase",
) -> core.FunctionCall:
    """List all hypothetical indexes.

    Returns information about all hypothetical indexes that have been created
    in the current session. The result includes the index OID, name, and
    the original index definition.

    This function calls hypopg() which is the actual PostgreSQL function
    that returns hypothetical index information as a set of records.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall for hypopg()

    Example:
        >>> hypopg_show_indexes(dialect)
    """
    return core.FunctionCall(dialect, "hypopg")


def hypopg_estimate_size(
    dialect: "SQLDialectBase",
    index_id: Union[int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Estimate the disk size of a hypothetical index.

    Returns the estimated size in bytes that the hypothetical index would
    occupy on disk if it were actually created. This is useful for evaluating
    the storage cost of a potential index.

    Uses hypopg_relation_size() which is the actual PostgreSQL function.

    Args:
        dialect: The SQL dialect instance
        index_id: The OID of the hypothetical index (from hypopg_create_index
                  or hypopg_show_indexes)

    Returns:
        FunctionCall for hypopg_relation_size(index_id)

    Example:
        >>> hypopg_estimate_size(dialect, 12345)
    """
    return core.FunctionCall(
        dialect, "hypopg_relation_size",
        _convert_to_expression(dialect, index_id),
    )


__all__ = [
    # Hypothetical index functions
    "hypopg_create_index",
    "hypopg_reset",
    "hypopg_show_indexes",
    "hypopg_estimate_size",
]
