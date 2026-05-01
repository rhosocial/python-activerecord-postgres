# src/rhosocial/activerecord/backend/impl/postgres/functions/citext.py
"""
PostgreSQL citext Extension Functions.

This module provides SQL expression generators for PostgreSQL citext
extension functions. All functions return Expression objects (Literal,
BaseExpression) that integrate with the expression-dialect architecture.

The citext extension provides a case-insensitive character string type.
Values of type citext are compared using standard PostgreSQL comparison
rules, except that the comparison is case-insensitive.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/citext.html

The citext extension must be installed:
    CREATE EXTENSION IF NOT EXISTS citext;

Supported functions:
- citext_literal: Create a citext literal value with type cast

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (Literal, BaseExpression, etc.)
- They do not concatenate SQL strings directly
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


# ============== citext Functions ==============

def citext_literal(
    dialect: "SQLDialectBase",
    value: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Create a citext literal expression with type cast.

    Wraps a string value as a Literal expression with a citext type cast,
    producing SQL like 'value'::citext. The resulting expression can be
    used directly in WHERE clauses against citext columns to ensure
    case-insensitive comparison behavior.

    Args:
        dialect: The SQL dialect instance
        value: The string value to wrap as a citext literal

    Returns:
        BaseExpression with ::citext type cast applied

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> citext_literal(dialect, 'Hello World')
        # Produces: 'Hello World'::citext
    """
    converted = _convert_to_expression(dialect, value)
    return converted.cast("citext")


__all__ = [
    # citext functions
    "citext_literal",
]
