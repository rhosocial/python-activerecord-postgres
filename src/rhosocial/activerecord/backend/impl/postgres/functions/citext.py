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
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresCitextType,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


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
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> citext_literal(dialect, 'Hello World')
        # Produces: 'Hello World'::citext
    """
    converted = value if isinstance(value, bases.BaseExpression) else core.Literal(dialect, value)
    return core.CastExpression(dialect, converted, PostgresCitextType(dialect))


def citext_column(dialect, column_name: str, length: int = None):
    """Build a column definition whose data type is CITEXT.

    PostgreSQL's citext has no length modifier; *length* is kept only for
    backwards compatibility with the old scalar helper and is rejected.
    """
    from rhosocial.activerecord.backend.expression import ColumnDefinition

    if length is not None:
        raise ValueError("CITEXT has no length modifier in PostgreSQL")
    return ColumnDefinition(dialect, column_name, dialect.parse_type("CITEXT"))


__all__ = [
    "citext_column",
    # citext functions
    "citext_literal",
]
