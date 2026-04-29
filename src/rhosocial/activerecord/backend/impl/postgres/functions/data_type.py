# src/rhosocial/activerecord/backend/impl/postgres/functions/data_type.py
"""
PostgreSQL data type literal construction functions.

This module provides SQL expression generators for constructing
PostgreSQL-specific data type literals, including xid8 and array types.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (Literal with type cast, etc.)
- They do not concatenate SQL strings directly
"""

from typing import Any, List, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def xid8_literal(
    dialect: "SQLDialectBase",
    value: int,
) -> "bases.BaseExpression":
    """Create an xid8 (64-bit transaction ID) literal expression.

    Wraps an integer value as a Literal expression with an xid8 type cast,
    producing SQL like ``value::xid8``.

    Args:
        dialect: The SQL dialect instance
        value: The transaction ID value

    Returns:
        BaseExpression with ::xid8 type cast applied

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> xid8_literal(dialect, 123456789)
        # Produces: 123456789::xid8
    """
    return core.Literal(dialect, value).cast("xid8")


def array_literal(
    dialect: "SQLDialectBase",
    elements: List[Any],
    element_type: Optional[str] = None,
) -> "bases.BaseExpression":
    """Construct an array literal expression.

    Uses PostgreSQL array literal format ``'{elem1,elem2}'`` with optional
    type cast. This produces parameterized queries.

    For string elements, values are double-quoted inside the braces as per
    PostgreSQL array literal syntax. NULL elements are represented as the
    unquoted keyword NULL.

    Args:
        dialect: The SQL dialect instance
        elements: List of array elements
        element_type: Optional element type cast (e.g., 'int', 'text')

    Returns:
        BaseExpression representing the array literal

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> array_literal(dialect, [1, 2, 3], 'int')
        # Produces parameterized: '{1,2,3}'::int[]
        >>> array_literal(dialect, ['a', 'b'], 'text')
        # Produces parameterized: '{"a","b"}'::text[]
    """
    if not elements:
        if element_type:
            return core.Literal(dialect, "{}").cast(f"{element_type}[]")
        return core.Literal(dialect, "{}")

    formatted = []
    for e in elements:
        if isinstance(e, str):
            formatted.append(f'"{e}"')
        elif e is None:
            formatted.append("NULL")
        else:
            formatted.append(str(e))

    array_str = "{" + ",".join(formatted) + "}"
    result = core.Literal(dialect, array_str)
    if element_type:
        return result.cast(f"{element_type}[]")
    return result


__all__ = [
    "xid8_literal",
    "array_literal",
]
