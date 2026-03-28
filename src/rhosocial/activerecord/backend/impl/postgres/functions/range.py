# src/rhosocial/activerecord/backend/impl/postgres/functions/range.py
"""
PostgreSQL Range Functions and Operators.

This module provides SQL expression generators for PostgreSQL range
functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-range.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return SQL expression strings
"""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def range_contains(dialect: "SQLDialectBase", range_value: Any, element: Any) -> str:
    """Generate SQL expression for range contains element operator.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value (PostgresRange, string, or column reference)
        element: Element value to check containment

    Returns:
        SQL expression: range @> element

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> range_contains(dialect, 'int4range_col', 5)
        'int4range_col @> 5'
    """
    from ..types.range import PostgresRange

    if isinstance(range_value, PostgresRange):
        range_str = range_value.to_postgres_string()
    else:
        range_str = str(range_value)

    if isinstance(element, PostgresRange):
        element_str = element.to_postgres_string()
    else:
        element_str = str(element)

    return f"{range_str} @> {element_str}"


def range_contained_by(dialect: "SQLDialectBase", element: Any, range_value: Any) -> str:
    """Generate SQL expression for element contained by range operator.

    Args:
        dialect: The SQL dialect instance
        element: Element value to check containment
        range_value: Range value (PostgresRange, string, or column reference)

    Returns:
        SQL expression: element <@ range

    Example:
        >>> range_contained_by(dialect, 5, 'int4range_col')
        '5 <@ int4range_col'
    """
    from ..types.range import PostgresRange

    if isinstance(range_value, PostgresRange):
        range_str = range_value.to_postgres_string()
    else:
        range_str = str(range_value)

    if isinstance(element, PostgresRange):
        element_str = element.to_postgres_string()
    else:
        element_str = str(element)

    return f"{element_str} <@ {range_str}"


def range_contains_range(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range contains range operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range (PostgresRange, string, or column reference)
        range2: Second range (PostgresRange, string, or column reference)

    Returns:
        SQL expression: range1 @> range2

    Example:
        >>> range_contains_range(dialect, 'col1', 'col2')
        'col1 @> col2'
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} @> {r2_str}"


def range_overlaps(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range overlaps operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 && range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} && {r2_str}"


def range_adjacent(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range adjacent operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 -|- range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} -|- {r2_str}"


def range_strictly_left_of(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range strictly left of operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 << range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} << {r2_str}"


def range_strictly_right_of(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range strictly right of operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 >> range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} >> {r2_str}"


def range_not_extend_right(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range does not extend to the right operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 &< range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} &< {r2_str}"


def range_not_extend_left(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range does not extend to the left operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 &> range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} &> {r2_str}"


def range_union(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range union operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 + range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} + {r2_str}"


def range_intersection(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range intersection operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 * range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} * {r2_str}"


def range_difference(dialect: "SQLDialectBase", range1: Any, range2: Any) -> str:
    """Generate SQL expression for range difference operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        SQL expression: range1 - range2
    """
    from ..types.range import PostgresRange

    r1_str = range1.to_postgres_string() if isinstance(range1, PostgresRange) else str(range1)
    r2_str = range2.to_postgres_string() if isinstance(range2, PostgresRange) else str(range2)
    return f"{r1_str} - {r2_str}"


def range_lower(dialect: "SQLDialectBase", range_value: Any) -> str:
    """Generate SQL expression for range lower bound function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        SQL expression: lower(range)
    """
    from ..types.range import PostgresRange

    r_str = range_value.to_postgres_string() if isinstance(range_value, PostgresRange) else str(range_value)
    return f"lower({r_str})"


def range_upper(dialect: "SQLDialectBase", range_value: Any) -> str:
    """Generate SQL expression for range upper bound function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        SQL expression: upper(range)
    """
    from ..types.range import PostgresRange

    r_str = range_value.to_postgres_string() if isinstance(range_value, PostgresRange) else str(range_value)
    return f"upper({r_str})"


def range_is_empty(dialect: "SQLDialectBase", range_value: Any) -> str:
    """Generate SQL expression for range isempty function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        SQL expression: isempty(range)
    """
    from ..types.range import PostgresRange

    r_str = range_value.to_postgres_string() if isinstance(range_value, PostgresRange) else str(range_value)
    return f"isempty({r_str})"


def range_lower_inc(dialect: "SQLDialectBase", range_value: Any) -> str:
    """Generate SQL expression for range lower_inc function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        SQL expression: lower_inc(range)
    """
    from ..types.range import PostgresRange

    r_str = range_value.to_postgres_string() if isinstance(range_value, PostgresRange) else str(range_value)
    return f"lower_inc({r_str})"


def range_upper_inc(dialect: "SQLDialectBase", range_value: Any) -> str:
    """Generate SQL expression for range upper_inc function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        SQL expression: upper_inc(range)
    """
    from ..types.range import PostgresRange

    r_str = range_value.to_postgres_string() if isinstance(range_value, PostgresRange) else str(range_value)
    return f"upper_inc({r_str})"


def range_lower_inf(dialect: "SQLDialectBase", range_value: Any) -> str:
    """Generate SQL expression for range lower_inf function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        SQL expression: lower_inf(range)
    """
    from ..types.range import PostgresRange

    r_str = range_value.to_postgres_string() if isinstance(range_value, PostgresRange) else str(range_value)
    return f"lower_inf({r_str})"


def range_upper_inf(dialect: "SQLDialectBase", range_value: Any) -> str:
    """Generate SQL expression for range upper_inf function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        SQL expression: upper_inf(range)
    """
    from ..types.range import PostgresRange

    r_str = range_value.to_postgres_string() if isinstance(range_value, PostgresRange) else str(range_value)
    return f"upper_inf({r_str})"


__all__ = [
    "range_contains",
    "range_contained_by",
    "range_contains_range",
    "range_overlaps",
    "range_adjacent",
    "range_strictly_left_of",
    "range_strictly_right_of",
    "range_not_extend_right",
    "range_not_extend_left",
    "range_union",
    "range_intersection",
    "range_difference",
    "range_lower",
    "range_upper",
    "range_is_empty",
    "range_lower_inc",
    "range_upper_inc",
    "range_lower_inf",
    "range_upper_inf",
]
