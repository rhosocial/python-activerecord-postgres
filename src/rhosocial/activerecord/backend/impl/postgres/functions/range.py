# src/rhosocial/activerecord/backend/impl/postgres/functions/range.py
"""
PostgreSQL Range Functions and Operators.

This module provides SQL expression generators for PostgreSQL range
functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-range.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
"""

from typing import Any, List, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression
from rhosocial.activerecord.backend.impl.postgres.type_values.range import PostgresRange

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ============== Range Operators ==============

def range_contains(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
    element: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range contains element operator.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value (PostgresRange, string, or column reference)
        element: Element value to check containment

    Returns:
        BinaryExpression for range @> element

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> range_contains(dialect, 'int4range_col', 5)
    """
    return BinaryExpression(
        dialect, "@>",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value),
        element if isinstance(element, bases.BaseExpression)
        else core.Literal(dialect, element.to_postgres_string())
        if isinstance(element, PostgresRange)
        else core.Literal(dialect, element),
    )


def range_contained_by(
    dialect: "SQLDialectBase",
    element: bases.BaseExpression,
    range_value: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for element contained by range operator.

    Args:
        dialect: The SQL dialect instance
        element: Element value to check containment
        range_value: Range value (PostgresRange, string, or column reference)

    Returns:
        BinaryExpression for element <@ range

    Example:
        >>> range_contained_by(dialect, 5, 'int4range_col')
    """
    return BinaryExpression(
        dialect, "<@",
        element if isinstance(element, bases.BaseExpression)
        else core.Literal(dialect, element.to_postgres_string())
        if isinstance(element, PostgresRange)
        else core.Literal(dialect, element),
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value),
    )


def range_contains_range(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range contains range operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range (PostgresRange, string, or column reference)
        range2: Second range (PostgresRange, string, or column reference)

    Returns:
        BinaryExpression for range1 @> range2

    Example:
        >>> range_contains_range(dialect, 'col1', 'col2')
    """
    return BinaryExpression(
        dialect, "@>",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_overlaps(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range overlaps operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 && range2
    """
    return BinaryExpression(
        dialect, "&&",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_adjacent(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range adjacent operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 -|- range2
    """
    return BinaryExpression(
        dialect, "-|-",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_strictly_left_of(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range strictly left of operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 << range2
    """
    return BinaryExpression(
        dialect, "<<",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_strictly_right_of(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range strictly right of operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 >> range2
    """
    return BinaryExpression(
        dialect, ">>",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_not_extend_right(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range does not extend to the right operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 &< range2
    """
    return BinaryExpression(
        dialect, "&<",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_not_extend_left(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range does not extend to the left operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 &> range2
    """
    return BinaryExpression(
        dialect, "&>",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_union(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range union operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 + range2
    """
    return BinaryExpression(
        dialect, "+",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_intersection(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range intersection operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 * range2
    """
    return BinaryExpression(
        dialect, "*",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


def range_difference(
    dialect: "SQLDialectBase",
    range1: bases.BaseExpression,
    range2: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for range difference operator.

    Args:
        dialect: The SQL dialect instance
        range1: First range
        range2: Second range

    Returns:
        BinaryExpression for range1 - range2
    """
    return BinaryExpression(
        dialect, "-",
        range1 if isinstance(range1, bases.BaseExpression)
        else core.Literal(dialect, range1.to_postgres_string())
        if isinstance(range1, PostgresRange)
        else core.Literal(dialect, range1),
        range2 if isinstance(range2, bases.BaseExpression)
        else core.Literal(dialect, range2.to_postgres_string())
        if isinstance(range2, PostgresRange)
        else core.Literal(dialect, range2),
    )


# ============== Range Functions ==============

def range_lower(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range lower bound function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        FunctionCall for lower(range)
    """
    return core.FunctionCall(
        dialect, "lower",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value))


def range_upper(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range upper bound function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        FunctionCall for upper(range)
    """
    return core.FunctionCall(
        dialect, "upper",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value))


def range_is_empty(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range isempty function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        FunctionCall for isempty(range)
    """
    return core.FunctionCall(
        dialect, "isempty",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value))


def range_lower_inc(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range lower_inc function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        FunctionCall for lower_inc(range)
    """
    return core.FunctionCall(
        dialect, "lower_inc",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value))


def range_upper_inc(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range upper_inc function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        FunctionCall for upper_inc(range)
    """
    return core.FunctionCall(
        dialect, "upper_inc",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value))


def range_lower_inf(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range lower_inf function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        FunctionCall for lower_inf(range)
    """
    return core.FunctionCall(
        dialect, "lower_inf",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value))


def range_upper_inf(
    dialect: "SQLDialectBase",
    range_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range upper_inf function.

    Args:
        dialect: The SQL dialect instance
        range_value: Range value

    Returns:
        FunctionCall for upper_inf(range)
    """
    return core.FunctionCall(
        dialect, "upper_inf",
        range_value if isinstance(range_value, bases.BaseExpression)
        else core.Literal(dialect, range_value.to_postgres_string())
        if isinstance(range_value, PostgresRange)
        else core.Literal(dialect, range_value))


# ============== Multirange Operators ==============

def multirange_contains(
    dialect: "SQLDialectBase",
    multirange_value: bases.BaseExpression,
    element: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for multirange contains element operator.

    Args:
        dialect: The SQL dialect instance
        multirange_value: Multirange value (string, or column reference)
        element: Value or range to check containment

    Returns:
        BinaryExpression for multirange @> element

    Example:
        >>> multirange_contains(dialect, 'periods', '5')
    """
    return BinaryExpression(
        dialect, "@>",
        multirange_value if isinstance(multirange_value, bases.BaseExpression)
        else core.Literal(dialect, multirange_value.to_postgres_string())
        if isinstance(multirange_value, PostgresRange)
        else core.Literal(dialect, multirange_value),
        element if isinstance(element, bases.BaseExpression)
        else core.Literal(dialect, element.to_postgres_string())
        if isinstance(element, PostgresRange)
        else core.Literal(dialect, element),
    )


def multirange_is_contained_by(
    dialect: "SQLDialectBase",
    multirange_value: bases.BaseExpression,
    element: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for multirange is contained by operator.

    Args:
        dialect: The SQL dialect instance
        multirange_value: Multirange value (string, or column reference)
        element: Value or range to check

    Returns:
        BinaryExpression for multirange <@ element

    Example:
        >>> multirange_is_contained_by(dialect, 'periods', '[1,100)')
    """
    return BinaryExpression(
        dialect, "<@",
        multirange_value if isinstance(multirange_value, bases.BaseExpression)
        else core.Literal(dialect, multirange_value.to_postgres_string())
        if isinstance(multirange_value, PostgresRange)
        else core.Literal(dialect, multirange_value),
        element if isinstance(element, bases.BaseExpression)
        else core.Literal(dialect, element.to_postgres_string())
        if isinstance(element, PostgresRange)
        else core.Literal(dialect, element),
    )


def multirange_overlaps(
    dialect: "SQLDialectBase",
    multirange_value: bases.BaseExpression,
    other: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for multirange overlaps operator.

    Args:
        dialect: The SQL dialect instance
        multirange_value: Multirange value
        other: Other multirange/range to check overlap with

    Returns:
        BinaryExpression for multirange && other

    Example:
        >>> multirange_overlaps(dialect, 'periods', '[10,20)')
    """
    return BinaryExpression(
        dialect, "&&",
        multirange_value if isinstance(multirange_value, bases.BaseExpression)
        else core.Literal(dialect, multirange_value.to_postgres_string())
        if isinstance(multirange_value, PostgresRange)
        else core.Literal(dialect, multirange_value),
        other if isinstance(other, bases.BaseExpression)
        else core.Literal(dialect, other.to_postgres_string())
        if isinstance(other, PostgresRange)
        else core.Literal(dialect, other),
    )


def multirange_union(
    dialect: "SQLDialectBase",
    multirange_value: bases.BaseExpression,
    other: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for multirange union operator.

    Args:
        dialect: The SQL dialect instance
        multirange_value: Multirange value
        other: Other multirange/range to union with

    Returns:
        BinaryExpression for multirange + other

    Example:
        >>> multirange_union(dialect, 'periods', '[10,20)')
    """
    return BinaryExpression(
        dialect, "+",
        multirange_value if isinstance(multirange_value, bases.BaseExpression)
        else core.Literal(dialect, multirange_value.to_postgres_string())
        if isinstance(multirange_value, PostgresRange)
        else core.Literal(dialect, multirange_value),
        other if isinstance(other, bases.BaseExpression)
        else core.Literal(dialect, other.to_postgres_string())
        if isinstance(other, PostgresRange)
        else core.Literal(dialect, other),
    )


def multirange_intersection(
    dialect: "SQLDialectBase",
    multirange_value: bases.BaseExpression,
    other: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for multirange intersection operator.

    Args:
        dialect: The SQL dialect instance
        multirange_value: Multirange value
        other: Other multirange/range to intersect with

    Returns:
        BinaryExpression for multirange * other

    Example:
        >>> multirange_intersection(dialect, 'periods', '[10,20)')
    """
    return BinaryExpression(
        dialect, "*",
        multirange_value if isinstance(multirange_value, bases.BaseExpression)
        else core.Literal(dialect, multirange_value.to_postgres_string())
        if isinstance(multirange_value, PostgresRange)
        else core.Literal(dialect, multirange_value),
        other if isinstance(other, bases.BaseExpression)
        else core.Literal(dialect, other.to_postgres_string())
        if isinstance(other, PostgresRange)
        else core.Literal(dialect, other),
    )


def multirange_difference(
    dialect: "SQLDialectBase",
    multirange_value: bases.BaseExpression,
    other: bases.BaseExpression,
) -> BinaryExpression:
    """Generate SQL expression for multirange difference operator.

    Args:
        dialect: The SQL dialect instance
        multirange_value: Multirange value
        other: Other multirange/range to subtract

    Returns:
        BinaryExpression for multirange - other

    Example:
        >>> multirange_difference(dialect, 'periods', '[10,20)')
    """
    return BinaryExpression(
        dialect, "-",
        multirange_value if isinstance(multirange_value, bases.BaseExpression)
        else core.Literal(dialect, multirange_value.to_postgres_string())
        if isinstance(multirange_value, PostgresRange)
        else core.Literal(dialect, multirange_value),
        other if isinstance(other, bases.BaseExpression)
        else core.Literal(dialect, other.to_postgres_string())
        if isinstance(other, PostgresRange)
        else core.Literal(dialect, other),
    )


# ============== Multirange Functions ==============

def range_merge(
    dialect: "SQLDialectBase",
    multirange_value: bases.BaseExpression,
) -> core.FunctionCall:
    """Generate SQL expression for range_merge function.

    The range_merge function returns the smallest range that includes
    all ranges in the multirange.

    Args:
        dialect: The SQL dialect instance
        multirange_value: The multirange value

    Returns:
        FunctionCall for range_merge(multirange)

    Example:
        >>> range_merge(dialect, 'my_multirange')
    """
    return core.FunctionCall(
        dialect, "range_merge",
        multirange_value if isinstance(multirange_value, bases.BaseExpression)
        else core.Literal(dialect, multirange_value.to_postgres_string())
        if isinstance(multirange_value, PostgresRange)
        else core.Literal(dialect, multirange_value))


def multirange_literal(
    dialect: "SQLDialectBase",
    ranges: List[str],
    multirange_type: str,
) -> core.FunctionCall:
    """Construct a multirange literal expression.

    Args:
        dialect: The SQL dialect instance
        ranges: List of range literal strings (e.g., ['[1,5)', '[10,20)'])
        multirange_type: The multirange type name (e.g., 'int4multirange')

    Returns:
        FunctionCall for multirange_type(range1, range2, ...)

    Example:
        >>> multirange_literal(dialect, ['[1,5)', '[10,20)'], 'int4multirange')
    """
    args = [
        r if isinstance(r, bases.BaseExpression)
        else core.Literal(dialect, r.to_postgres_string())
        if isinstance(r, PostgresRange)
        else core.Literal(dialect, r)
        for r in ranges
    ]
    return core.FunctionCall(dialect, multirange_type, *args)


def multirange_constructor(
    dialect: "SQLDialectBase",
    multirange_type: str,
    *range_values: str,
) -> core.FunctionCall:
    """Construct a multirange constructor function call.

    Args:
        dialect: The SQL dialect instance
        multirange_type: The multirange type name
        *range_values: Range values as strings

    Returns:
        FunctionCall for multirange_type(range1, range2, ...)

    Example:
        >>> multirange_constructor(dialect, 'int4multirange', '[1,5)', '[10,20)')
    """
    args = [
        r if isinstance(r, bases.BaseExpression)
        else core.Literal(dialect, r.to_postgres_string())
        if isinstance(r, PostgresRange)
        else core.Literal(dialect, r)
        for r in range_values
    ]
    return core.FunctionCall(dialect, multirange_type, *args)


# ============== Range Type Constructors ==============

# Sentinel value to detect when bounds argument was not provided
_BOUNDS_UNSET = object()


def int4range(
    dialect: "SQLDialectBase",
    lower: Any = None,
    upper: Any = None,
    bounds: Any = _BOUNDS_UNSET,
) -> core.FunctionCall:
    """Construct an integer range.

    Args:
        dialect: The SQL dialect instance
        lower: Lower bound (integer or None for unbounded)
        upper: Upper bound (integer or None for unbounded)
        bounds: Bounds string: '[]' (inclusive), '[)' (lower inclusive), '(]' (upper inclusive), '()' (exclusive).
                If not provided, defaults to inclusive '[]'.

    Returns:
        FunctionCall for int4range(lower, upper[, bounds])

    Example:
        >>> int4range(dialect, 1, 10)
        >>> int4range(dialect, 1, 10, '[)')
    """
    args = _build_range_constructor_args(dialect, "int4range", lower, upper, bounds)
    return core.FunctionCall(dialect, "int4range", *args)


def int8range(
    dialect: "SQLDialectBase",
    lower: Any = None,
    upper: Any = None,
    bounds: Any = _BOUNDS_UNSET,
) -> core.FunctionCall:
    """Construct a big integer range.

    Args:
        dialect: The SQL dialect instance
        lower: Lower bound (bigint or None for unbounded)
        upper: Upper bound (bigint or None for unbounded)
        bounds: Bounds string: '[]' (inclusive), '[)' (lower inclusive), '(]' (upper inclusive), '()' (exclusive).
                If not provided, defaults to inclusive '[]'.

    Returns:
        FunctionCall for int8range(lower, upper[, bounds])

    Example:
        >>> int8range(dialect, 1, 1000000)
    """
    args = _build_range_constructor_args(dialect, "int8range", lower, upper, bounds)
    return core.FunctionCall(dialect, "int8range", *args)


def numrange(
    dialect: "SQLDialectBase",
    lower: Any = None,
    upper: Any = None,
    bounds: Any = _BOUNDS_UNSET,
) -> core.FunctionCall:
    """Construct a numeric range.

    Args:
        dialect: The SQL dialect instance
        lower: Lower bound (numeric or None for unbounded)
        upper: Upper bound (numeric or None for unbounded)
        bounds: Bounds string: '[]' (inclusive), '[)' (lower inclusive), '(]' (upper inclusive), '()' (exclusive).
                If not provided, defaults to inclusive '[]'.

    Returns:
        FunctionCall for numrange(lower, upper[, bounds])

    Example:
        >>> numrange(dialect, 1.5, 10.5)
    """
    args = _build_range_constructor_args(dialect, "numrange", lower, upper, bounds)
    return core.FunctionCall(dialect, "numrange", *args)


def tsrange(
    dialect: "SQLDialectBase",
    lower: Any = None,
    upper: Any = None,
    bounds: Any = _BOUNDS_UNSET,
) -> core.FunctionCall:
    """Construct a timestamp range (without time zone).

    Args:
        dialect: The SQL dialect instance
        lower: Lower bound (timestamp or None for unbounded)
        upper: Upper bound (timestamp or None for unbounded)
        bounds: Bounds string: '[]' (inclusive), '[)' (lower inclusive), '(]' (upper inclusive), '()' (exclusive).
                If not provided, defaults to inclusive '[]'.

    Returns:
        FunctionCall for tsrange(lower, upper[, bounds])

    Example:
        >>> tsrange(dialect, "'2024-01-01 00:00:00'", "'2024-12-31 23:59:59'")
    """
    args = _build_range_constructor_args(dialect, "tsrange", lower, upper, bounds)
    return core.FunctionCall(dialect, "tsrange", *args)


def tstzrange(
    dialect: "SQLDialectBase",
    lower: Any = None,
    upper: Any = None,
    bounds: Any = _BOUNDS_UNSET,
) -> core.FunctionCall:
    """Construct a timestamp range (with time zone).

    Args:
        dialect: The SQL dialect instance
        lower: Lower bound (timestamptz or None for unbounded)
        upper: Upper bound (timestamptz or None for unbounded)
        bounds: Bounds string: '[]' (inclusive), '[)' (lower inclusive), '(]' (upper inclusive), '()' (exclusive).
                If not provided, defaults to inclusive '[]'.

    Returns:
        FunctionCall for tstzrange(lower, upper[, bounds])

    Example:
        >>> tstzrange(dialect, "'2024-01-01 00:00:00+00'", "'2024-12-31 23:59:59+00'")
    """
    args = _build_range_constructor_args(dialect, "tstzrange", lower, upper, bounds)
    return core.FunctionCall(dialect, "tstzrange", *args)


def daterange(
    dialect: "SQLDialectBase",
    lower: Any = None,
    upper: Any = None,
    bounds: Any = _BOUNDS_UNSET,
) -> core.FunctionCall:
    """Construct a date range.

    Args:
        dialect: The SQL dialect instance
        lower: Lower bound (date or None for unbounded)
        upper: Upper bound (date or None for unbounded)
        bounds: Bounds string: '[]' (inclusive), '[)' (lower inclusive), '(]' (upper inclusive), '()' (exclusive).
                If not provided, defaults to inclusive '[]'.

    Returns:
        FunctionCall for daterange(lower, upper[, bounds])

    Example:
        >>> daterange(dialect, "'2024-01-01'", "'2024-12-31'")
    """
    args = _build_range_constructor_args(dialect, "daterange", lower, upper, bounds)
    return core.FunctionCall(dialect, "daterange", *args)


def _build_range_constructor_args(
    dialect: "SQLDialectBase",
    func_name: str,
    lower: Any,
    upper: Any,
    bounds: Any,
) -> list:
    """Build the argument list for a range constructor function call.

    Handles the complex argument logic for range constructors:
    - No args: range()
    - NULL values for unbounded bounds
    - Optional bounds string argument

    Args:
        dialect: The SQL dialect instance
        func_name: The range function name (for context)
        lower: Lower bound value (None for unbounded)
        upper: Upper bound value (None for unbounded)
        bounds: Bounds string or _BOUNDS_UNSET sentinel

    Returns:
        List of BaseExpression arguments for the FunctionCall
    """
    if lower is None and upper is None:
        return []

    args = []

    # Lower bound: NULL if None, otherwise convert to expression
    if lower is None:
        args.append(core.Literal(dialect, "NULL"))
    else:
        args.append(lower if isinstance(lower, bases.BaseExpression)
        else core.Literal(dialect, lower.to_postgres_string())
        if isinstance(lower, PostgresRange)
        else core.Literal(dialect, lower))

    # Upper bound: NULL if None, otherwise convert to expression
    if upper is None:
        args.append(core.Literal(dialect, "NULL"))
    else:
        args.append(upper if isinstance(upper, bases.BaseExpression)
        else core.Literal(dialect, upper.to_postgres_string())
        if isinstance(upper, PostgresRange)
        else core.Literal(dialect, upper))

    # Optional bounds argument
    if bounds is not _BOUNDS_UNSET:
        args.append(core.Literal(dialect, bounds))

    return args


__all__ = [
    # Range operators
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
    # Range functions
    "range_lower",
    "range_upper",
    "range_is_empty",
    "range_lower_inc",
    "range_upper_inc",
    "range_lower_inf",
    "range_upper_inf",
    # Multirange operators
    "multirange_contains",
    "multirange_is_contained_by",
    "multirange_overlaps",
    "multirange_union",
    "multirange_intersection",
    "multirange_difference",
    # Multirange functions
    "range_merge",
    "multirange_literal",
    "multirange_constructor",
    # Range type constructors
    "int4range",
    "int8range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
]
