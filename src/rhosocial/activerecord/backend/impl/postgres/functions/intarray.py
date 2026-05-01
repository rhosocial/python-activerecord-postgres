# src/rhosocial/activerecord/backend/impl/postgres/functions/intarray.py
"""
PostgreSQL intarray Extension Functions and Operators.

This module provides SQL expression generators for PostgreSQL intarray
extension functions and operators. All functions return Expression objects
(FunctionCall, BinaryExpression) that integrate with the expression-dialect
architecture.

The intarray extension provides functions and operators for manipulating
null-free arrays of integers. It provides array-specific operators for
containment, overlap, and indexed lookup.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/intarray.html

The intarray extension must be installed:
    CREATE EXTENSION IF NOT EXISTS intarray;

Supported operators:
- @>  : Contains (array contains the elements of the right argument)
- <@  : Contained by (array is contained by the right argument)
- &&  : Overlap (arrays have any elements in common)

Supported functions:
- idx: Return index of first matching array element
- subarray: Extract a portion of an integer array
- uniq: Remove duplicate elements from an integer array
- sort: Sort an integer array

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly
"""

from typing import Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryExpression

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


# ============== intarray Operators ==============

def intarray_contains(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    values: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for the intarray contains operator (@>).

    Checks if the left array contains all elements of the right array.
    This operator can use a GiST index for efficient lookups.

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression (left side)
        values: The integer array to check containment of (right side)

    Returns:
        BinaryExpression for column @> values

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> intarray_contains(dialect, 'tags', '{1,2}')
        # Generates SQL: tags @> '{1,2}'
    """
    return BinaryExpression(
        dialect, "@>",
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, values),
    )


def intarray_contained_by(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    values: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for the intarray contained-by operator (<@).

    Checks if the left array is contained by the right array (all elements
    of the left array appear in the right array).
    This operator can use a GiST index for efficient lookups.

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression (left side)
        values: The integer array to check containment by (right side)

    Returns:
        BinaryExpression for column <@ values

    Example:
        >>> intarray_contained_by(dialect, 'tags', '{1,2,3}')
        # Generates SQL: tags <@ '{1,2,3}'
    """
    return BinaryExpression(
        dialect, "<@",
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, values),
    )


def intarray_overlaps(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    values: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for the intarray overlap operator (&&).

    Checks if the arrays have any elements in common (at least one element
    is shared between the two arrays).
    This operator can use a GiST index for efficient lookups.

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression (left side)
        values: The integer array to check overlap with (right side)

    Returns:
        BinaryExpression for column && values

    Example:
        >>> intarray_overlaps(dialect, 'tags', '{2,3}')
        # Generates SQL: tags && '{2,3}'
    """
    return BinaryExpression(
        dialect, "&&",
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, values),
    )


# ============== intarray Functions ==============

def intarray_idx(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    value: Union[int, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for the idx function.

    Returns the subscript of the first occurrence of the value in the
    array, or 0 if the value is not found. The array must be one-dimensional.

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression
        value: The integer value to search for

    Returns:
        FunctionCall for idx(column, value)

    Example:
        >>> intarray_idx(dialect, 'tags', 5)
        # Generates SQL: idx(tags, 5)
    """
    return core.FunctionCall(
        dialect, "idx",
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, value),
    )


def intarray_subarray(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    start: Union[int, str, "bases.BaseExpression"],
    length: Optional[Union[int, str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Generate SQL expression for the subarray function.

    Extracts a portion of the integer array, starting at the given
    position (1-indexed). If length is provided, extracts that many
    elements; otherwise extracts from start to the end.

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression
        start: Starting position (1-indexed)
        length: Optional number of elements to extract

    Returns:
        FunctionCall for subarray(column, start[, length])

    Example:
        >>> intarray_subarray(dialect, 'tags', 2, 3)
        # Generates SQL: subarray(tags, 2, 3)
        >>> intarray_subarray(dialect, 'tags', 2)
        # Generates SQL: subarray(tags, 2)
    """
    args = [
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, start),
    ]
    if length is not None:
        args.append(_convert_to_expression(dialect, length))
    return core.FunctionCall(dialect, "subarray", *args)


def intarray_uniq(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for the uniq function.

    Removes duplicate elements from an integer array. The array must
    be sorted for correct results; consider using intarray_sort first.

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression

    Returns:
        FunctionCall for uniq(column)

    Example:
        >>> intarray_uniq(dialect, 'tags')
        # Generates SQL: uniq(tags)
    """
    return core.FunctionCall(
        dialect, "uniq",
        _convert_to_expression(dialect, column),
    )


def intarray_sort(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for the sort function.

    Sorts the integer array in ascending order.

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression

    Returns:
        FunctionCall for sort(column)

    Example:
        >>> intarray_sort(dialect, 'tags')
        # Generates SQL: sort(tags)
    """
    return core.FunctionCall(
        dialect, "sort",
        _convert_to_expression(dialect, column),
    )


def intarray_operator(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    operator: str,
    value: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for a generic intarray operator.

    Provides a generic interface for any intarray operator. This is
    useful for operators that may not have a dedicated function wrapper.

    Common operators:
    - '@>': Contains
    - '<@': Contained by
    - '&&': Overlap

    Args:
        dialect: The SQL dialect instance
        column: The integer array column or expression (left side)
        operator: The operator string (e.g., '@>', '<@', '&&')
        value: The right-hand side value or expression

    Returns:
        BinaryExpression for column operator value

    Example:
        >>> intarray_operator(dialect, 'tags', '@>', '{1,2}')
        # Generates SQL: tags @> '{1,2}'
    """
    return BinaryExpression(
        dialect, operator,
        _convert_to_expression(dialect, column),
        _convert_to_expression(dialect, value),
    )


__all__ = [
    # intarray operators
    "intarray_contains",
    "intarray_contained_by",
    "intarray_overlaps",
    # intarray functions
    "intarray_idx",
    "intarray_subarray",
    "intarray_uniq",
    "intarray_sort",
    # generic operator
    "intarray_operator",
]
