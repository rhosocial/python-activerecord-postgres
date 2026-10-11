# src/rhosocial/activerecord/backend/impl/postgres/functions/array.py
"""
PostgreSQL Array Function Factories.

This module provides SQL expression generators for PostgreSQL array
functions and operators. All functions return FunctionCall expression
objects that integrate with the expression-dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-array.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return core.FunctionCall expression objects (not raw SQL strings)
"""

from typing import Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def array_agg(
    dialect: "SQLDialectBase",
    expression: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Aggregate function that collects values into an array.

    Args:
        dialect: The SQL dialect instance
        expression: The expression to aggregate

    Returns:
        FunctionCall: SQL expression for array_agg(expression)

    Example:
        >>> func = array_agg(dialect, "value")
        >>> func.to_sql()
        ('array_agg(%s)', ('value',))
    """
    expr = expression if isinstance(expression, bases.BaseExpression) else core.Literal(dialect, expression)
    return core.FunctionCall(dialect, "array_agg", expr)


def array_agg_distinct(
    dialect: "SQLDialectBase",
    expression: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Aggregate function that collects distinct values into an array.

    Uses the is_distinct parameter of FunctionCall to produce
    array_agg(DISTINCT expression).

    Args:
        dialect: The SQL dialect instance
        expression: The expression to aggregate

    Returns:
        FunctionCall: SQL expression for array_agg(DISTINCT expression)

    Example:
        >>> func = array_agg_distinct(dialect, "value")
        >>> func.to_sql()
        ('array_agg(DISTINCT %s)', ('value',))
    """
    expr = expression if isinstance(expression, bases.BaseExpression) else core.Literal(dialect, expression)
    return core.FunctionCall(dialect, "array_agg", expr, is_distinct=True)


def array_append(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    element: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Append an element to the end of an array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to append

    Returns:
        FunctionCall: SQL expression for array_append(array, element)

    Example:
        >>> func = array_append(dialect, "arr", 3)
        >>> func.to_sql()
        ('array_append(%s, %s)', ('arr', 3))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    elem_expr = element if isinstance(element, bases.BaseExpression) else core.Literal(dialect, element)
    return core.FunctionCall(dialect, "array_append", arr_expr, elem_expr)


def array_cat(
    dialect: "SQLDialectBase",
    array1: bases.BaseExpression,
    array2: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Concatenate two arrays.

    Args:
        dialect: The SQL dialect instance
        array1: First array
        array2: Second array

    Returns:
        FunctionCall: SQL expression for array_cat(array1, array2)

    Example:
        >>> func = array_cat(dialect, "arr1", "arr2")
        >>> func.to_sql()
        ('array_cat(%s, %s)', ('arr1', 'arr2'))
    """
    arr1_expr = array1 if isinstance(array1, bases.BaseExpression) else core.Literal(dialect, array1)
    arr2_expr = array2 if isinstance(array2, bases.BaseExpression) else core.Literal(dialect, array2)
    return core.FunctionCall(dialect, "array_cat", arr1_expr, arr2_expr)


def array_dims(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Return the dimensions of an array as text.

    Args:
        dialect: The SQL dialect instance
        array: The array expression

    Returns:
        FunctionCall: SQL expression for array_dims(array)

    Example:
        >>> func = array_dims(dialect, "arr")
        >>> func.to_sql()
        ('array_dims(%s)', ('arr',))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    return core.FunctionCall(dialect, "array_dims", arr_expr)


def array_fill(
    dialect: "SQLDialectBase",
    element: bases.BaseExpression,
    dimensions: int,
    bounds: Optional[Union[int, str]] = None,
) -> core.FunctionCall:
    """
    Return an array filled with a value for specified dimensions.

    Args:
        dialect: The SQL dialect instance
        element: The value to fill the array with
        dimensions: The array dimensions
        bounds: Optional lower bound for each dimension

    Returns:
        FunctionCall: SQL expression for array_fill(element, dimensions[, bounds])

    Example:
        >>> func = array_fill(dialect, 0, "dims")
        >>> func.to_sql()
        ('array_fill(%s, %s)', (0, 'dims'))
    """
    elem_expr = element if isinstance(element, bases.BaseExpression) else core.Literal(dialect, element)
    dims_expr = dimensions if isinstance(dimensions, bases.BaseExpression) else core.Literal(dialect, dimensions)
    args = [elem_expr, dims_expr]
    if bounds is not None:
        bounds_expr = bounds if isinstance(bounds, bases.BaseExpression) else core.Literal(dialect, bounds)
        args.append(bounds_expr)
    return core.FunctionCall(dialect, "array_fill", *args)


def array_length(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    dimension: int = 1,
) -> core.FunctionCall:
    """
    Return the length of an array for the specified dimension.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        dimension: The dimension (1-indexed, default 1)

    Returns:
        FunctionCall: SQL expression for array_length(array, dimension)

    Example:
        >>> func = array_length(dialect, "arr", 1)
        >>> func.to_sql()
        ('array_length(%s, %s)', ('arr', 1))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    dim_expr = dimension if isinstance(dimension, bases.BaseExpression) else core.Literal(dialect, dimension)
    return core.FunctionCall(dialect, "array_length", arr_expr, dim_expr)


def array_lower(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    dimension: int = 1,
) -> core.FunctionCall:
    """
    Return the lower bound of an array's dimension.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        dimension: The dimension (1-indexed, default 1)

    Returns:
        FunctionCall: SQL expression for array_lower(array, dimension)

    Example:
        >>> func = array_lower(dialect, "arr", 1)
        >>> func.to_sql()
        ('array_lower(%s, %s)', ('arr', 1))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    dim_expr = dimension if isinstance(dimension, bases.BaseExpression) else core.Literal(dialect, dimension)
    return core.FunctionCall(dialect, "array_lower", arr_expr, dim_expr)


def array_ndims(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Return the number of dimensions of an array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression

    Returns:
        FunctionCall: SQL expression for array_ndims(array)

    Example:
        >>> func = array_ndims(dialect, "arr")
        >>> func.to_sql()
        ('array_ndims(%s)', ('arr',))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    return core.FunctionCall(dialect, "array_ndims", arr_expr)


def array_position(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    element: bases.BaseExpression,
    start: Optional[int] = None,
) -> core.FunctionCall:
    """
    Return the position of the first occurrence of element in array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to find
        start: Optional starting position for the search

    Returns:
        FunctionCall: SQL expression for array_position(array, element[, start])

    Example:
        >>> func = array_position(dialect, "arr", "elem")
        >>> func.to_sql()
        ('array_position(%s, %s)', ('arr', 'elem'))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    elem_expr = element if isinstance(element, bases.BaseExpression) else core.Literal(dialect, element)
    args = [arr_expr, elem_expr]
    if start is not None:
        start_expr = start if isinstance(start, bases.BaseExpression) else core.Literal(dialect, start)
        args.append(start_expr)
    return core.FunctionCall(dialect, "array_position", *args)


def array_positions(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    element: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Return an array of positions of element in array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to find

    Returns:
        FunctionCall: SQL expression for array_positions(array, element)

    Example:
        >>> func = array_positions(dialect, "arr", "elem")
        >>> func.to_sql()
        ('array_positions(%s, %s)', ('arr', 'elem'))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    elem_expr = element if isinstance(element, bases.BaseExpression) else core.Literal(dialect, element)
    return core.FunctionCall(dialect, "array_positions", arr_expr, elem_expr)


def array_prepend(
    dialect: "SQLDialectBase",
    element: bases.BaseExpression,
    array: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Prepend an element to the beginning of an array.

    Args:
        dialect: The SQL dialect instance
        element: The element to prepend
        array: The array expression

    Returns:
        FunctionCall: SQL expression for array_prepend(element, array)

    Example:
        >>> func = array_prepend(dialect, 1, "arr")
        >>> func.to_sql()
        ('array_prepend(%s, %s)', (1, 'arr'))
    """
    elem_expr = element if isinstance(element, bases.BaseExpression) else core.Literal(dialect, element)
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    return core.FunctionCall(dialect, "array_prepend", elem_expr, arr_expr)


def array_remove(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    element: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Remove all elements equal to the specified value from array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to remove

    Returns:
        FunctionCall: SQL expression for array_remove(array, element)

    Example:
        >>> func = array_remove(dialect, "arr", 2)
        >>> func.to_sql()
        ('array_remove(%s, %s)', ('arr', 2))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    elem_expr = element if isinstance(element, bases.BaseExpression) else core.Literal(dialect, element)
    return core.FunctionCall(dialect, "array_remove", arr_expr, elem_expr)


def array_replace(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    from_elem: bases.BaseExpression,
    to_elem: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Replace all occurrences of from_elem with to_elem in array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        from_elem: The value to search for
        to_elem: The value to replace with

    Returns:
        FunctionCall: SQL expression for array_replace(array, from_elem, to_elem)

    Example:
        >>> func = array_replace(dialect, "arr", 2, 4)
        >>> func.to_sql()
        ('array_replace(%s, %s, %s)', ('arr', 2, 4))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    from_expr = from_elem if isinstance(from_elem, bases.BaseExpression) else core.Literal(dialect, from_elem)
    to_expr = to_elem if isinstance(to_elem, bases.BaseExpression) else core.Literal(dialect, to_elem)
    return core.FunctionCall(dialect, "array_replace", arr_expr, from_expr, to_expr)


def array_to_string(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    delimiter: bases.BaseExpression,
    null_text: Optional[str] = None,
) -> core.FunctionCall:
    """
    Convert array to string using delimiter.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        delimiter: The delimiter string
        null_text: Optional text to represent NULL values

    Returns:
        FunctionCall: SQL expression for array_to_string(array, delimiter[, null_text])

    Example:
        >>> func = array_to_string(dialect, "arr", ",")
        >>> func.to_sql()
        ('array_to_string(%s, %s)', ('arr', ','))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    delim_expr = delimiter if isinstance(delimiter, bases.BaseExpression) else core.Literal(dialect, delimiter)
    args = [arr_expr, delim_expr]
    if null_text is not None:
        null_expr = null_text if isinstance(null_text, bases.BaseExpression) else core.Literal(dialect, null_text)
        args.append(null_expr)
    return core.FunctionCall(dialect, "array_to_string", *args)


def array_upper(
    dialect: "SQLDialectBase",
    array: bases.BaseExpression,
    dimension: int = 1,
) -> core.FunctionCall:
    """
    Return the upper bound of an array's dimension.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        dimension: The dimension (1-indexed, default 1)

    Returns:
        FunctionCall: SQL expression for array_upper(array, dimension)

    Example:
        >>> func = array_upper(dialect, "arr", 1)
        >>> func.to_sql()
        ('array_upper(%s, %s)', ('arr', 1))
    """
    arr_expr = array if isinstance(array, bases.BaseExpression) else core.Literal(dialect, array)
    dim_expr = dimension if isinstance(dimension, bases.BaseExpression) else core.Literal(dialect, dimension)
    return core.FunctionCall(dialect, "array_upper", arr_expr, dim_expr)


def unnest(
    dialect: "SQLDialectBase",
    *arrays: bases.BaseExpression,
) -> core.FunctionCall:
    """
    Expand an array into a set of rows.

    Args:
        dialect: The SQL dialect instance
        *arrays: One or more array expressions

    Returns:
        FunctionCall: SQL expression for unnest(array1, array2, ...)

    Example:
        >>> func = unnest(dialect, "arr1", "arr2")
        >>> func.to_sql()
        ('unnest(%s, %s)', ('arr1', 'arr2'))
    """
    args = [
        a if isinstance(a, bases.BaseExpression) else core.Literal(dialect, a)
        for a in arrays
    ]
    return core.FunctionCall(dialect, "unnest", *args)


def string_to_array(
    dialect: "SQLDialectBase",
    string: bases.BaseExpression,
    delimiter: bases.BaseExpression,
    null_text: Optional[str] = None,
) -> core.FunctionCall:
    """
    Split string into array using delimiter.

    Args:
        dialect: The SQL dialect instance
        string: The input string
        delimiter: The delimiter string
        null_text: If specified, occurrences of this string are replaced by NULL

    Returns:
        FunctionCall: SQL expression for string_to_array(string, delimiter[, null_text])

    Example:
        >>> func = string_to_array(dialect, "str", ",")
        >>> func.to_sql()
        ('string_to_array(%s, %s)', ('str', ','))
    """
    str_expr = string if isinstance(string, bases.BaseExpression) else core.Literal(dialect, string)
    delim_expr = delimiter if isinstance(delimiter, bases.BaseExpression) else core.Literal(dialect, delimiter)
    args = [str_expr, delim_expr]
    if null_text is not None:
        null_expr = null_text if isinstance(null_text, bases.BaseExpression) else core.Literal(dialect, null_text)
        args.append(null_expr)
    return core.FunctionCall(dialect, "string_to_array", *args)


__all__ = [
    "array_agg",
    "array_append",
    "array_cat",
    "array_dims",
    "array_fill",
    "array_length",
    "array_lower",
    "array_ndims",
    "array_position",
    "array_positions",
    "array_prepend",
    "array_remove",
    "array_replace",
    "array_to_string",
    "array_upper",
    "unnest",
    "array_agg_distinct",
    "string_to_array",
]
