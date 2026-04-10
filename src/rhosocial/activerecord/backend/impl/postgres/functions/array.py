# src/rhosocial/activerecord/backend/impl/postgres/functions/array.py
"""
PostgreSQL Array Functions.

This module provides SQL expression generators for PostgreSQL array
functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-array.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return SQL expression strings
"""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _to_sql(expr: Any) -> str:
    """Convert an expression to its SQL string representation."""
    if hasattr(expr, 'to_sql'):
        return expr.to_sql()[0]
    return str(expr)


def array_agg(dialect: "SQLDialectBase", expression: Any) -> str:
    """
    Aggregate function that collects values into an array.

    Args:
        dialect: The SQL dialect instance
        expression: The expression to aggregate

    Returns:
        SQL expression: array_agg(expression)

    Example:
        >>> array_agg(dialect, "SELECT value FROM table")
        'array_agg(value)'
    """
    return f"array_agg({_to_sql(expression)})"


def array_append(dialect: "SQLDialectBase", array: Any, element: Any) -> str:
    """
    Append an element to the end of an array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to append

    Returns:
        SQL expression: array_append(array, element)

    Example:
        >>> array_append(dialect, "'{1,2}'", 3)
        "array_append('{1,2}', 3)"
    """
    return f"array_append({_to_sql(array)}, {_to_sql(element)})"


def array_cat(dialect: "SQLDialectBase", array1: Any, array2: Any) -> str:
    """
    Concatenate two arrays.

    Args:
        dialect: The SQL dialect instance
        array1: First array
        array2: Second array

    Returns:
        SQL expression: array_cat(array1, array2)

    Example:
        >>> array_cat(dialect, "'{1,2}'", "'{3,4}'")
        "array_cat('{1,2}', '{3,4}')"
    """
    return f"array_cat({_to_sql(array1)}, {_to_sql(array2)})"


def array_dims(dialect: "SQLDialectBase", array: Any) -> str:
    """
    Return the dimensions of an array as text.

    Args:
        dialect: The SQL dialect instance
        array: The array expression

    Returns:
        SQL expression: array_dims(array)

    Example:
        >>> array_dims(dialect, "'{{1,2},{3,4}}'")
        "array_dims('{{1,2},{3,4}}')"
    """
    return f"array_dims({_to_sql(array)})"


def array_fill(dialect: "SQLDialectBase", value: Any, dims: Any) -> str:
    """
    Return an array filled with a value for specified dimensions.

    Args:
        dialect: The SQL dialect instance
        value: The value to fill the array with
        dims: The array dimensions

    Returns:
        SQL expression: array_fill(value, dims)

    Example:
        >>> array_fill(dialect, 0, "'{3,3}'")
        "array_fill(0, '{3,3}')"
    """
    return f"array_fill({_to_sql(value)}, {_to_sql(dims)})"


def array_length(dialect: "SQLDialectBase", array: Any, dimension: int = 1) -> str:
    """
    Return the length of an array for the specified dimension.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        dimension: The dimension (1-indexed, default 1)

    Returns:
        SQL expression: array_length(array, dimension)

    Example:
        >>> array_length(dialect, "'{1,2,3}'", 1)
        "array_length('{1,2,3}', 1)"
    """
    return f"array_length({_to_sql(array)}, {dimension})"


def array_lower(dialect: "SQLDialectBase", array: Any, dimension: int = 1) -> str:
    """
    Return the lower bound of an array's dimension.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        dimension: The dimension (1-indexed, default 1)

    Returns:
        SQL expression: array_lower(array, dimension)

    Example:
        >>> array_lower(dialect, "'[1:3]={1,2,3}'", 1)
        "array_lower('[1:3]={1,2,3}', 1)"
    """
    return f"array_lower({_to_sql(array)}, {dimension})"


def array_ndims(dialect: "SQLDialectBase", array: Any) -> str:
    """
    Return the number of dimensions of an array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression

    Returns:
        SQL expression: array_ndims(array)

    Example:
        >>> array_ndims(dialect, "'{{1,2},{3,4}}'")
        "array_ndims('{{1,2},{3,4}}')"
    """
    return f"array_ndims({_to_sql(array)})"


def array_position(dialect: "SQLDialectBase", array: Any, element: Any) -> str:
    """
    Return the position of the first occurrence of element in array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to find

    Returns:
        SQL expression: array_position(array, element)

    Example:
        >>> array_position(dialect, "'{a,b,c}'", "'b'")
        "array_position('{a,b,c}', 'b')"
    """
    return f"array_position({_to_sql(array)}, {_to_sql(element)})"


def array_positions(dialect: "SQLDialectBase", array: Any, element: Any) -> str:
    """
    Return an array of positions of element in array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to find

    Returns:
        SQL expression: array_positions(array, element)

    Example:
        >>> array_positions(dialect, "'[1,3,5,3]'", 3)
        "array_positions('[1,3,5,3]', 3)"
    """
    return f"array_positions({_to_sql(array)}, {_to_sql(element)})"


def array_prepend(dialect: "SQLDialectBase", element: Any, array: Any) -> str:
    """
    Prepend an element to the beginning of an array.

    Args:
        dialect: The SQL dialect instance
        element: The element to prepend
        array: The array expression

    Returns:
        SQL expression: array_prepend(element, array)

    Example:
        >>> array_prepend(dialect, 1, "'{2,3}'")
        "array_prepend(1, '{2,3}')"
    """
    return f"array_prepend({_to_sql(element)}, {_to_sql(array)})"


def array_remove(dialect: "SQLDialectBase", array: Any, element: Any) -> str:
    """
    Remove all elements equal to the specified value from array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        element: The element to remove

    Returns:
        SQL expression: array_remove(array, element)

    Example:
        >>> array_remove(dialect, "'{1,2,3,2}'", 2)
        "array_remove('{1,2,3,2}', 2)"
    """
    return f"array_remove({_to_sql(array)}, {_to_sql(element)})"


def array_replace(dialect: "SQLDialectBase", array: Any, search: Any, replace: Any) -> str:
    """
    Replace all occurrences of search with replace in array.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        search: The value to search for
        replace: The value to replace with

    Returns:
        SQL expression: array_replace(array, search, replace)

    Example:
        >>> array_replace(dialect, "'{1,2,3}'", 2, 4)
        "array_replace('{1,2,3}', 2, 4)"
    """
    return f"array_replace({_to_sql(array)}, {_to_sql(search)}, {_to_sql(replace)})"


def array_to_string(dialect: "SQLDialectBase", array: Any, delimiter: str) -> str:
    """
    Convert array to string using delimiter.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        delimiter: The delimiter string

    Returns:
        SQL expression: array_to_string(array, delimiter)

    Example:
        >>> array_to_string(dialect, "'{1,2,3}'", ',')
        "array_to_string('{1,2,3}', ',')"
    """
    return f"array_to_string({_to_sql(array)}, '{delimiter}')"


def array_upper(dialect: "SQLDialectBase", array: Any, dimension: int = 1) -> str:
    """
    Return the upper bound of an array's dimension.

    Args:
        dialect: The SQL dialect instance
        array: The array expression
        dimension: The dimension (1-indexed, default 1)

    Returns:
        SQL expression: array_upper(array, dimension)

    Example:
        >>> array_upper(dialect, "'[1:3]={1,2,3}'", 1)
        "array_upper('[1:3]={1,2,3}', 1)"
    """
    return f"array_upper({_to_sql(array)}, {dimension})"


def unnest(dialect: "SQLDialectBase", *arrays: Any) -> str:
    """
    Expand an array into a set of rows.

    Args:
        dialect: The SQL dialect instance
        *arrays: One or more array expressions

    Returns:
        SQL expression: unnest(array1, array2, ...)

    Example:
        >>> unnest(dialect, "'{1,2}'", "'{a,b}'")
        "unnest('{1,2}', '{a,b}')"
    """
    if not arrays:
        return "unnest()"
    return f"unnest({', '.join(_to_sql(a) for a in arrays)})"


def array_agg_distinct(dialect: "SQLDialectBase", expression: Any) -> str:
    """
    Aggregate function that collects distinct values into an array.

    Args:
        dialect: The SQL dialect instance
        expression: The expression to aggregate

    Returns:
        SQL expression: array_agg(DISTINCT expression)

    Example:
        >>> array_agg_distinct(dialect, "SELECT value FROM table")
        'array_agg(DISTINCT value)'
    """
    return f"array_agg(DISTINCT {_to_sql(expression)})"


def string_to_array(dialect: "SQLDialectBase", string: Any, delimiter: str, null_string: str = None) -> str:
    """
    Split string into array using delimiter.

    Args:
        dialect: The SQL dialect instance
        string: The input string
        delimiter: The delimiter string
        null_string: If specified, occurrences of this string in string are replaced by NULL

    Returns:
        SQL expression: string_to_array(string, delimiter[, null_string])

    Example:
        >>> string_to_array(dialect, "'a,b,c'", ',')
        "string_to_array('a,b,c', ',')"
    """
    if null_string is not None:
        return f"string_to_array({_to_sql(string)}, '{delimiter}', '{null_string}')"
    return f"string_to_array({_to_sql(string)}, '{delimiter}')"


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