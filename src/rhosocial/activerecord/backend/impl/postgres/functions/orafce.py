# src/rhosocial/activerecord/backend/impl/postgres/functions/orafce.py
"""
PostgreSQL orafce Extension Functions.

This module provides SQL expression generators for PostgreSQL orafce
extension functions. All functions return Expression objects (FunctionCall,
BinaryExpression) that integrate with the expression-dialect architecture.

The orafce extension provides Oracle-compatible functions for PostgreSQL,
making it easier to migrate applications from Oracle to PostgreSQL.

PostgreSQL Documentation: https://github.com/orafce/orafce

The orafce extension must be installed:
    CREATE EXTENSION IF NOT EXISTS orafce;

Supported functions:
- Date functions: ADD_MONTHS, LAST_DAY, MONTHS_BETWEEN, NEXT_DAY
- Null handling: NVL, NVL2
- Conditional: DECODE
- Numeric: TRUNC, ROUND
- String: INSTR, SUBSTR

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
"""

from typing import Union, Optional, TYPE_CHECKING

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


# ============== Date Functions ==============

def add_months(
    dialect: "SQLDialectBase",
    date_expr: Union[str, "bases.BaseExpression"],
    months: Union[int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Add months to a date.

    Returns the date plus the specified number of months. If the resulting
    date would have a day component that does not exist (e.g., adding one
    month to January 31), the result is the last day of the resulting month.

    Args:
        dialect: The SQL dialect instance
        date_expr: Date expression to add months to
        months: Number of months to add (can be negative)

    Returns:
        FunctionCall for ADD_MONTHS(date_expr, months)

    Example:
        >>> add_months(dialect, '2024-01-15', 3)
        >>> add_months(dialect, '2024-01-31', 1)
    """
    return core.FunctionCall(
        dialect, "ADD_MONTHS",
        _convert_to_expression(dialect, date_expr),
        _convert_to_expression(dialect, months),
    )


def last_day(
    dialect: "SQLDialectBase",
    date_expr: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Return the last day of the month for a given date.

    Returns the date of the last day of the month that contains the
    specified date. This is useful for calculating month-end dates.

    Args:
        dialect: The SQL dialect instance
        date_expr: Date expression

    Returns:
        FunctionCall for LAST_DAY(date_expr)

    Example:
        >>> last_day(dialect, '2024-02-15')
        >>> last_day(dialect, '2024-01-01')
    """
    return core.FunctionCall(
        dialect, "LAST_DAY",
        _convert_to_expression(dialect, date_expr),
    )


def months_between(
    dialect: "SQLDialectBase",
    date1: Union[str, "bases.BaseExpression"],
    date2: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Calculate the number of months between two dates.

    Returns the number of months between date1 and date2. If both dates
    are the same day of the month, or both are the last day of the month,
    the result is an integer. Otherwise, the fractional portion is based
    on a 31-day month.

    Args:
        dialect: The SQL dialect instance
        date1: First date (minuend)
        date2: Second date (subtrahend)

    Returns:
        FunctionCall for MONTHS_BETWEEN(date1, date2)

    Example:
        >>> months_between(dialect, '2024-06-15', '2024-01-15')
        >>> months_between(dialect, '2024-06-20', '2024-01-10')
    """
    return core.FunctionCall(
        dialect, "MONTHS_BETWEEN",
        _convert_to_expression(dialect, date1),
        _convert_to_expression(dialect, date2),
    )


def next_day(
    dialect: "SQLDialectBase",
    date_expr: Union[str, "bases.BaseExpression"],
    day: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Return the date of the next specified day of the week after a given date.

    Returns the date of the first occurrence of the specified day of the week
    that is later than the given date.

    Args:
        dialect: The SQL dialect instance
        date_expr: Date expression to start from
        day: Day of week name (e.g., 'Monday', 'Tuesday') or abbreviation

    Returns:
        FunctionCall for NEXT_DAY(date_expr, day)

    Example:
        >>> next_day(dialect, '2024-01-01', 'Monday')
        >>> next_day(dialect, '2024-06-15', 'FRIDAY')
    """
    return core.FunctionCall(
        dialect, "NEXT_DAY",
        _convert_to_expression(dialect, date_expr),
        _convert_to_expression(dialect, day),
    )


# ============== Null Handling Functions ==============

def nvl(
    dialect: "SQLDialectBase",
    expr1: Union[str, "bases.BaseExpression"],
    expr2: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Return expr2 if expr1 is NULL, otherwise return expr1.

    This is the Oracle-compatible NULL handling function, equivalent to
    COALESCE(expr1, expr2) in standard SQL.

    Args:
        dialect: The SQL dialect instance
        expr1: Expression to check for NULL
        expr2: Value to return if expr1 is NULL

    Returns:
        FunctionCall for NVL(expr1, expr2)

    Example:
        >>> nvl(dialect, 'middle_name', "'N/A'")
        >>> nvl(dialect, 'discount', '0')
    """
    return core.FunctionCall(
        dialect, "NVL",
        _convert_to_expression(dialect, expr1),
        _convert_to_expression(dialect, expr2),
    )


def nvl2(
    dialect: "SQLDialectBase",
    expr1: Union[str, "bases.BaseExpression"],
    expr2: Union[str, "bases.BaseExpression"],
    expr3: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Return expr2 if expr1 is NOT NULL, otherwise return expr3.

    This is the Oracle-compatible NULL handling function. Unlike NVL,
    NVL2 allows specifying different return values for both the NULL
    and NOT NULL cases.

    Args:
        dialect: The SQL dialect instance
        expr1: Expression to check for NULL
        expr2: Value to return if expr1 is NOT NULL
        expr3: Value to return if expr1 is NULL

    Returns:
        FunctionCall for NVL2(expr1, expr2, expr3)

    Example:
        >>> nvl2(dialect, 'email', "'has email'", "'no email'")
    """
    return core.FunctionCall(
        dialect, "NVL2",
        _convert_to_expression(dialect, expr1),
        _convert_to_expression(dialect, expr2),
        _convert_to_expression(dialect, expr3),
    )


# ============== Conditional Functions ==============

def decode(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
    *matches: Union[str, "bases.BaseExpression"],
    default: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Oracle-compatible DECODE conditional expression.

    Compares the expression to each search value one by one. If the
    expression equals a search value, the corresponding result is returned.
    If no match is found, the default value is returned (or NULL if no
    default is specified).

    The matches parameter accepts pairs of (search, result) values. The
    number of match arguments should be even (each search value paired
    with its result).

    Args:
        dialect: The SQL dialect instance
        expr: Expression to evaluate
        *matches: Pairs of (search, result) values
        default: Optional default value when no match is found

    Returns:
        FunctionCall for DECODE(expr, search1, result1, search2, result2, ..., default)

    Example:
        >>> decode(dialect, 'status', "'active'", '1', "'inactive'", '0', default='-1')
        >>> decode(dialect, 'color', "'red'", "'R'", "'blue'", "'B'")
    """
    args = [_convert_to_expression(dialect, expr)]
    for m in matches:
        args.append(_convert_to_expression(dialect, m))
    if default is not None:
        args.append(_convert_to_expression(dialect, default))
    return core.FunctionCall(dialect, "DECODE", *args)


# ============== Numeric Functions ==============

def trunc(
    dialect: "SQLDialectBase",
    value: Union[str, "bases.BaseExpression"],
    format: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Truncate a date or number to the specified precision.

    When used with a number, truncates toward zero to the specified decimal
    places. When used with a date, truncates to the specified unit
    (e.g., 'YEAR', 'MONTH', 'DAY').

    Args:
        dialect: The SQL dialect instance
        value: Value to truncate (number or date expression)
        format: Optional format/precision specification. For numbers, this
                is the number of decimal places. For dates, it is the unit
                (e.g., 'YEAR', 'Q', 'MONTH', 'WW', 'DAY', 'HH', 'MI').

    Returns:
        FunctionCall for TRUNC(value) or TRUNC(value, format)

    Example:
        >>> trunc(dialect, 'price', '2')
        >>> trunc(dialect, 'created_at', "'MONTH'")
        >>> trunc(dialect, '123.456')
    """
    args = [_convert_to_expression(dialect, value)]
    if format is not None:
        args.append(_convert_to_expression(dialect, format))
    return core.FunctionCall(dialect, "TRUNC", *args)


def round(
    dialect: "SQLDialectBase",
    value: Union[str, "bases.BaseExpression"],
    format: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Round a date or number to the specified precision.

    When used with a number, rounds to the specified decimal places.
    When used with a date, rounds to the specified unit (e.g., 'YEAR',
    'MONTH', 'DAY').

    Args:
        dialect: The SQL dialect instance
        value: Value to round (number or date expression)
        format: Optional format/precision specification. For numbers, this
                is the number of decimal places. For dates, it is the unit
                (e.g., 'YEAR', 'Q', 'MONTH', 'WW', 'DAY', 'HH', 'MI').

    Returns:
        FunctionCall for ROUND(value) or ROUND(value, format)

    Example:
        >>> round(dialect, 'price', '2')
        >>> round(dialect, 'created_at', "'MONTH'")
        >>> round(dialect, '123.456')
    """
    args = [_convert_to_expression(dialect, value)]
    if format is not None:
        args.append(_convert_to_expression(dialect, format))
    return core.FunctionCall(dialect, "ROUND", *args)


# ============== String Functions ==============

def instr(
    dialect: "SQLDialectBase",
    string_expr: Union[str, "bases.BaseExpression"],
    substring_expr: Union[str, "bases.BaseExpression"],
    position: Union[int, "bases.BaseExpression"] = 1,
    occurrence: Union[int, "bases.BaseExpression"] = 1,
) -> core.FunctionCall:
    """Find the position of a substring within a string.

    Returns the position (1-based) of the specified occurrence of the
    substring within the string, starting the search at the specified
    position. Returns 0 if the substring is not found.

    Args:
        dialect: The SQL dialect instance
        string_expr: String to search in
        substring_expr: Substring to find
        position: Starting position for the search (1-based, default: 1)
        occurrence: Which occurrence to find (default: 1)

    Returns:
        FunctionCall for INSTR(string_expr, substring_expr, position, occurrence)

    Example:
        >>> instr(dialect, 'full_name', "'@'")
        >>> instr(dialect, 'description', "'the'", position=1, occurrence=2)
    """
    return core.FunctionCall(
        dialect, "INSTR",
        _convert_to_expression(dialect, string_expr),
        _convert_to_expression(dialect, substring_expr),
        _convert_to_expression(dialect, position),
        _convert_to_expression(dialect, occurrence),
    )


def substr(
    dialect: "SQLDialectBase",
    string_expr: Union[str, "bases.BaseExpression"],
    position: Union[int, "bases.BaseExpression"],
    length: Optional[Union[int, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Extract a substring from a string.

    Returns a portion of the string starting at the specified position
    (1-based). If length is specified, returns that many characters.
    If length is omitted, returns all characters from the position to
    the end of the string.

    Args:
        dialect: The SQL dialect instance
        string_expr: String expression to extract from
        position: Starting position (1-based)
        length: Optional length of the substring. If None, returns to end of string.

    Returns:
        FunctionCall for SUBSTR(string_expr, position) or
        FunctionCall for SUBSTR(string_expr, position, length)

    Example:
        >>> substr(dialect, 'full_name', '1', '5')
        >>> substr(dialect, 'description', '10')
    """
    args = [
        _convert_to_expression(dialect, string_expr),
        _convert_to_expression(dialect, position),
    ]
    if length is not None:
        args.append(_convert_to_expression(dialect, length))
    return core.FunctionCall(dialect, "SUBSTR", *args)


__all__ = [
    # Date functions
    "add_months",
    "last_day",
    "months_between",
    "next_day",
    # Null handling functions
    "nvl",
    "nvl2",
    # Conditional functions
    "decode",
    # Numeric functions
    "trunc",
    "round",
    # String functions
    "instr",
    "substr",
]
