# src/rhosocial/activerecord/backend/impl/postgres/functions/enum.py
"""
PostgreSQL Enum Functions and Operators.

This module provides SQL expression generators for PostgreSQL enum
functions and comparison operators. All functions return Expression
objects that integrate with the Expression/Dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-enum.html

Supported functions:
- enum_range(enum_type): Returns all values of the enum type
- enum_range(enum_value): Returns range of values up to the given value
- enum_range(start, end): Returns range of values between two enum values
- enum_first(enum_type_or_value): Returns the first value of the enum
- enum_last(enum_type_or_value): Returns the last value of the enum

Supported comparison operators:
- enum_lt(e1, e2): Less than (<)
- enum_le(e1, e2): Less than or equal (<=)
- enum_gt(e1, e2): Greater than (>)
- enum_ge(e1, e2): Greater than or equal (>=)
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


# ============== Functions ==============


def enum_range(
    dialect: "SQLDialectBase",
    enum_type: Optional[Union[str, "bases.BaseExpression"]] = None,
    enum_value: Optional[Union[str, "bases.BaseExpression"]] = None,
    start_value: Optional[Union[str, "bases.BaseExpression"]] = None,
    end_value: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL enum_range function.

    Supports three calling conventions:
    1. enum_range(enum_type) - Returns all values of the enum type,
       with the type cast to regtype.
    2. enum_range(enum_value) - Returns range of values up to the
       given enum value.
    3. enum_range(start_value, end_value) - Returns range of values
       between two enum values.

    Args:
        dialect: The SQL dialect instance
        enum_type: Enum type name (generates enum_type::regtype cast)
        enum_value: Enum value (generates enum_range(enum_value))
        start_value: Start value when using two-argument form
        end_value: End value when using two-argument form

    Returns:
        FunctionCall expression for enum_range

    Examples:
        >>> func = enum_range(dialect, enum_type='status')
        >>> func.to_sql()
        ("enum_range(CAST(%s AS regtype))", ('status',))

        >>> func = enum_range(dialect, enum_value='pending')
        >>> func.to_sql()
        ('enum_range(%s)', ('pending',))

        >>> func = enum_range(dialect, start_value='pending', end_value='ready')
        >>> func.to_sql()
        ('enum_range(%s, %s)', ('pending', 'ready'))
    """
    if start_value is not None and end_value is not None:
        start_expr = _convert_to_expression(dialect, start_value)
        end_expr = _convert_to_expression(dialect, end_value)
        return core.FunctionCall(dialect, "enum_range", start_expr, end_expr)
    if enum_type is not None:
        type_expr = _convert_to_expression(dialect, enum_type)
        return core.FunctionCall(dialect, "enum_range", type_expr.cast("regtype"))
    if enum_value is not None:
        value_expr = _convert_to_expression(dialect, enum_value)
        return core.FunctionCall(dialect, "enum_range", value_expr)
    raise ValueError("Must provide enum_type, enum_value, or both start_value and end_value")


def enum_first(
    dialect: "SQLDialectBase",
    enum_type_or_value: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL enum_first function.

    Returns the first value of the enum type. The argument is
    cast to regtype.

    Args:
        dialect: The SQL dialect instance
        enum_type_or_value: Enum type name or an enum value

    Returns:
        FunctionCall expression for enum_first

    Examples:
        >>> func = enum_first(dialect, 'status')
        >>> func.to_sql()
        ("enum_first(CAST(%s AS regtype))", ('status',))
    """
    type_expr = _convert_to_expression(dialect, enum_type_or_value)
    return core.FunctionCall(dialect, "enum_first", type_expr.cast("regtype"))


def enum_last(
    dialect: "SQLDialectBase",
    enum_type_or_value: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL expression for PostgreSQL enum_last function.

    Returns the last value of the enum type. The argument is
    cast to regtype.

    Args:
        dialect: The SQL dialect instance
        enum_type_or_value: Enum type name or an enum value

    Returns:
        FunctionCall expression for enum_last

    Examples:
        >>> func = enum_last(dialect, 'status')
        >>> func.to_sql()
        ("enum_last(CAST(%s AS regtype))", ('status',))
    """
    type_expr = _convert_to_expression(dialect, enum_type_or_value)
    return core.FunctionCall(dialect, "enum_last", type_expr.cast("regtype"))


# ============== Comparison Operators ==============


def enum_lt(
    dialect: "SQLDialectBase",
    e1: Union[str, "bases.BaseExpression"],
    e2: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for enum less-than comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        BinaryExpression for e1 < e2

    Examples:
        >>> expr = enum_lt(dialect, 'pending', 'active')
        >>> expr.to_sql()
        ('%s < %s', ('pending', 'active'))
    """
    left = _convert_to_expression(dialect, e1)
    right = _convert_to_expression(dialect, e2)
    return BinaryExpression(dialect, "<", left, right)


def enum_le(
    dialect: "SQLDialectBase",
    e1: Union[str, "bases.BaseExpression"],
    e2: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for enum less-than-or-equal comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        BinaryExpression for e1 <= e2

    Examples:
        >>> expr = enum_le(dialect, 'pending', 'active')
        >>> expr.to_sql()
        ('%s <= %s', ('pending', 'active'))
    """
    left = _convert_to_expression(dialect, e1)
    right = _convert_to_expression(dialect, e2)
    return BinaryExpression(dialect, "<=", left, right)


def enum_gt(
    dialect: "SQLDialectBase",
    e1: Union[str, "bases.BaseExpression"],
    e2: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for enum greater-than comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        BinaryExpression for e1 > e2

    Examples:
        >>> expr = enum_gt(dialect, 'pending', 'active')
        >>> expr.to_sql()
        ('%s > %s', ('pending', 'active'))
    """
    left = _convert_to_expression(dialect, e1)
    right = _convert_to_expression(dialect, e2)
    return BinaryExpression(dialect, ">", left, right)


def enum_ge(
    dialect: "SQLDialectBase",
    e1: Union[str, "bases.BaseExpression"],
    e2: Union[str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Generate SQL expression for enum greater-than-or-equal comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        BinaryExpression for e1 >= e2

    Examples:
        >>> expr = enum_ge(dialect, 'pending', 'active')
        >>> expr.to_sql()
        ('%s >= %s', ('pending', 'active'))
    """
    left = _convert_to_expression(dialect, e1)
    right = _convert_to_expression(dialect, e2)
    return BinaryExpression(dialect, ">=", left, right)


__all__ = [
    "enum_range",
    "enum_first",
    "enum_last",
    "enum_lt",
    "enum_le",
    "enum_gt",
    "enum_ge",
]
