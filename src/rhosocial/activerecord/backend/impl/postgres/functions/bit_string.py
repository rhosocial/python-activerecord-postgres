# src/rhosocial/activerecord/backend/impl/postgres/functions/bit_string.py
"""
PostgreSQL Bit String Functions and Operators.

This module provides SQL expression generators for PostgreSQL bit string
functions and operators. All functions return Expression objects
that integrate with the Expression/Dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-bitstring.html

Supported operators:
- ||  : Bit string concatenation
- &   : Bitwise AND
- |   : Bitwise OR
- #   : Bitwise XOR
- ~   : Bitwise NOT (unary)
- <<  : Bitwise left shift
- >>  : Bitwise right shift

Supported functions:
- length(bit_string)         : Bit string length
- bit_length(bit_string)     : Bit string bit length
- octet_length(bit_string)   : Bit string octet length
- get_bit(bit_string, n)     : Get bit at position
- set_bit(bit_string, n, v)  : Set bit at position
- bit_count(bit_string)      : Count set bits (PostgreSQL 14+)
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import (
    BinaryArithmeticExpression,
    UnaryExpression,
)

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


# ============== Operators ==============

def bit_concat(
    dialect: "SQLDialectBase",
    bit1: Union[str, "bases.BaseExpression"],
    bit2: Union[str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Generate SQL for bit string concatenation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        BinaryArithmeticExpression for bit1 || bit2
    """
    return BinaryArithmeticExpression(
        dialect, "||",
        _convert_to_expression(dialect, bit1),
        _convert_to_expression(dialect, bit2),
    )


def bit_and(
    dialect: "SQLDialectBase",
    bit1: Union[str, "bases.BaseExpression"],
    bit2: Union[str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Generate SQL for bitwise AND operation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        BinaryArithmeticExpression for bit1 & bit2
    """
    return BinaryArithmeticExpression(
        dialect, "&",
        _convert_to_expression(dialect, bit1),
        _convert_to_expression(dialect, bit2),
    )


def bit_or(
    dialect: "SQLDialectBase",
    bit1: Union[str, "bases.BaseExpression"],
    bit2: Union[str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Generate SQL for bitwise OR operation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        BinaryArithmeticExpression for bit1 | bit2
    """
    return BinaryArithmeticExpression(
        dialect, "|",
        _convert_to_expression(dialect, bit1),
        _convert_to_expression(dialect, bit2),
    )


def bit_xor(
    dialect: "SQLDialectBase",
    bit1: Union[str, "bases.BaseExpression"],
    bit2: Union[str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Generate SQL for bitwise XOR operation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        BinaryArithmeticExpression for bit1 # bit2
    """
    return BinaryArithmeticExpression(
        dialect, "#",
        _convert_to_expression(dialect, bit1),
        _convert_to_expression(dialect, bit2),
    )


def bit_not(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
) -> UnaryExpression:
    """Generate SQL for bitwise NOT operation.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        UnaryExpression for ~bit
    """
    return UnaryExpression(
        dialect, "~",
        _convert_to_expression(dialect, bit),
        pos="before",
    )


def bit_shift_left(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
    n: Union[str, int, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Generate SQL for bitwise left shift operation.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Number of positions to shift

    Returns:
        BinaryArithmeticExpression for bit << n
    """
    return BinaryArithmeticExpression(
        dialect, "<<",
        _convert_to_expression(dialect, bit),
        _convert_to_expression(dialect, n),
    )


def bit_shift_right(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
    n: Union[str, int, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Generate SQL for bitwise right shift operation.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Number of positions to shift

    Returns:
        BinaryArithmeticExpression for bit >> n
    """
    return BinaryArithmeticExpression(
        dialect, ">>",
        _convert_to_expression(dialect, bit),
        _convert_to_expression(dialect, n),
    )


# ============== Functions ==============

def bit_length(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL for length function on bit string.

    Note: PostgreSQL uses length() for bit strings, not bit_length().

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        FunctionCall for length(bit)
    """
    return core.FunctionCall(dialect, "length", _convert_to_expression(dialect, bit))


def bit_length_func(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL for bit_length function on bit string.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        FunctionCall for bit_length(bit)
    """
    return core.FunctionCall(dialect, "bit_length", _convert_to_expression(dialect, bit))


def bit_octet_length(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL for octet_length function on bit string.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        FunctionCall for octet_length(bit)
    """
    return core.FunctionCall(dialect, "octet_length", _convert_to_expression(dialect, bit))


def bit_get_bit(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
    n: Union[str, int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL for get_bit function.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Bit position (0-indexed)

    Returns:
        FunctionCall for get_bit(bit, n)
    """
    return core.FunctionCall(
        dialect, "get_bit",
        _convert_to_expression(dialect, bit),
        _convert_to_expression(dialect, n),
    )


def bit_set_bit(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
    n: Union[str, int, "bases.BaseExpression"],
    value: Union[str, int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL for set_bit function.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Bit position (0-indexed)
        value: Bit value (0 or 1)

    Returns:
        FunctionCall for set_bit(bit, n, value)
    """
    return core.FunctionCall(
        dialect, "set_bit",
        _convert_to_expression(dialect, bit),
        _convert_to_expression(dialect, n),
        _convert_to_expression(dialect, value),
    )


def bit_count(
    dialect: "SQLDialectBase",
    bit: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate SQL for bit_count function (PostgreSQL 14+).

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        FunctionCall for bit_count(bit)
    """
    return core.FunctionCall(dialect, "bit_count", _convert_to_expression(dialect, bit))


__all__ = [
    "bit_concat",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bit_not",
    "bit_shift_left",
    "bit_shift_right",
    "bit_length",
    "bit_length_func",
    "bit_octet_length",
    "bit_get_bit",
    "bit_set_bit",
    "bit_count",
]
