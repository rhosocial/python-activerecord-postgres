# src/rhosocial/activerecord/backend/impl/postgres/functions/bit_string.py
"""
PostgreSQL Bit String Functions and Operators.

This module provides SQL expression generators for PostgreSQL bit string
functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-bitstring.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return SQL expression strings
"""

from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def bit_concat(dialect: "SQLDialectBase", bit1: str, bit2: str) -> str:
    """Generate SQL for bit string concatenation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        SQL expression: bit1 || bit2
    """
    return f"({bit1} || {bit2})"


def bit_and(dialect: "SQLDialectBase", bit1: str, bit2: str) -> str:
    """Generate SQL for bitwise AND operation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        SQL expression: bit1 & bit2
    """
    return f"({bit1} & {bit2})"


def bit_or(dialect: "SQLDialectBase", bit1: str, bit2: str) -> str:
    """Generate SQL for bitwise OR operation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        SQL expression: bit1 | bit2
    """
    return f"({bit1} | {bit2})"


def bit_xor(dialect: "SQLDialectBase", bit1: str, bit2: str) -> str:
    """Generate SQL for bitwise XOR operation.

    Args:
        dialect: The SQL dialect instance
        bit1: First bit string expression
        bit2: Second bit string expression

    Returns:
        SQL expression: bit1 # bit2
    """
    return f"({bit1} # {bit2})"


def bit_not(dialect: "SQLDialectBase", bit: str) -> str:
    """Generate SQL for bitwise NOT operation.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        SQL expression: ~bit
    """
    return f"(~{bit})"


def bit_shift_left(dialect: "SQLDialectBase", bit: str, n: Union[str, int]) -> str:
    """Generate SQL for bitwise left shift operation.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Number of positions to shift

    Returns:
        SQL expression: bit << n
    """
    return f"({bit} << {n})"


def bit_shift_right(dialect: "SQLDialectBase", bit: str, n: Union[str, int]) -> str:
    """Generate SQL for bitwise right shift operation.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Number of positions to shift

    Returns:
        SQL expression: bit >> n
    """
    return f"({bit} >> {n})"


def bit_length(dialect: "SQLDialectBase", bit: str) -> str:
    """Generate SQL for bit_length function.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        SQL expression: bit_length(bit)
    """
    return f"bit_length({bit})"


def bit_length_func(dialect: "SQLDialectBase", bit: str) -> str:
    """Generate SQL for length function on bit string.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        SQL expression: length(bit)
    """
    return f"length({bit})"


def bit_octet_length(dialect: "SQLDialectBase", bit: str) -> str:
    """Generate SQL for octet_length function on bit string.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        SQL expression: octet_length(bit)
    """
    return f"octet_length({bit})"


def bit_get_bit(dialect: "SQLDialectBase", bit: str, n: Union[str, int]) -> str:
    """Generate SQL for get_bit function.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Bit position (0-indexed)

    Returns:
        SQL expression: get_bit(bit, n)
    """
    return f"get_bit({bit}, {n})"


def bit_set_bit(dialect: "SQLDialectBase", bit: str, n: Union[str, int], value: Union[str, int]) -> str:
    """Generate SQL for set_bit function.

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression
        n: Bit position (0-indexed)
        value: Bit value (0 or 1)

    Returns:
        SQL expression: set_bit(bit, n, value)
    """
    return f"set_bit({bit}, {n}, {value})"


def bit_count(dialect: "SQLDialectBase", bit: str) -> str:
    """Generate SQL for bit_count function (PostgreSQL 14+).

    Args:
        dialect: The SQL dialect instance
        bit: Bit string expression

    Returns:
        SQL expression: bit_count(bit)
    """
    return f"bit_count({bit})"


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
