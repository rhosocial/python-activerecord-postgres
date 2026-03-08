# src/rhosocial/activerecord/backend/impl/postgres/functions/enum.py
"""
PostgreSQL Enum Functions.

This module provides SQL expression generators for PostgreSQL enum
functions.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-enum.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return SQL expression strings
"""
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def enum_range(dialect: "SQLDialectBase",
               enum_type: Optional[str] = None,
               enum_value: Optional[str] = None,
               start_value: Optional[str] = None,
               end_value: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL enum_range function.

    Args:
        dialect: The SQL dialect instance
        enum_type: Enum type name (generates 'enum_type::regtype')
        enum_value: Enum value (generates 'enum_range(enum_value)')
        start_value: Start value when using two-argument form
        end_value: End value when using two-argument form

    Returns:
        SQL expression string

    Examples:
        >>> enum_range(dialect, enum_type='status')
        "enum_range('status'::regtype)"

        >>> enum_range(dialect, enum_value='pending')
        "enum_range('pending')"

        >>> enum_range(dialect, start_value='pending', end_value='ready')
        "enum_range('pending', 'ready')"
    """
    if start_value is not None and end_value is not None:
        return f"enum_range('{start_value}', '{end_value}')"
    if enum_type is not None:
        return f"enum_range('{enum_type}'::regtype)"
    if enum_value is not None:
        return f"enum_range('{enum_value}')"
    raise ValueError("Must provide enum_type, enum_value, or both start_value and end_value")


def enum_first(dialect: "SQLDialectBase", enum_type_or_value: str) -> str:
    """Generate SQL expression for PostgreSQL enum_first function.

    Args:
        dialect: The SQL dialect instance
        enum_type_or_value: Enum type name or an enum value

    Returns:
        SQL expression string

    Examples:
        >>> enum_first(dialect, 'status')
        "enum_first('status'::regtype)"

        >>> enum_first(dialect, 'pending')
        "enum_first('pending')"
    """
    return f"enum_first('{enum_type_or_value}')"


def enum_last(dialect: "SQLDialectBase", enum_type_or_value: str) -> str:
    """Generate SQL expression for PostgreSQL enum_last function.

    Args:
        dialect: The SQL dialect instance
        enum_type_or_value: Enum type name or an enum value

    Returns:
        SQL expression string

    Examples:
        >>> enum_last(dialect, 'status')
        "enum_last('status'::regtype)"

        >>> enum_last(dialect, 'pending')
        "enum_last('pending')"
    """
    return f"enum_last('{enum_type_or_value}')"


def enum_lt(dialect: "SQLDialectBase", e1: str, e2: str) -> str:
    """Generate SQL expression for enum less-than comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        SQL expression string
    """
    return f"'{e1}' < '{e2}'"


def enum_le(dialect: "SQLDialectBase", e1: str, e2: str) -> str:
    """Generate SQL expression for enum less-than-or-equal comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        SQL expression string
    """
    return f"'{e1}' <= '{e2}'"


def enum_gt(dialect: "SQLDialectBase", e1: str, e2: str) -> str:
    """Generate SQL expression for enum greater-than comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        SQL expression string
    """
    return f"'{e1}' > '{e2}'"


def enum_ge(dialect: "SQLDialectBase", e1: str, e2: str) -> str:
    """Generate SQL expression for enum greater-than-or-equal comparison.

    Args:
        dialect: The SQL dialect instance
        e1: First enum value
        e2: Second enum value

    Returns:
        SQL expression string
    """
    return f"'{e1}' >= '{e2}'"


__all__ = [
    'enum_range',
    'enum_first',
    'enum_last',
    'enum_lt',
    'enum_le',
    'enum_gt',
    'enum_ge',
]
