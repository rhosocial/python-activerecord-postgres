# src/rhosocial/activerecord/backend/impl/postgres/functions/uuid.py
"""
PostgreSQL UUID Functions.

This module provides SQL expression generators for PostgreSQL UUID
functions and operators.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-uuid.html

Note: Some functions require the 'uuid-ossp' extension.
Install with: CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

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


def uuid_generate_v1(dialect: "SQLDialectBase") -> str:
    """
    Generate a version 1 (time-based) UUID.

    Requires the uuid-ossp extension.

    Returns:
        SQL expression: uuid_generate_v1()

    Example:
        >>> uuid_generate_v1(dialect)
        'uuid_generate_v1()'
    """
    return "uuid_generate_v1()"


def uuid_generate_v1mc(dialect: "SQLDialectBase") -> str:
    """
    Generate a version 1 UUID using a random multicast MAC address.

    Requires the uuid-ossp extension.

    Returns:
        SQL expression: uuid_generate_v1mc()

    Example:
        >>> uuid_generate_v1mc(dialect)
        'uuid_generate_v1mc()'
    """
    return "uuid_generate_v1mc()"


def uuid_generate_v3(dialect: "SQLDialectBase", namespace: str, name: str) -> str:
    """
    Generate a version 3 (MD5 hash-based) UUID in the given namespace.

    Requires the uuid-ossp extension.

    Args:
        dialect: The SQL dialect instance
        namespace: UUID namespace (e.g., 'dns', 'url', 'oid', 'x500' or a specific UUID)
        name: The name to hash

    Returns:
        SQL expression: uuid_generate_v3(namespace, name)

    Example:
        >>> uuid_generate_v3(dialect, "'dns'", "'example.com'")
        "uuid_generate_v3('dns', 'example.com')"
    """
    return f"uuid_generate_v3({_to_sql(namespace)}, {_to_sql(name)})"


def uuid_generate_v4(dialect: "SQLDialectBase") -> str:
    """
    Generate a version 4 (random) UUID.

    Requires the uuid-ossp extension.

    Returns:
        SQL expression: uuid_generate_v4()

    Example:
        >>> uuid_generate_v4(dialect)
        'uuid_generate_v4()'
    """
    return "uuid_generate_v4()"


def uuid_generate_v5(dialect: "SQLDialectBase", namespace: str, name: str) -> str:
    """
    Generate a version 5 (SHA-1 hash-based) UUID in the given namespace.

    Requires the uuid-ossp extension.

    Args:
        dialect: The SQL dialect instance
        namespace: UUID namespace (e.g., 'dns', 'url', 'oid', 'x500' or a specific UUID)
        name: The name to hash

    Returns:
        SQL expression: uuid_generate_v5(namespace, name)

    Example:
        >>> uuid_generate_v5(dialect, "'dns'", "'example.com'")
        "uuid_generate_v5('dns', 'example.com')"
    """
    return f"uuid_generate_v5({_to_sql(namespace)}, {_to_sql(name)})"


def uuid_nil(dialect: "SQLDialectBase") -> str:
    """
    Return the nil UUID (all zeros).

    Returns:
        SQL expression: uuid_nil()

    Example:
        >>> uuid_nil(dialect)
        'uuid_nil()'
    """
    return "uuid_nil()"


def uuid_max(dialect: "SQLDialectBase") -> str:
    """
    Return the maximum UUID (all ones except the first byte).

    Returns:
        SQL expression: uuid_max()

    Example:
        >>> uuid_max(dialect)
        'uuid_max()'
    """
    return "uuid_max()"


__all__ = [
    "uuid_generate_v1",
    "uuid_generate_v1mc",
    "uuid_generate_v3",
    "uuid_generate_v4",
    "uuid_generate_v5",
    "uuid_nil",
    "uuid_max",
]