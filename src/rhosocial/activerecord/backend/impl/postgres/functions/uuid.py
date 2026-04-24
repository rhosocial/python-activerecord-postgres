# src/rhosocial/activerecord/backend/impl/postgres/functions/uuid.py
"""
PostgreSQL UUID function factories.

This module provides SQL expression generators for PostgreSQL UUID
functions and operators. All functions return FunctionCall expression
objects that integrate with the expression-dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-uuid.html

Note: Some functions require the 'uuid-ossp' extension.
Install with: CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression."""
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


def uuid_generate_v1(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Generate a version 1 (time-based) UUID.

    Requires the uuid-ossp extension.

    Returns:
        FunctionCall: SQL expression for uuid_generate_v1()

    Example:
        >>> func = uuid_generate_v1(dialect)
        >>> func.to_sql()
        ('uuid_generate_v1()', ())
    """
    return core.FunctionCall(dialect, "uuid_generate_v1")


def uuid_generate_v1mc(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Generate a version 1 UUID using a random multicast MAC address.

    Requires the uuid-ossp extension.

    Returns:
        FunctionCall: SQL expression for uuid_generate_v1mc()

    Example:
        >>> func = uuid_generate_v1mc(dialect)
        >>> func.to_sql()
        ('uuid_generate_v1mc()', ())
    """
    return core.FunctionCall(dialect, "uuid_generate_v1mc")


def uuid_generate_v3(
    dialect: "SQLDialectBase",
    namespace: Union[str, "bases.BaseExpression"],
    name: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """
    Generate a version 3 (MD5 hash-based) UUID in the given namespace.

    Requires the uuid-ossp extension.

    Args:
        dialect: The SQL dialect instance
        namespace: UUID namespace (e.g., 'dns', 'url', 'oid', 'x500' or a specific UUID)
        name: The name to hash

    Returns:
        FunctionCall: SQL expression for uuid_generate_v3(namespace, name)

    Example:
        >>> func = uuid_generate_v3(dialect, "dns", "example.com")
        >>> func.to_sql()
        ('uuid_generate_v3(%s, %s)', ('dns', 'example.com'))
    """
    ns_expr = _convert_to_expression(dialect, namespace)
    name_expr = _convert_to_expression(dialect, name)
    return core.FunctionCall(dialect, "uuid_generate_v3", ns_expr, name_expr)


def uuid_generate_v4(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Generate a version 4 (random) UUID.

    Requires the uuid-ossp extension.

    Returns:
        FunctionCall: SQL expression for uuid_generate_v4()

    Example:
        >>> func = uuid_generate_v4(dialect)
        >>> func.to_sql()
        ('uuid_generate_v4()', ())
    """
    return core.FunctionCall(dialect, "uuid_generate_v4")


def uuid_generate_v5(
    dialect: "SQLDialectBase",
    namespace: Union[str, "bases.BaseExpression"],
    name: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """
    Generate a version 5 (SHA-1 hash-based) UUID in the given namespace.

    Requires the uuid-ossp extension.

    Args:
        dialect: The SQL dialect instance
        namespace: UUID namespace (e.g., 'dns', 'url', 'oid', 'x500' or a specific UUID)
        name: The name to hash

    Returns:
        FunctionCall: SQL expression for uuid_generate_v5(namespace, name)

    Example:
        >>> func = uuid_generate_v5(dialect, "dns", "example.com")
        >>> func.to_sql()
        ('uuid_generate_v5(%s, %s)', ('dns', 'example.com'))
    """
    ns_expr = _convert_to_expression(dialect, namespace)
    name_expr = _convert_to_expression(dialect, name)
    return core.FunctionCall(dialect, "uuid_generate_v5", ns_expr, name_expr)


def uuid_ns_dns(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the DNS namespace UUID constant for use with uuid_generate_v3/v5.

    Requires the uuid-ossp extension.

    Returns:
        FunctionCall: SQL expression for uuid_ns_dns()

    Example:
        >>> func = uuid_ns_dns(dialect)
        >>> func.to_sql()
        ('uuid_ns_dns()', ())
    """
    return core.FunctionCall(dialect, "uuid_ns_dns")


def uuid_ns_url(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the URL namespace UUID constant for use with uuid_generate_v3/v5.

    Requires the uuid-ossp extension.

    Returns:
        FunctionCall: SQL expression for uuid_ns_url()
    """
    return core.FunctionCall(dialect, "uuid_ns_url")


def uuid_ns_oid(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the OID namespace UUID constant for use with uuid_generate_v3/v5.

    Requires the uuid-ossp extension.

    Returns:
        FunctionCall: SQL expression for uuid_ns_oid()
    """
    return core.FunctionCall(dialect, "uuid_ns_oid")


def uuid_ns_x500(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the X.500 namespace UUID constant for use with uuid_generate_v3/v5.

    Requires the uuid-ossp extension.

    Returns:
        FunctionCall: SQL expression for uuid_ns_x500()
    """
    return core.FunctionCall(dialect, "uuid_ns_x500")


def uuid_nil(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the nil UUID (all zeros).

    Returns:
        FunctionCall: SQL expression for uuid_nil()

    Example:
        >>> func = uuid_nil(dialect)
        >>> func.to_sql()
        ('uuid_nil()', ())
    """
    return core.FunctionCall(dialect, "uuid_nil")


def uuid_max(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Return the maximum UUID (all ones except the first byte).

    Returns:
        FunctionCall: SQL expression for uuid_max()

    Example:
        >>> func = uuid_max(dialect)
        >>> func.to_sql()
        ('uuid_max()', ())
    """
    return core.FunctionCall(dialect, "uuid_max")


__all__ = [
    "uuid_generate_v1",
    "uuid_generate_v1mc",
    "uuid_generate_v3",
    "uuid_generate_v4",
    "uuid_generate_v5",
    "uuid_ns_dns",
    "uuid_ns_url",
    "uuid_ns_oid",
    "uuid_ns_x500",
    "uuid_nil",
    "uuid_max",
]
