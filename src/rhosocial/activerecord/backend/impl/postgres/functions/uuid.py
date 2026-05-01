# src/rhosocial/activerecord/backend/impl/postgres/functions/uuid.py
"""
PostgreSQL UUID function factories.

This module provides SQL expression generators for PostgreSQL UUID
functions and operators. All functions return FunctionCall expression
objects that integrate with the expression-dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-uuid.html

UUID generation functions:
- gen_random_uuid(): Built-in since PostgreSQL 13, no extension required
- uuid_generate_v1/v3/v4/v5(): Require the 'uuid-ossp' extension
  Install with: CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

For DDL column defaults, use uuid_default_generator(dialect) which
automatically selects the appropriate function based on server version.
"""

import uuid as _uuid
from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[_uuid.UUID, str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression."""
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, _uuid.UUID):
        return core.Literal(dialect, str(expr))
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
    namespace: Union[_uuid.UUID, str, "bases.BaseExpression"],
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
    namespace: Union[_uuid.UUID, str, "bases.BaseExpression"],
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


def gen_random_uuid(dialect: "SQLDialectBase") -> core.FunctionCall:
    """
    Generate a version 4 (random) UUID.

    This is a built-in function available since PostgreSQL 13.
    For PostgreSQL < 13, use uuid_generate_v4() from the uuid-ossp extension.

    No extension required for PostgreSQL 13+.

    Returns:
        FunctionCall: SQL expression for gen_random_uuid()

    Example:
        >>> func = gen_random_uuid(dialect)
        >>> func.to_sql()
        ('gen_random_uuid()', ())
    """
    return core.FunctionCall(dialect, "gen_random_uuid")


def uuid_default_generator(dialect: "SQLDialectBase") -> core.FunctionCall:
    """Return the appropriate UUID generation function based on dialect version.

    This is the recommended way to obtain a UUID default value generator
    for DDL column definitions. It automatically selects the correct function:

    - PostgreSQL 13+: gen_random_uuid() (built-in, no extension required)
    - PostgreSQL < 13: uuid_generate_v4() (requires uuid-ossp extension)

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall: SQL expression for the version-appropriate UUID generator

    Example:
        >>> from rhosocial.activerecord.backend.expression.statements import (
        ...     ColumnDefinition, ColumnConstraint, ColumnConstraintType,
        ... )
        >>> uuid_func = uuid_default_generator(dialect)
        >>> ColumnDefinition('id', 'UUID', constraints=[
        ...     ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
        ...     ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=uuid_func),
        ... ])
    """
    version = dialect.get_server_version()
    if version >= (13, 0, 0):
        return gen_random_uuid(dialect)
    return uuid_generate_v4(dialect)


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
    "gen_random_uuid",
    "uuid_default_generator",
]
