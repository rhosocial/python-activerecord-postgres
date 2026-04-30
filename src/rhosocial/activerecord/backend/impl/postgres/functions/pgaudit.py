# src/rhosocial/activerecord/backend/impl/postgres/functions/pgaudit.py
"""
PostgreSQL pgaudit Extension Functions.

This module provides SQL expression generators for PostgreSQL pgaudit
extension configuration functions. All functions return Expression objects
(FunctionCall) that integrate with the expression-dialect architecture.

The pgaudit extension provides session and object audit logging for
PostgreSQL via the standard PostgreSQL logging facility.

PostgreSQL Documentation: https://github.com/pgaudit/pgaudit

The pgaudit extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pgaudit;

Supported functions:
- pgaudit_set_role: Set the pgaudit.role configuration parameter
- pgaudit_log_level: Set the pgaudit.log_level configuration parameter
- pgaudit_include_catalog: Set the pgaudit.log_catalog configuration parameter

These functions use PostgreSQL's set_config() function internally to
set pgaudit GUC (Grand Unified Configuration) parameters at the session
level.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    For string inputs, generates a literal expression. For
    BaseExpression inputs, returns them unchanged.

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


# ============== Configuration Functions ==============

def pgaudit_set_role(
    dialect: "SQLDialectBase",
    role: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Set the pgaudit.role configuration parameter.

    Sets the role that pgaudit will use to determine which objects
    to audit. When an object is owned by the specified role, audit
    records will be generated for operations on that object.

    This function generates: SELECT set_config('pgaudit.role', role, false)

    The third parameter (false) means the setting applies only to the
    current session, not to the entire database.

    Args:
        dialect: The SQL dialect instance
        role: The role name to set for pgaudit auditing

    Returns:
        FunctionCall for set_config('pgaudit.role', role, false)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> pgaudit_set_role(dialect, 'audit_role')
        # Generates: set_config('pgaudit.role', 'audit_role', false)
    """
    return core.FunctionCall(
        dialect, "set_config",
        _convert_to_expression(dialect, "pgaudit.role"),
        _convert_to_expression(dialect, role),
        _convert_to_expression(dialect, "false"),
    )


def pgaudit_log_level(
    dialect: "SQLDialectBase",
    level: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Set the pgaudit.log_level configuration parameter.

    Sets the log level that pgaudit will use when writing audit
    records to the PostgreSQL log. This controls the severity
    level of the audit log messages.

    Supported log levels:
    - 'debug1' through 'debug5': Debug levels
    - 'info': Informational messages
    - 'notice': Notice messages
    - 'warning': Warning messages
    - 'log': Log messages (default)

    This function generates: SELECT set_config('pgaudit.log_level', level, false)

    Args:
        dialect: The SQL dialect instance
        level: The log level to set for pgaudit output

    Returns:
        FunctionCall for set_config('pgaudit.log_level', level, false)

    Example:
        >>> pgaudit_log_level(dialect, 'log')
        >>> pgaudit_log_level(dialect, 'notice')
        # Generates: set_config('pgaudit.log_level', 'notice', false)
    """
    return core.FunctionCall(
        dialect, "set_config",
        _convert_to_expression(dialect, "pgaudit.log_level"),
        _convert_to_expression(dialect, level),
        _convert_to_expression(dialect, "false"),
    )


def pgaudit_include_catalog(
    dialect: "SQLDialectBase",
    include: Union[bool, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Set the pgaudit.log_catalog configuration parameter.

    Controls whether pgaudit generates audit records for operations
    on catalog relations (pg_class, pg_attribute, etc.). When set
    to 'all', catalog relations are audited; when set to 'none',
    they are not.

    This function generates: SELECT set_config('pgaudit.log_catalog', 'all'/'none', false)

    Args:
        dialect: The SQL dialect instance
        include: Whether to include catalog relations in auditing.
                 When True (or 'all'), catalog relations are audited;
                 when False (or 'none'), they are not

    Returns:
        FunctionCall for set_config('pgaudit.log_catalog', 'all'/'none', false)

    Example:
        >>> pgaudit_include_catalog(dialect, True)
        # Generates: set_config('pgaudit.log_catalog', 'all', false)
        >>> pgaudit_include_catalog(dialect, False)
        # Generates: set_config('pgaudit.log_catalog', 'none', false)
    """
    if isinstance(include, bool):
        value = "all" if include else "none"
    elif isinstance(include, bases.BaseExpression):
        value = include
    else:
        value = include
    return core.FunctionCall(
        dialect, "set_config",
        _convert_to_expression(dialect, "pgaudit.log_catalog"),
        _convert_to_expression(dialect, value),
        _convert_to_expression(dialect, "false"),
    )


__all__ = [
    # Configuration functions
    "pgaudit_set_role",
    "pgaudit_log_level",
    "pgaudit_include_catalog",
]
