# src/rhosocial/activerecord/backend/impl/postgres/functions/schema.py
"""
PostgreSQL schema resolution functions.

This module provides SQL expression generators for asking the server which
schema an unqualified reference resolves against.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall)
- They do not concatenate SQL strings directly

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-info.html
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import core

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def current_schema(dialect: "SQLDialectBase") -> "core.FunctionCall":
    """Create a CURRENT_SCHEMA function call.

    Returns the first schema in ``search_path`` that actually exists, which
    is the schema an unqualified reference resolves against. The search path
    may name schemas that do not exist, so this walks past them.

    The server returns NULL when ``search_path`` resolves to no existing
    schema. Callers must treat that as "no current schema" rather than an
    error, so this factory does not reject or default the value.

    Usage:
        - current_schema(dialect) -> current_schema()

    Args:
        dialect: The SQL dialect instance

    Returns:
        A FunctionCall instance representing CURRENT_SCHEMA()
    """
    return core.FunctionCall(dialect, "current_schema")
