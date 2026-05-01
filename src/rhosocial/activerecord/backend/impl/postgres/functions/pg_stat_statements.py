# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_stat_statements.py
"""
PostgreSQL pg_stat_statements Extension Functions.

This module provides SQL expression generators for PostgreSQL pg_stat_statements
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pg_stat_statements extension provides a means for tracking execution
statistics of all SQL statements executed by a running PostgreSQL server.
It is useful for identifying performance bottlenecks and analyzing query
patterns.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pgstatstatements.html

The pg_stat_statements extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

The pg_stat_statements extension requires shared preload libraries:
    shared_preload_libraries = 'pg_stat_statements'

Supported functions:
- pg_stat_statements_reset: Reset all statistics gathered by pg_stat_statements

Note:
    Most pg_stat_statements operations involve querying the pg_stat_statements
    view with specific SELECT patterns (sorting, filtering, limiting). These
    query patterns are handled by the PostgresPgStatStatementsMixin, not by
    function factories, since they generate complete SELECT statements rather
    than individual function calls.

    Only pg_stat_statements_reset() is a true function call that can be
    represented as a FunctionCall expression.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, etc.)
- They do not concatenate SQL strings directly
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression import core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ============== pg_stat_statements Functions ==============

def pg_stat_statements_reset(
    dialect: "SQLDialectBase",
) -> core.FunctionCall:
    """Generate SQL expression for resetting pg_stat_statements statistics.

    Discards all statistics gathered so far by pg_stat_statements.
    By default, this function can only be executed by superusers.

    Args:
        dialect: The SQL dialect instance

    Returns:
        FunctionCall for pg_stat_statements_reset()

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> pg_stat_statements_reset(dialect)
        # Generates: pg_stat_statements_reset()
    """
    return core.FunctionCall(dialect, "pg_stat_statements_reset")


__all__ = [
    # pg_stat_statements functions
    "pg_stat_statements_reset",
]
