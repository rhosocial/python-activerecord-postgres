# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/routine.py
"""PostgreSQL FUNCTION / AGGREGATE routine DDL protocol definition.

This module contains the :class:`PostgresRoutineDDLSupport` protocol.
CREATE/DROP PROCEDURE is covered by the existing
``PostgresStoredProcedureSupport``.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.routine import (
        PostgresCreateAggregateExpression,
        PostgresCreateFunctionExpression,
        PostgresDropAggregateExpression,
        PostgresDropFunctionExpression,
    )


@runtime_checkable
class PostgresRoutineDDLSupport(Protocol):
    """PostgreSQL FUNCTION / AGGREGATE routine DDL protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - CREATE FUNCTION:  https://www.postgresql.org/docs/current/sql-createfunction.html
    - DROP FUNCTION:    https://www.postgresql.org/docs/current/sql-dropfunction.html
    - CREATE AGGREGATE: https://www.postgresql.org/docs/current/sql-createaggregate.html
    - DROP AGGREGATE:   https://www.postgresql.org/docs/current/sql-dropaggregate.html

    Version Requirements:
    - CREATE/DROP FUNCTION: PostgreSQL 9.6+
    - CREATE/DROP AGGREGATE: PostgreSQL 9.6+
    """

    def supports_function_ddl(self) -> bool:
        """Whether CREATE/DROP FUNCTION is supported (9.6+)."""
        ...

    def supports_aggregate_ddl(self) -> bool:
        """Whether CREATE/DROP AGGREGATE is supported (9.6+)."""
        ...

    def format_create_function_ddl_statement(
        self, expr: "PostgresCreateFunctionExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CREATE FUNCTION`` statement.

        Args:
            expr: ``PostgresCreateFunctionExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        ...

    def format_drop_function_ddl_statement(
        self, expr: "PostgresDropFunctionExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``DROP FUNCTION`` statement."""
        ...

    def format_create_aggregate_ddl_statement(
        self, expr: "PostgresCreateAggregateExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CREATE AGGREGATE`` statement."""
        ...

    def format_drop_aggregate_ddl_statement(
        self, expr: "PostgresDropAggregateExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``DROP AGGREGATE`` statement."""
        ...