# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/foreign_table.py
"""PostgreSQL FOREIGN TABLE DDL protocol definition.

This module contains the :class:`PostgresForeignTableDDLSupport` protocol
which defines the interface for PostgreSQL's native CREATE/DROP FOREIGN TABLE.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.foreign_table import (
        PostgresCreateForeignTableExpression,
        PostgresDropForeignTableExpression,
    )


@runtime_checkable
class PostgresForeignTableDDLSupport(Protocol):
    """PostgreSQL FOREIGN TABLE DDL protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - CREATE FOREIGN TABLE: https://www.postgresql.org/docs/current/sql-createforeigntable.html
    - DROP FOREIGN TABLE:   https://www.postgresql.org/docs/current/sql-dropforeigntable.html

    Version Requirements:
    - CREATE/DROP FOREIGN TABLE: PostgreSQL 9.6+
    """

    def supports_foreign_table_ddl(self) -> bool:
        """Whether CREATE/DROP FOREIGN TABLE is supported (9.6+)."""
        ...

    def format_create_foreign_table_statement(
        self, expr: "PostgresCreateForeignTableExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CREATE FOREIGN TABLE`` statement.

        Args:
            expr: ``PostgresCreateForeignTableExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
        """
        ...

    def format_drop_foreign_table_statement(
        self, expr: "PostgresDropForeignTableExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``DROP FOREIGN TABLE`` statement.

        Args:
            expr: ``PostgresDropForeignTableExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
            ValueError: both ``cascade`` and ``restrict``.
        """
        ...