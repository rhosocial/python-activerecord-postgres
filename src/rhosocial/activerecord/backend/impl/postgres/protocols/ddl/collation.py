# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/collation.py
"""PostgreSQL COLLATION object DDL protocol definition.

This module contains the :class:`PostgresCollationDDLSupport` protocol.
It is distinct from the ``COLLATE``-expression support provided by the
existing ``PostgresCollationSupport``.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.collation import (
        PostgresCreateCollationExpression,
        PostgresDropCollationExpression,
    )


@runtime_checkable
class PostgresCollationDDLSupport(Protocol):
    """PostgreSQL COLLATION object DDL protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - CREATE COLLATION: https://www.postgresql.org/docs/current/sql-createcollation.html
    - DROP COLLATION:   https://www.postgresql.org/docs/current/sql-dropcollation.html

    Version Requirements:
    - CREATE/DROP COLLATION: PostgreSQL 9.6+
    """

    def supports_collation_ddl(self) -> bool:
        """Whether CREATE/DROP COLLATION is supported (9.6+)."""
        ...

    def format_create_collation_ddl_statement(
        self, expr: "PostgresCreateCollationExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CREATE COLLATION`` statement.

        Args:
            expr: ``PostgresCreateCollationExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
        """
        ...

    def format_drop_collation_ddl_statement(
        self, expr: "PostgresDropCollationExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``DROP COLLATION`` statement.

        Args:
            expr: ``PostgresDropCollationExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
            ValueError: both ``cascade`` and ``restrict``.
        """
        ...