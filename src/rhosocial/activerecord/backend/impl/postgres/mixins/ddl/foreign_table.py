# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/foreign_table.py
"""PostgreSQL FOREIGN TABLE DDL implementation.

Implements CREATE/DROP FOREIGN TABLE formatting for the postgres dialect.

Version Requirements:
- CREATE/DROP FOREIGN TABLE: PostgreSQL 9.6+
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import ForeignTable

if TYPE_CHECKING:
    from ...expression.ddl.foreign_table import (
        PostgresCreateForeignTableExpression,
        PostgresDropForeignTableExpression,
    )


class PostgresForeignTableMixin:
    """PostgreSQL FOREIGN TABLE implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_foreign_table_ddl(self) -> bool:
        """CREATE/DROP FOREIGN TABLE require PostgreSQL 9.6+."""
        return self.version >= (9, 6, 0)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _format_foreign_table_ref(
        self, schema: Optional[str], name: str
    ) -> str:
        """Render the foreign table a FOREIGN TABLE statement names.

        The name is a :class:`ForeignTable` schema object rendered by the
        dialect, so the qualification and quoting follow the shared rules
        instead of a second copy of them.
        """
        sql, _ = self.format_foreign_table_object(ForeignTable(self, name, schema_name=schema))
        return sql

    # ------------------------------------------------------------------ #
    # CREATE FOREIGN TABLE
    # ------------------------------------------------------------------ #
    def format_create_foreign_table_statement(
        self, expr: "PostgresCreateForeignTableExpression"
    ) -> Tuple[str, tuple]:
        """Format a CREATE FOREIGN TABLE statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresCreateForeignTableExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.

        """
        if not self.supports_foreign_table_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE FOREIGN TABLE",
                suggestion="requires PostgreSQL 9.6+",
            )

        parts: List[str] = ["CREATE FOREIGN TABLE"]
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")
        parts.append(self._format_foreign_table_ref(expr.schema, expr.name))
        if expr.columns:
            parts.append("(" + ", ".join(expr.columns) + ")")
        parts.append("SERVER")
        parts.append(self.format_identifier(expr.server_name))
        if expr.options:
            parts.append("OPTIONS (" + ", ".join(expr.options) + ")")
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # DROP FOREIGN TABLE
    # ------------------------------------------------------------------ #
    def format_drop_foreign_table_statement(
        self, expr: "PostgresDropForeignTableExpression"
    ) -> Tuple[str, tuple]:
        """Format a DROP FOREIGN TABLE statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropForeignTableExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.
            ValueError: if both ``cascade`` and ``restrict`` are True.

        """
        if not self.supports_foreign_table_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "DROP FOREIGN TABLE",
                suggestion="requires PostgreSQL 9.6+",
            )
        if expr.cascade and expr.restrict:
            raise ValueError(
                "DROP FOREIGN TABLE: CASCADE and RESTRICT are mutually "
                "exclusive"
            )
        parts: List[str] = ["DROP FOREIGN TABLE"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_foreign_table_ref(expr.schema, expr.name))
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()