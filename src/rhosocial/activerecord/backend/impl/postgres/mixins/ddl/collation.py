# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/collation.py
"""PostgreSQL COLLATION object DDL implementation.

Implements CREATE/DROP COLLATION DDL formatting for the postgres dialect.
This is distinct from ``PostgresCollationMixin`` (mixins/collation.py),
which handles the ``COLLATE`` expression and nondeterministic-collation
capability detection.

Version Requirements:
- CREATE/DROP COLLATION: PostgreSQL 9.6+
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ...expression.ddl.collation import (
        PostgresCreateCollationExpression,
        PostgresDropCollationExpression,
    )


class PostgresCollationDDLMixin:
    """PostgreSQL COLLATION object DDL implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_collation_ddl(self) -> bool:
        """CREATE/DROP COLLATION require PostgreSQL 9.6+."""
        return self.version >= (9, 6, 0)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _format_collation_ref(self, schema: Optional[str], name: str) -> str:
        """Format ``name`` (optionally schema-qualified) as identifier(s)."""
        if schema:
            return (
                f"{self.format_identifier(schema)}."
                f"{self.format_identifier(name)}"
            )
        return self.format_identifier(name)

    # ------------------------------------------------------------------ #
    # CREATE COLLATION
    # ------------------------------------------------------------------ #
    def format_create_collation_ddl_statement(
        self, expr: "PostgresCreateCollationExpression"
    ) -> Tuple[str, tuple]:
        """Format a CREATE COLLATION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresCreateCollationExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.

        """
        if not self.supports_collation_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE COLLATION",
                suggestion="requires PostgreSQL 9.6+",
            )

        parts: List[str] = ["CREATE COLLATION"]
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")
        parts.append(self._format_collation_ref(expr.schema, expr.name))
        params_list = [
            ("LOCALE", expr.locale),
            ("LC_COLLATE", expr.lc_collate),
            ("LC_CTYPE", expr.lc_ctype),
            ("PROVIDER", expr.provider),
            ("VERSION", expr.version),
        ]
        clauses = []
        for keyword, value in params_list:
            if value is not None:
                clauses.append(f"{keyword} = {value}")
        if clauses:
            parts.append("(" + ", ".join(clauses) + ")")
        return " ".join(parts), ()
        if clauses:
            parts.append("(" + ", ".join(clauses) + ")")
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # DROP COLLATION
    # ------------------------------------------------------------------ #
    def format_drop_collation_ddl_statement(
        self, expr: "PostgresDropCollationExpression"
    ) -> Tuple[str, tuple]:
        """Format a DROP COLLATION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropCollationExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.
            ValueError: if both ``cascade`` and ``restrict`` are True.

        """
        if not self.supports_collation_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "DROP COLLATION",
                suggestion="requires PostgreSQL 9.6+",
            )
        if expr.cascade and expr.restrict:
            raise ValueError(
                "DROP COLLATION: CASCADE and RESTRICT are mutually exclusive"
            )
        parts: List[str] = ["DROP COLLATION"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_collation_ref(expr.schema, expr.name))
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()