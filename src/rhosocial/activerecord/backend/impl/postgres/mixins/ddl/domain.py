# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/domain.py
"""PostgreSQL DOMAIN DDL implementation.

Implements CREATE/ALTER/DROP DOMAIN formatting for the postgres dialect.

Version Requirements:
- CREATE/ALTER/DROP DOMAIN: PostgreSQL 9.6+
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from ...expression.ddl.domain import AlterDomainActionType

if TYPE_CHECKING:
    from ...expression.ddl.domain import (
        PostgresAlterDomainExpression,
        PostgresCreateDomainExpression,
        PostgresDropDomainExpression,
    )


class PostgresDomainMixin:
    """PostgreSQL DOMAIN implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_create_domain(self) -> bool:
        """CREATE/ALTER/DROP DOMAIN require PostgreSQL 9.6+."""
        return self.version >= (9, 6, 0)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _format_domain_ref(self, schema: Optional[str], name: str) -> str:
        """Format ``name`` (optionally schema-qualified) as identifier(s)."""
        if schema:
            return f"{self.format_identifier(schema)}.{self.format_identifier(name)}"
        return self.format_identifier(name)

    # ------------------------------------------------------------------ #
    # CREATE DOMAIN
    # ------------------------------------------------------------------ #
    def format_create_domain_statement(
        self, expr: "PostgresCreateDomainExpression"
    ) -> Tuple[str, tuple]:
        """Format a CREATE DOMAIN statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresCreateDomainExpression`.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.

        """
        if not self.supports_create_domain():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE DOMAIN",
                suggestion="requires PostgreSQL 9.6+",
            )

        parts: List[str] = [
            "CREATE DOMAIN",
            self._format_domain_ref(expr.schema, expr.name),
            "AS",
            expr.data_type,
        ]
        if expr.collation:
            parts.append(f"COLLATE {expr.collation}")
        if expr.default is not None:
            parts.append(f"DEFAULT {expr.default}")
        for constraint in expr.constraints:
            parts.append(constraint)
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # ALTER DOMAIN
    # ------------------------------------------------------------------ #
    def format_postgres_alter_domain_statement(
        self, expr: "PostgresAlterDomainExpression"
    ) -> Tuple[str, tuple]:
        """Format an ALTER DOMAIN statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresAlterDomainExpression`.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.

        """
        if not self.supports_create_domain():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER DOMAIN",
                suggestion="requires PostgreSQL 9.6+",
            )

        base = f"ALTER DOMAIN {self._format_domain_ref(expr.schema, expr.name)}"
        action = expr.action
        if action is AlterDomainActionType.SET_DEFAULT:
            return f"{base} SET DEFAULT {expr.new_value}", ()
        if action is AlterDomainActionType.DROP_DEFAULT:
            return f"{base} DROP DEFAULT", ()
        if action is AlterDomainActionType.RENAME_TO:
            return (
                f"{base} RENAME TO {self.format_identifier(expr.new_name)}",
                (),
            )
        raise ValueError(f"Unsupported ALTER DOMAIN action: {action}")

    # ------------------------------------------------------------------ #
    # DROP DOMAIN
    # ------------------------------------------------------------------ #
    def format_drop_domain_statement(
        self, expr: "PostgresDropDomainExpression"
    ) -> Tuple[str, tuple]:
        """Format a DROP DOMAIN statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropDomainExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.
            ValueError: if both ``cascade`` and ``restrict`` are True.

        """
        if not self.supports_create_domain():
            raise UnsupportedFeatureError(
                self.name,
                "DROP DOMAIN",
                suggestion="requires PostgreSQL 9.6+",
            )
        if expr.cascade and expr.restrict:
            raise ValueError(
                "DROP DOMAIN: CASCADE and RESTRICT are mutually exclusive"
            )
        parts: List[str] = ["DROP DOMAIN"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_domain_ref(expr.schema, expr.name))
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()