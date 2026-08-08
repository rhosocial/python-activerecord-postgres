# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/publication.py
"""PostgreSQL PUBLICATION / SUBSCRIPTION DDL implementation.

Implements CREATE/DROP PUBLICATION and CREATE/DROP SUBSCRIPTION for the
postgres dialect (logical replication).

Version Requirements:
- CREATE/DROP PUBLICATION: PostgreSQL 10+
- CREATE/DROP SUBSCRIPTION: PostgreSQL 10+
"""

from typing import List, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ...expression.ddl.publication import (
        PostgresCreatePublicationExpression,
        PostgresCreateSubscriptionExpression,
        PostgresDropPublicationExpression,
        PostgresDropSubscriptionExpression,
    )


class PostgresPublicationMixin:
    """PostgreSQL PUBLICATION / SUBSCRIPTION implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_publication(self) -> bool:
        """CREATE/DROP PUBLICATION require PostgreSQL 10+."""
        return self.version >= (10, 0, 0)

    def supports_subscription(self) -> bool:
        """CREATE/DROP SUBSCRIPTION require PostgreSQL 10+."""
        return self.version >= (10, 0, 0)

    # ------------------------------------------------------------------ #
    # CREATE PUBLICATION
    # ------------------------------------------------------------------ #
    def format_create_publication_statement(
        self, expr: "PostgresCreatePublicationExpression"
    ) -> Tuple[str, tuple]:
        """Format a CREATE PUBLICATION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresCreatePublicationExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 10.
            ValueError: if both ``tables`` and ``all_tables`` are given, or
                neither is.

        """
        if not self.supports_publication():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE PUBLICATION",
                suggestion="requires PostgreSQL 10+",
            )
        for_target = expr.tables is not None and len(expr.tables) > 0
        if expr.all_tables and expr.tables:
            raise ValueError(
                "CREATE PUBLICATION: tables and all_tables are mutually "
                "exclusive"
            )
        if not expr.all_tables and not for_target and expr.tables is None:
            # No table list at all: PostgreSQL requires either FOR TABLE or
            # FOR ALL TABLES in modern versions (defaults varied historically).
            raise ValueError(
                "CREATE PUBLICATION requires either tables or all_tables"
            )

        parts: List[str] = ["CREATE PUBLICATION", self.format_identifier(expr.name)]
        if expr.all_tables:
            parts.append("FOR ALL TABLES")
        elif for_target:
            quoted = ", ".join(self.format_identifier(t) for t in expr.tables)
            parts.append(f"FOR TABLE {quoted}")
        if expr.options:
            parts.append("WITH (" + ", ".join(expr.options) + ")")
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # DROP PUBLICATION
    # ------------------------------------------------------------------ #
    def format_drop_publication_statement(
        self, expr: "PostgresDropPublicationExpression"
    ) -> Tuple[str, tuple]:
        """Format a DROP PUBLICATION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropPublicationExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 10.

        """
        if not self.supports_publication():
            raise UnsupportedFeatureError(
                self.name,
                "DROP PUBLICATION",
                suggestion="requires PostgreSQL 10+",
            )
        if expr.cascade and expr.restrict:
            raise ValueError(
                "DROP PUBLICATION: CASCADE and RESTRICT are mutually exclusive"
            )
        parts: List[str] = ["DROP PUBLICATION"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.name))
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # CREATE SUBSCRIPTION
    # ------------------------------------------------------------------ #
    def format_create_subscription_statement(
        self, expr: "PostgresCreateSubscriptionExpression"
    ) -> Tuple[str, tuple]:
        """Format a CREATE SUBSCRIPTION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresCreateSubscriptionExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 10.

        """
        if not self.supports_subscription():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE SUBSCRIPTION",
                suggestion="requires PostgreSQL 10+",
            )
        pub_list = ", ".join(
            self.format_identifier(p) for p in expr.publications
        )
        parts: List[str] = [
            "CREATE SUBSCRIPTION",
            self.format_identifier(expr.name),
            f"CONNECTION '{expr.connection}'",
            f"PUBLICATION {pub_list}",
        ]
        if expr.options:
            parts.append("WITH (" + ", ".join(expr.options) + ")")
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # DROP SUBSCRIPTION
    # ------------------------------------------------------------------ #
    def format_drop_subscription_statement(
        self, expr: "PostgresDropSubscriptionExpression"
    ) -> Tuple[str, tuple]:
        """Format a DROP SUBSCRIPTION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropSubscriptionExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 10.

        """
        if not self.supports_subscription():
            raise UnsupportedFeatureError(
                self.name,
                "DROP SUBSCRIPTION",
                suggestion="requires PostgreSQL 10+",
            )
        parts: List[str] = ["DROP SUBSCRIPTION"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.name))
        if expr.cascade:
            parts.append("CASCADE")
        return " ".join(parts), ()