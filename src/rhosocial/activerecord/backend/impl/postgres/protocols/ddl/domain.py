# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/domain.py
"""PostgreSQL DOMAIN protocol definition.

This module contains the :class:`PostgresDomainSupport` protocol which
defines the interface for PostgreSQL's native CREATE/ALTER/DROP DOMAIN DDL.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.domain import (
        PostgresAlterDomainExpression,
        PostgresCreateDomainExpression,
        PostgresDropDomainExpression,
    )


@runtime_checkable
class PostgresDomainSupport(Protocol):
    """PostgreSQL DOMAIN protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - CREATE DOMAIN: https://www.postgresql.org/docs/current/sql-createdomain.html
    - ALTER DOMAIN:  https://www.postgresql.org/docs/current/sql-alterdomain.html
    - DROP DOMAIN:   https://www.postgresql.org/docs/current/sql-dropdomain.html

    Version Requirements:
    - CREATE/ALTER/DROP DOMAIN: PostgreSQL 9.6+
    """

    def supports_create_domain(self) -> bool:
        """Whether CREATE/ALTER/DROP DOMAIN is supported (9.6+)."""
        ...

    def format_create_domain_statement(
        self, expr: "PostgresCreateDomainExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CREATE DOMAIN`` statement.

        Args:
            expr: ``PostgresCreateDomainExpression``.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
        """
        ...

    def format_postgres_alter_domain_statement(
        self, expr: "PostgresAlterDomainExpression"
    ) -> Tuple[str, tuple]:
        """Format an ``ALTER DOMAIN`` statement.

        Args:
            expr: ``PostgresAlterDomainExpression``.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
        """
        ...

    def format_drop_domain_statement(
        self, expr: "PostgresDropDomainExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``DROP DOMAIN`` statement.

        Args:
            expr: ``PostgresDropDomainExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
            ValueError: both ``cascade`` and ``restrict``.
        """
        ...