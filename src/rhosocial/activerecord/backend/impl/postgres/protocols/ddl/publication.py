# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/publication.py
"""PostgreSQL PUBLICATION / SUBSCRIPTION protocol definition.

This module contains the :class:`PostgresPublicationSupport` protocol which
defines the interface for PostgreSQL's logical replication PUBLICATION and
SUBSCRIPTION DDL commands.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.publication import (
        PostgresCreatePublicationExpression,
        PostgresCreateSubscriptionExpression,
        PostgresDropPublicationExpression,
        PostgresDropSubscriptionExpression,
    )


@runtime_checkable
class PostgresPublicationSupport(Protocol):
    """PostgreSQL PUBLICATION / SUBSCRIPTION protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - CREATE PUBLICATION: https://www.postgresql.org/docs/current/sql-createpublication.html
    - DROP PUBLICATION:   https://www.postgresql.org/docs/current/sql-droppublication.html
    - CREATE SUBSCRIPTION: https://www.postgresql.org/docs/current/sql-createsubscription.html
    - DROP SUBSCRIPTION:  https://www.postgresql.org/docs/current/sql-dropsubscription.html

    Version Requirements:
    - CREATE/DROP PUBLICATION: PostgreSQL 10+
    - CREATE/DROP SUBSCRIPTION: PostgreSQL 10+
    """

    def supports_publication(self) -> bool:
        """Whether CREATE/DROP PUBLICATION is supported (10+)."""
        ...

    def supports_subscription(self) -> bool:
        """Whether CREATE/DROP SUBSCRIPTION is supported (10+)."""
        ...

    def format_create_publication_statement(
        self, expr: "PostgresCreatePublicationExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CREATE PUBLICATION`` statement."""
        ...

    def format_drop_publication_statement(
        self, expr: "PostgresDropPublicationExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``DROP PUBLICATION`` statement."""
        ...

    def format_create_subscription_statement(
        self, expr: "PostgresCreateSubscriptionExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CREATE SUBSCRIPTION`` statement."""
        ...

    def format_drop_subscription_statement(
        self, expr: "PostgresDropSubscriptionExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``DROP SUBSCRIPTION`` statement."""
        ...