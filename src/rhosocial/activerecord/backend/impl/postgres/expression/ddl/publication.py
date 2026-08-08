# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/publication.py
"""
PostgreSQL DDL expressions: PUBLICATION and SUBSCRIPTION (logical
replication).

PostgreSQL Documentation:
- CREATE PUBLICATION:    https://www.postgresql.org/docs/current/sql-createpublication.html
- DROP PUBLICATION:      https://www.postgresql.org/docs/current/sql-droppublication.html
- CREATE SUBSCRIPTION:   https://www.postgresql.org/docs/current/sql-createsubscription.html
- DROP SUBSCRIPTION:     https://www.postgresql.org/docs/current/sql-dropsubscription.html

Version Requirements:
- CREATE/DROP PUBLICATION: PostgreSQL 10+
- CREATE/DROP SUBSCRIPTION: PostgreSQL 10+
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresCreatePublicationExpression",
    "PostgresDropPublicationExpression",
    "PostgresCreateSubscriptionExpression",
    "PostgresDropSubscriptionExpression",
]


class PostgresCreatePublicationExpression(BaseExpression):
    """PostgreSQL CREATE PUBLICATION statement expression.

    Attributes:
        name: Name of the publication.
        tables: Optional list of table identifiers to publish with
            ``FOR TABLE``. Exactly one of ``tables`` or ``all_tables``
            should be given.
        all_tables: When True, publish all tables (``FOR ALL TABLES``).
        options: Optional list of ``WITH (...)`` options (e.g. ``publish`
            strings).
        dialect_options: Reserved.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        tables: Optional[List[str]] = None,
        all_tables: bool = False,
        options: Optional[List[str]] = None,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.tables = tables
        self.all_tables = all_tables
        self.options = options or []
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the CREATE PUBLICATION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_create_publication_statement(self)


class PostgresDropPublicationExpression(BaseExpression):
    """PostgreSQL DROP PUBLICATION statement expression.

    Attributes:
        name: Name of the publication to drop.
        if_exists: When True, add ``IF EXISTS``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        cascade: bool = False,
        if_exists: bool = False,
        restrict: bool = False,
    ):
        super().__init__(dialect)
        self.name = name
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict

    def to_sql(self) -> "Tuple[str, tuple]":
        """Return the DROP PUBLICATION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_publication_statement(self)


class PostgresCreateSubscriptionExpression(BaseExpression):
    """PostgreSQL CREATE SUBSCRIPTION statement expression.

    Attributes:
        name: Name of the subscription.
        connection: Connection string for ``CONNECTION '...'``.
        publications: Publication names to subscribe to.
        options: Optional ``WITH (``...)`` creation options.
        dialect_options: Reserved.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        connection: str,
        publications: List[str],
        options: Optional[List[str]] = None,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.connection = connection
        self.publications = publications
        self.options = options or []
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the CREATE SUBSCRIPTION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_create_subscription_statement(self)


class PostgresDropSubscriptionExpression(BaseExpression):
    """PostgreSQL DROP SUBSCRIPTION statement expression.

    Attributes:
        name: Name of the subscription to drop.
        if_exists: When True, add ``IF EXISTS``.
        cascade: When True, add ``CASCADE``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        if_exists: bool = False,
        cascade: bool = False,
    ):
        super().__init__(dialect)
        self.name = name
        self.if_exists = if_exists
        self.cascade = cascade

    def to_sql(self) -> "Tuple[str, tuple]":
        """Return the DROP SUBSCRIPTION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_subscription_statement(self)