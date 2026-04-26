# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/extension.py
"""
PostgreSQL DDL expressions: Extension operations.

PostgreSQL Documentation:
- CREATE EXTENSION: https://www.postgresql.org/docs/current/sql-createextension.html
- DROP EXTENSION: https://www.postgresql.org/docs/current/sql-dropextension.html

Version Requirements:
- CREATE EXTENSION: PostgreSQL 9.1+
- DROP EXTENSION: PostgreSQL 9.1+
"""

from typing import Optional, Dict, Any, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresCreateExtensionExpression",
    "PostgresDropExtensionExpression",
]


class PostgresCreateExtensionExpression(BaseExpression):
    """PostgreSQL CREATE EXTENSION statement expression.

    Installs a PostgreSQL extension into the database.

    Attributes:
        name: Name of the extension to create.
        schema: Schema to install extension into (default: public).
        version: Specific version to install.
        if_not_exists: Add IF NOT EXISTS clause.
        cascade: Cascade to dependent extensions.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> create_ext = PostgresCreateExtensionExpression(
        ...     dialect=dialect,
        ...     name="uuid-ossp",
        ... )
        >>> sql, params = create_ext.to_sql()
        >>> sql
        'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'

        >>> # With schema and version
        >>> create_ext = PostgresCreateExtensionExpression(
        ...     dialect=dialect,
        ...     name="postgis",
        ...     schema="public",
        ...     version="3.4.0",
        ...     if_not_exists=True,
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        version: Optional[str] = None,
        if_not_exists: bool = True,
        cascade: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.version = version
        self.if_not_exists = if_not_exists
        self.cascade = cascade
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate CREATE EXTENSION SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_create_extension(self)


class PostgresDropExtensionExpression(BaseExpression):
    """PostgreSQL DROP EXTENSION statement expression.

    Removes an extension from the database.

    Attributes:
        name: Name of the extension to drop.
        schema: Schema containing the extension.
        if_exists: Add IF EXISTS clause (prevent error if not exists).
        cascade: Drop dependent objects (CASCADE).
        restrict: Drop only if no dependent objects (RESTRICT).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> drop_ext = PostgresDropExtensionExpression(
        ...     dialect=dialect,
        ...     name="uuid-ossp",
        ...     if_exists=True,
        ...     cascade=True,
        ... )
        >>> sql, params = drop_ext.to_sql()
        >>> sql
        'DROP EXTENSION IF EXISTS "uuid-ossp" CASCADE'
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = True,
        cascade: bool = False,
        restrict: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate DROP EXTENSION SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_drop_extension(self)