# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/collation.py
"""
PostgreSQL DDL expressions: COLLATION objects.

PostgreSQL Documentation:
- CREATE COLLATION: https://www.postgresql.org/docs/current/sql-createcollation.html
- ALTER COLLATION:  https://www.postgresql.org/docs/current/sql-altercollation.html
- DROP COLLATION:   https://www.postgresql.org/docs/current/sql-dropcollation.html

Version Requirements:
- CREATE/DROP COLLATION: PostgreSQL 9.6+ (all supported versions)
"""

from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresCreateCollationExpression", "PostgresDropCollationExpression"]


class PostgresCreateCollationExpression(BaseExpression):
    """PostgreSQL CREATE COLLATION statement expression.

    Defines a new collation object, typically from an OS locale.

    Attributes:
        name: Name of the collation.
        schema: Optional schema for the collation.
        locale: Optional ``LOCALE`` (provider-agnostic shorthand).
        lc_collate: Optional ``LC_COLLATE``.
        lc_ctype: Optional ``LC_CTYPE``.
        provider: Optional ``PROVIDER`` (e.g. ``libc`` or ``icu``).
        version: Optional ``VERSION``.
        if_not_exists: When True, add ``IF NOT EXISTS``.
        dialect_options: Reserved.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        locale: Optional[str] = None,
        lc_collate: Optional[str] = None,
        lc_ctype: Optional[str] = None,
        provider: Optional[str] = None,
        version: Optional[str] = None,
        if_not_exists: bool = False,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.locale = locale
        self.lc_collate = lc_collate
        self.lc_ctype = lc_ctype
        self.provider = provider
        self.version = version
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the CREATE COLLATION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_create_collation_ddl_statement(self)


class PostgresDropCollationExpression(BaseExpression):
    """PostgreSQL DROP COLLATION statement expression.

    Attributes:
        name: Name of the collation to drop.
        schema: Optional schema for the collation.
        if_exists: When True, add ``IF EXISTS``.
        cascade: When True, add ``CASCADE``.
        restrict: When True, add ``RESTRICT``. Mutually exclusive with
            ``cascade``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict

    def to_sql(self) -> "Tuple[str, tuple]":
        """Return the DROP COLLATION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_collation_ddl_statement(self)