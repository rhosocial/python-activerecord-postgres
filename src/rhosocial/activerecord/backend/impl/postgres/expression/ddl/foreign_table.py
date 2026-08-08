# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/foreign_table.py
"""
PostgreSQL DDL expressions: FOREIGN TABLE.

PostgreSQL Documentation:
- CREATE FOREIGN TABLE: https://www.postgresql.org/docs/current/sql-createforeigntable.html
- DROP FOREIGN TABLE:   https://www.postgresql.org/docs/current/sql-dropforeigntable.html

Version Requirements:
- CREATE/DROP FOREIGN TABLE: PostgreSQL 9.6+
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresCreateForeignTableExpression", "PostgresDropForeignTableExpression"]


class PostgresCreateForeignTableExpression(BaseExpression):
    """PostgreSQL CREATE FOREIGN TABLE statement expression.

    Defines a foreign table backed by a foreign server (FDW).

    Attributes:
        name: Name of the foreign table.
        schema: Optional schema for the table.
        columns: Optional list of column definition strings.
        server_name: Name of the foreign server to bind.
        options: Optional ``OPTIONS ( option 'value', ... )`` list of
            ``"option 'value'"`` strings.
        if_not_exists: When True, add ``IF NOT EXISTS``.
        dialect_options: Reserved.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        server_name: str,
        schema: Optional[str] = None,
        columns: Optional[List[str]] = None,
        options: Optional[List[str]] = None,
        if_not_exists: bool = False,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.server_name = server_name
        self.columns = columns or []
        self.options = options or []
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the CREATE FOREIGN TABLE statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_create_foreign_table_statement(self)


class PostgresDropForeignTableExpression(BaseExpression):
    """PostgreSQL DROP FOREIGN TABLE statement expression.

    Attributes:
        name: Name of the foreign table to drop.
        schema: Optional schema for the foreign table.
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
        """Return the DROP FOREIGN TABLE statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_foreign_table_statement(self)