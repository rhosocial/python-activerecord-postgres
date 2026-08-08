# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/domain.py
"""
PostgreSQL DDL expressions: DOMAIN operations.

PostgreSQL Documentation:
- CREATE DOMAIN: https://www.postgresql.org/docs/current/sql-createdomain.html
- ALTER DOMAIN:  https://www.postgresql.org/docs/current/sql-alterdomain.html
- DROP DOMAIN:   https://www.postgresql.org/docs/current/sql-dropdomain.html

Version Requirements:
- CREATE/ALTER/DROP DOMAIN: PostgreSQL 9.6+ (all supported versions)
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "AlterDomainActionType",
    "PostgresCreateDomainExpression",
    "PostgresAlterDomainExpression",
    "PostgresDropDomainExpression",
]


class AlterDomainActionType(Enum):
    """Actions supported by ALTER DOMAIN (minimal subset)."""

    SET_DEFAULT = "SET DEFAULT"
    DROP_DEFAULT = "DROP DEFAULT"
    RENAME_TO = "RENAME TO"


class PostgresCreateDomainExpression(BaseExpression):
    """PostgreSQL CREATE DOMAIN statement expression.

    Creates a new domain — a user-defined data type — over an underlying
    PostgreSQL type, often reusing types and default/constraint logic.

    Attributes:
        name: Name of the domain.
        data_type: Underlying PostgreSQL type name (e.g. ``NUMERIC(10, 2)``).
        schema: Optional schema for the domain.
        collation: Optional collation name.
        default: Optional literal default value expression.
        constraints: Optional list of constraint clauses (e.g. ``CHECK (...)``).
        dialect_options: Reserved for extensions.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        data_type: str,
        schema: Optional[str] = None,
        collation: Optional[str] = None,
        default: Optional[str] = None,
        constraints: Optional[List[str]] = None,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.data_type = data_type
        self.schema = schema
        self.collation = collation
        self.default = default
        self.constraints = constraints or []
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate CREATE DOMAIN SQL statement.

        Returns:
            Tuple of (SQL string, params tuple).

        """
        return self.dialect.format_create_domain_statement(self)


class PostgresAlterDomainExpression(BaseExpression):
    """PostgreSQL ALTER DOMAIN statement expression (minimal subset).

    Attributes:
        name: Name of the domain to alter.
        schema: Optional schema for the domain.
        action: The action to perform (:class:`AlterDomainActionType`).
        new_value: For ``SET DEFAULT``, the new literal default. Otherwise
            unused.
        new_name: For ``RENAME TO``, the new domain name.
        dialect_options: Reserved.

    Returns:
        Formatting is delegated to
        ``dialect.format_alter_domain_statement``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        action: AlterDomainActionType,
        schema: Optional[str] = None,
        new_value: Any = None,
        new_name: Optional[str] = None,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.action = action
        self.new_value = new_value
        self.new_name = new_name
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the ALTER DOMAIN SQL statement.

        Returns:
            Tuple of (SQL string, params tuple).

        """
        return self.dialect.format_postgres_alter_domain_statement(self)


class PostgresDropDomainExpression(BaseExpression):
    """PostgreSQL DROP DOMAIN statement expression.

    Attributes:
        name: Name of the domain to drop.
        schema: Optional schema for the domain.
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
        """Generate the DROP DOMAIN SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_domain_statement(self)