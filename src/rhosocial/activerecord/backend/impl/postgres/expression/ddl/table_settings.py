# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/table_settings.py
"""
PostgreSQL DDL expressions: ALTER TABLE table-level settings.

PostgreSQL Documentation:
- ALTER TABLE: https://www.postgresql.org/docs/current/sql-altertable.html

Version Requirements:
- SET LOGGED / SET UNLOGGED: PostgreSQL 9.6+ (exists since 9.5)
- SET ACCESS METHOD: PostgreSQL 15+
"""

from enum import Enum
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "LoggingMode",
    "PostgresAlterTableSettingsExpression",
]


class LoggingMode(Enum):
    """Whether a table is WAL-logged or not.

    Switching between these with ``ALTER TABLE ... SET LOGGED`` /
    ``SET UNLOGGED`` is commonly used to prototype with fast UNLOGGED tables
    and migrate to LOGGED for production.
    """

    LOGGED = "LOGGED"
    UNLOGGED = "UNLOGGED"


class PostgresAlterTableSettingsExpression(BaseExpression):
    """PostgreSQL ``ALTER TABLE ... SET`` table-level settings.

    Attributes:
        table_name: Name of the target table.
        schema: Optional schema for the table.
        logging_mode: Optional ``LOGGED`` / ``UNLOGGED`` switch
            (PostgreSQL 9.6+, exists since 9.5).
        access_method: Optional new access method name for ``SET ACCESS
            METHOD`` (PostgreSQL 15+). Mutually exclusive with
            ``logging_mode``.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> expr = PostgresAlterTableSettingsExpression(
        ...     dialect, table_name="orders", mode=LoggingMode.UNLOGGED
        ... )
        >>> sql, params = expr.to_sql()  # doctest: +SKIP

    Raises:
        ValueError: if neither ``mode`` nor ``access_method`` is given, or
            if both are given.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: str,
        schema: Optional[str] = None,
        mode: Optional[LoggingMode] = None,
        access_method: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.table_name = table_name
        self.schema = schema
        self.mode = mode
        self.access_method = access_method
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the ALTER TABLE ... SET statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_alter_table_settings_statement(self)