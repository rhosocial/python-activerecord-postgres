# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/statistics.py
"""
PostgreSQL DDL expressions: Statistics operations.

PostgreSQL Documentation:
- CREATE STATISTICS: https://www.postgresql.org/docs/current/sql-createstatistics.html
- DROP STATISTICS: https://www.postgresql.org/docs/current/sql-dropstatistics.html

Version Requirements:
- Extended statistics: PostgreSQL 10+
- ndistinct coefficient: PostgreSQL 10+
- MCV lists: PostgreSQL 12+
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["CreateStatisticsExpression", "DropStatisticsExpression"]


class CreateStatisticsExpression(BaseExpression):
    """PostgreSQL CREATE STATISTICS statement expression.

    Creates extended statistics for columns to improve query planning for complex queries.
    Supports ndistinct, dependencies, and MCV (most-common value) statistics.

    Attributes:
        name: Name of the statistics object.
        columns: List of columns to include in statistics.
        table_name: Name of the table containing the columns.
        schema: Schema name for the statistics.
        statistics_type: Type of statistics: 'ndistinct', 'dependencies', or 'mcv'.
                    Multiple types can be combined (e.g., 'ndistinct, mcv').
        if_not_exists: Add IF NOT EXISTS clause.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Create ndistinct statistics
        >>> stats = CreateStatisticsExpression(
        ...     dialect=dialect,
        ...     name="users_status_expr",
        ...     columns=["status", "category"],
        ...     table_name="users",
        ...     statistics_type="ndistinct",
        ... )
        >>> sql, params = stats.to_sql()
        >>> sql
        "CREATE STATISTICS users_status_expr ON (status, category) FROM users"

        >>> # Create multiple statistics types (PG 12+)
        >>> stats = CreateStatisticsExpression(
        ...     dialect=dialect,
        ...     name="order_stats",
        ...     columns=["customer_id", "status", "region"],
        ...     table_name="orders",
        ...     statistics_type="ndistinct, mcv",
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        columns: List[str],
        table_name: str,
        schema: Optional[str] = None,
        statistics_type: Optional[str] = None,
        if_not_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.columns = columns
        self.table_name = table_name
        self.schema = schema
        self.statistics_type = statistics_type
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate CREATE STATISTICS SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_create_statistics_statement(self)


class DropStatisticsExpression(BaseExpression):
    """PostgreSQL DROP STATISTICS statement expression.

    Drops an extended statistics object.

    Attributes:
        name: Name of the statistics object to drop.
        schema: Schema name for the statistics.
        if_exists: Add IF EXISTS clause (prevent error if not exists).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> drop = DropStatisticsExpression(
        ...     dialect=dialect,
        ...     name="users_status_expr",
        ... )
        >>> sql, params = drop.to_sql()
        >>> sql
        "DROP STATISTICS users_status_expr"
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate DROP STATISTICS SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_drop_statistics_statement(self)