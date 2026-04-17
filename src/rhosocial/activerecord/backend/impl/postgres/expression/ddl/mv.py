# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/mv.py
"""
PostgreSQL DDL expressions: Materialized View operations.

PostgreSQL Documentation:
- REFRESH MATERIALIZED VIEW: https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html

Version Requirements:
- REFRESH MATERIALIZED VIEW: PostgreSQL 9.3+
- CONCURRENTLY option: PostgreSQL 9.4+
- WITH NO DATA: PostgreSQL 9.4+
"""

from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["RefreshMaterializedViewPgExpression"]


class RefreshMaterializedViewPgExpression(BaseExpression):
    """PostgreSQL REFRESH MATERIALIZED VIEW statement expression.

    Replaces the contents of a materialized view by recalculating its query.
    The data is replaced atomically without affecting concurrent queries.

    Attributes:
        name: Name of the materialized view to refresh.
        schema: Schema name for the materialized view.
        concurrently: Refresh without locks (PG 9.4+, requires unique index).
        with_data: Whether to repopulate data:
                 - True: Refresh with data (default)
                 - False: Refresh without data (PG 9.4+)
                 None: Use default behavior

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Regular refresh
        >>> refresh = RefreshMaterializedViewPgExpression(
        ...     dialect=dialect,
        ...     name="monthly_sales_summary",
        ... )
        >>> sql, params = refresh.to_sql()
        >>> sql
        "REFRESH MATERIALIZED VIEW monthly_sales_summary"

        >>> # Concurrent refresh (PG 9.4+, requires unique index)
        >>> refresh = RefreshMaterializedViewPgExpression(
        ...     dialect=dialect,
        ...     name="monthly_sales_summary",
        ...     concurrently=True,
        ... )

        >>> # Refresh without data (create empty, PG 9.4+)
        >>> refresh = RefreshMaterializedViewPgExpression(
        ...     dialect=dialect,
        ...     name="monthly_sales_summary",
        ...     with_data=False,
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        concurrently: bool = False,
        with_data: Optional[bool] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.concurrently = concurrently
        self.with_data = with_data
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate REFRESH MATERIALIZED VIEW SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_refresh_materialized_view_pg_statement(self)