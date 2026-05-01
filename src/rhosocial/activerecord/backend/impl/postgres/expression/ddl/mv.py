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

from rhosocial.activerecord.backend.expression.statements.ddl_view import RefreshMaterializedViewExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresRefreshMaterializedViewExpression"]


class PostgresRefreshMaterializedViewExpression(RefreshMaterializedViewExpression):
    """PostgreSQL REFRESH MATERIALIZED VIEW statement expression.

    Replaces the contents of a materialized view by recalculating its query.
    The data is replaced atomically without affecting concurrent queries.

    Extends the generic RefreshMaterializedViewExpression with PostgreSQL-specific
    schema support and backward-compatible aliases.

    Attributes:
        view_name: Name of the materialized view to refresh (inherited).
        schema: Schema name for the materialized view (PostgreSQL-specific).
        concurrent: Whether to refresh concurrently (inherited).
        with_data: Whether to repopulate data (inherited).
        name: Alias for view_name (backward compatibility).
        concurrently: Alias for concurrent (backward compatibility).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Regular refresh
        >>> refresh = PostgresRefreshMaterializedViewExpression(
        ...     dialect=dialect,
        ...     name="monthly_sales_summary",
        ... )
        >>> sql, params = refresh.to_sql()
        >>> sql
        "REFRESH MATERIALIZED VIEW monthly_sales_summary"

        >>> # Concurrent refresh (PG 9.4+, requires unique index)
        >>> refresh = PostgresRefreshMaterializedViewExpression(
        ...     dialect=dialect,
        ...     name="monthly_sales_summary",
        ...     concurrently=True,
        ... )

        >>> # Refresh without data (create empty, PG 9.4+)
        >>> refresh = PostgresRefreshMaterializedViewExpression(
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
        super().__init__(
            dialect,
            view_name=name,
            concurrent=concurrently,
            with_data=with_data,
            dialect_options=dialect_options,
        )
        self.schema = schema

    @property
    def name(self):
        """Alias for view_name (backward compatibility)."""
        return self.view_name

    @property
    def concurrently(self):
        """Alias for concurrent (backward compatibility)."""
        return self.concurrent

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate REFRESH MATERIALIZED VIEW SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_refresh_materialized_view_pg_statement(self)