# src/rhosocial/activerecord/backend/impl/postgres/mixins/materialized_view.py
from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expressions import RefreshMaterializedViewPgExpression


class PostgresMaterializedViewMixin:
    """PostgreSQL materialized view extended features implementation."""

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """CONCURRENTLY is supported since PostgreSQL 9.4."""
        return self.version >= (9, 4, 0)

    def format_refresh_materialized_view_pg_statement(
        self, expr: "RefreshMaterializedViewPgExpression"
    ) -> Tuple[str, tuple]:
        """Format REFRESH MATERIALIZED VIEW statement with PG-specific options.

        Args:
            expr: RefreshMaterializedViewPgExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        full_name = f"{expr.schema}.{expr.name}" if expr.schema else expr.name

        sql = "REFRESH MATERIALIZED VIEW"

        if expr.concurrently:
            if not self.supports_materialized_view_concurrent_refresh():
                raise ValueError("CONCURRENTLY requires PostgreSQL 9.4+")
            sql += " CONCURRENTLY"

        sql += f" {full_name}"

        if expr.with_data is not None:
            if expr.with_data:
                sql += " WITH DATA"
            else:
                sql += " WITH NO DATA"

        return sql, ()
