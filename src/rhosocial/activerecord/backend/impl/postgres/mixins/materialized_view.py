# src/rhosocial/activerecord/backend/impl/postgres/mixins/materialized_view.py
from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import PostgresRefreshMaterializedViewExpression


class PostgresMaterializedViewMixin:
    """PostgreSQL materialized view extended features implementation."""

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """CONCURRENTLY is supported since PostgreSQL 9.4."""
        return self.version >= (9, 4, 0)

    def format_create_materialized_view_statement(self, expr) -> tuple:
        """Format CREATE MATERIALIZED VIEW statement for PostgreSQL.

        - ``expr.view_name`` — view name (identifier).
        - ``expr.column_aliases`` — optional list of column aliases.
        - ``expr.tablespace`` — optional tablespace.
        - ``expr.storage_options`` — optional dict of storage parameters (``WITH (… )``).
        - ``expr.query`` — source SELECT expression.
        - ``expr.with_data`` — ``WITH DATA`` / ``WITH NO DATA``.

        Args:
            expr: CreateMaterializedViewExpression instance

        Returns:
            Tuple of (SQL string, params tuple)

        """
        parts = ["CREATE MATERIALIZED VIEW"]
        parts.append(self.format_identifier(expr.view_name))

        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")

        if expr.tablespace and self.supports_materialized_view_tablespace():
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")

        if expr.storage_options and self.supports_materialized_view_storage_options():
            storage_parts = []
            for key, value in expr.storage_options.items():
                storage_parts.append(f"{key.upper()} = {value}")
            parts.append(f"WITH ({', '.join(storage_parts)})")

        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")

        if expr.with_data:
            parts.append("WITH DATA")
        else:
            parts.append("WITH NO DATA")

        return " ".join(parts), query_params

    def format_refresh_materialized_view_pg_statement(
        self, expr: "PostgresRefreshMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Format REFRESH MATERIALIZED VIEW statement with PG-specific options.

        Args:
            expr: PostgresRefreshMaterializedViewExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        full_name = f"{expr.schema}.{expr.view_name}" if expr.schema else expr.view_name

        sql = "REFRESH MATERIALIZED VIEW"

        if expr.concurrent:
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
