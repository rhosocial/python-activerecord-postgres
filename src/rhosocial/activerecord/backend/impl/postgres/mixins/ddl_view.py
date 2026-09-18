# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl_view.py
"""PostgreSQL view feature support implementation."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.statements.ddl_view import CreateViewExpression


class PostgresViewMixin:
    """PostgreSQL view feature support implementation."""

    def supports_or_replace_view(self) -> bool:
        return True

    def supports_create_or_replace_view(self) -> bool:
        return True

    def supports_if_not_exists_view(self) -> bool:
        """PostgreSQL does not support IF NOT EXISTS for views."""
        return False

    def supports_temporary_view(self) -> bool:
        return True

    def supports_if_exists_view(self) -> bool:
        return True

    def supports_view_check_option(self) -> bool:
        return True

    def supports_cascade_view(self) -> bool:
        return True

    def supports_materialized_view(self) -> bool:
        return True

    def supports_refresh_materialized_view(self) -> bool:
        return True

    def supports_materialized_view_tablespace(self) -> bool:
        return True

    def supports_materialized_view_storage_options(self) -> bool:
        return True

    def format_create_view_statement(self, expr: "CreateViewExpression") -> Tuple[str, tuple]:
        """Format CREATE VIEW statement for PostgreSQL.

        - ``expr.temporary`` — add ``TEMPORARY``.
        - ``expr.replace`` — add ``OR REPLACE``.
        - ``expr.view_name`` — view name (identifier).
        - ``expr.column_aliases`` — optional list of column aliases.
        - ``expr.query`` — source SELECT expression.
        - ``expr.options.check_option`` — ``WITH {LOCAL|CASCADED} CHECK OPTION``.

        Args:
            expr: CreateViewExpression instance

        Returns:
            Tuple of (SQL string, params tuple)

        """
        parts = ["CREATE"]

        if expr.temporary:
            parts.append("TEMPORARY")

        if expr.replace and self.supports_create_or_replace_view():
            parts.append("OR REPLACE")

        parts.append("VIEW")
        parts.append(self.format_identifier(expr.view_name))

        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")

        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")

        if expr.options and expr.options.check_option:
            check_option = expr.options.check_option.value
            parts.append(f"WITH {check_option} CHECK OPTION")

        return " ".join(parts), query_params
