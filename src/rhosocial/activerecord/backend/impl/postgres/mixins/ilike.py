# src/rhosocial/activerecord/backend/impl/postgres/mixins/ilike.py
"""PostgreSQL ilike feature support implementation."""


class PostgresILIKEMixin:
    """PostgreSQL ilike override implementation.

    All features are native, using version number for detection.
    """

    def supports_ilike(self) -> bool:
        return True

    def format_ilike_expression(self, column, pattern: str, negate: bool = False) -> tuple:
        """Format ILIKE expression for PostgreSQL.

        Args:
            column: Column name string or expression with ``to_sql()``.
            pattern: Right-hand-side pattern (replaced with ``%s`` placeholder).
            negate: If ``True``, produces ``NOT ILIKE`` instead of ``ILIKE``.

        Returns:
            Tuple of (SQL string, (pattern,) params tuple)

        """
        if isinstance(column, str):
            col_sql = self.format_identifier(column)
        else:
            col_sql, col_params = column.to_sql() if hasattr(column, "to_sql") else (str(column), ())

        if negate:
            sql = f"{col_sql} NOT ILIKE %s"
        else:
            sql = f"{col_sql} ILIKE %s"

        return sql, (pattern,)
