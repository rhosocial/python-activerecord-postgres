# src/rhosocial/activerecord/backend/impl/postgres/mixins/ilike.py
"""PostgreSQL ilike feature support implementation."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.impl.postgres.expression.ilike import (
        ILIKEExpression,
    )


class PostgresILIKEMixin:
    """PostgreSQL ilike override implementation.

    All features are native, using version number for detection.
    """

    def supports_ilike(self) -> bool:
        return True

    def format_ilike_expression(self, column, pattern: str = "", negate: bool = False) -> tuple:
        """Format ILIKE expression for PostgreSQL.

        Accepts either raw parameters or an ILIKEExpression object.

        Args:
            column: Column name string, expression with ``to_sql()``,
                    or an :class:`ILIKEExpression` instance.
            pattern: Right-hand-side pattern (replaced with ``%s`` placeholder).
                     Ignored when *column* is an ``ILIKEExpression``.
            negate: If ``True``, produces ``NOT ILIKE`` instead of ``ILIKE``.
                    Ignored when *column* is an ``ILIKEExpression``.

        Returns:
            Tuple of (SQL string, (pattern,) params tuple)

        """
        from rhosocial.activerecord.backend.impl.postgres.expression.ilike import (
            ILIKEExpression,
        )

        if isinstance(column, ILIKEExpression):
            expr = column
            column = expr.column
            pattern = expr.pattern
            negate = expr.negate

        if isinstance(column, str):
            col_sql = self.format_identifier(column)
        else:
            col_sql, col_params = column.to_sql() if hasattr(column, "to_sql") else (str(column), ())

        if negate:
            sql = f"{col_sql} NOT ILIKE %s"
        else:
            sql = f"{col_sql} ILIKE %s"

        return sql, (pattern,)
