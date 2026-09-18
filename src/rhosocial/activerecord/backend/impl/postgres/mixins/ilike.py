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

    def format_ilike_expression(self, expr: "ILIKEExpression") -> Tuple[str, tuple]:
        """Format an ILIKE expression for PostgreSQL.

        Args:
            expr: The :class:`ILIKEExpression` node to render. The node stores
                its own ``column``, ``pattern`` and ``negate`` data; the
                formatter takes no extra arguments so it can be dispatched
                uniformly as ``formatter(expr)``.

        Returns:
            Tuple of (SQL string, (pattern,) params tuple)

        """
        col_sql, _ = expr.column.to_sql()

        placeholder = self.p()
        if expr.negate:
            sql = f"{col_sql} NOT ILIKE {placeholder}"
        else:
            sql = f"{col_sql} ILIKE {placeholder}"

        return sql, (expr.pattern,)
