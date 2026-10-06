# src/rhosocial/activerecord/backend/impl/postgres/mixins/truncate.py
"""PostgreSQL truncate feature support implementation."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Table

if TYPE_CHECKING:
    from ....expression.statements.ddl_truncate import TruncateExpression


class PostgresTruncateMixin:
    """PostgreSQL truncate override implementation.

    All features are native, using version number for detection.
    """

    def supports_truncate_restart_identity(self) -> bool:
        return self.version >= (8, 4, 0)

    def supports_truncate_cascade(self) -> bool:
        return True

    def format_truncate_statement(self, expr: "TruncateExpression") -> Tuple[str, tuple]:
        """Format TRUNCATE statement for PostgreSQL.

        - ``expr.table`` — the table being truncated, which renders its own
          name and namespace.
        - ``expr.restart_identity`` — add ``RESTART IDENTITY`` (PG 8.4+).
        - ``expr.cascade`` — add ``CASCADE``.

        Raises:
            TypeError: ``expr.table`` is not a :class:`Table`. Another object kind
                would have had its own name rendered as the table's.
        """
        if not isinstance(expr.table, Table):
            raise TypeError(
                f"TruncateExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )
        parts = ["TRUNCATE TABLE", expr.table.to_sql()[0]]

        if expr.restart_identity:
            if not self.supports_truncate_restart_identity():
                raise UnsupportedFeatureError(
                    self.name,
                    "RESTART IDENTITY",
                    f"{self.name} does not support TRUNCATE ... RESTART IDENTITY "
                    f"for versions < 8.4."
                )
            parts.append("RESTART IDENTITY")

        if expr.cascade:
            parts.append("CASCADE")

        return " ".join(parts), ()
