# src/rhosocial/activerecord/backend/impl/postgres/mixins/truncate.py
"""PostgreSQL truncate feature support implementation."""

from typing import Tuple


class PostgresTruncateMixin:
    """PostgreSQL truncate override implementation.

    All features are native, using version number for detection.
    """

    def supports_truncate_restart_identity(self) -> bool:
        return self.version >= (8, 4, 0)

    def supports_truncate_cascade(self) -> bool:
        return True

    def format_truncate_statement(self, expr) -> Tuple[str, tuple]:
        """Format TRUNCATE statement for PostgreSQL.

        - ``expr.table_name`` — target table.
        - ``expr.restart_identity`` — add ``RESTART IDENTITY`` (PG 8.4+).
        - ``expr.cascade`` — add ``CASCADE``.
        """
        parts = ["TRUNCATE TABLE"]
        parts.append(self.format_identifier(expr.table_name))

        if expr.restart_identity and self.supports_truncate_restart_identity():
            parts.append("RESTART IDENTITY")

        if expr.cascade:
            parts.append("CASCADE")

        return " ".join(parts), ()
