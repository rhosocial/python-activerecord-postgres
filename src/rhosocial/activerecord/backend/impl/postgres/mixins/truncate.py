# src/rhosocial/activerecord/backend/impl/postgres/mixins/truncate.py
"""PostgreSQL truncate feature support implementation."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Table

if TYPE_CHECKING:
    from ....expression.statements.ddl_truncate import TruncateExpression


class PostgresTruncateMixin:
    """PostgreSQL truncate override implementation.

    ``TRUNCATE`` is in the synopsis of every version this backend supports;
    only its options are version-gated. The live matrix (9.6 through
    19beta4) accepts the bare statement everywhere (measured).
    """

    def supports_truncate(self) -> bool:
        """``TRUNCATE`` itself is in the PostgreSQL synopsis.

        Version-independent: accepted by every server in this repository's
        matrix (9.6 through 19beta4, measured). The options keep their own
        probes below.
        """
        return True

    def supports_truncate_restart_identity(self) -> bool:
        return self.version >= (8, 4, 0)

    def supports_truncate_cascade(self) -> bool:
        return True

    def supports_truncate_restrict(self) -> bool:
        """``TRUNCATE ... RESTRICT`` is in the synopsis.

        PostgreSQL accepts ``TRUNCATE [ TABLE ] name [ RESTART IDENTITY |
        CONTINUE IDENTITY ] [ CASCADE | RESTRICT ]`` (measured on PostgreSQL
        16); RESTRICT is the default behavior but the token itself is legal.
        """
        return True

    def format_truncate_statement(self, expr: "TruncateExpression") -> Tuple[str, tuple]:
        """Format TRUNCATE statement for PostgreSQL.

        - ``expr.table`` — the table being truncated, which renders its own
          name and namespace.
        - ``expr.restart_identity`` / ``expr.continue_identity`` — add
          ``RESTART IDENTITY`` / ``CONTINUE IDENTITY`` (PG 8.4+); neither set
          renders neither token.
        - ``expr.cascade`` / ``expr.restrict`` — add ``CASCADE`` /
          ``RESTRICT``; neither set renders neither token.

        Raises:
            TypeError: ``expr.table`` is not a :class:`Table`. Another object kind
                would have had its own name rendered as the table's.
            UnsupportedFeatureError: If TRUNCATE itself is not supported by this
                version, or if the requested identity continuation or
                dependent-object behavior is not supported by this version.
        """
        if not isinstance(expr.table, Table):
            raise TypeError(
                f"TruncateExpression.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )
        if not self.supports_truncate():
            raise UnsupportedFeatureError(
                self.name,
                "TRUNCATE",
                f"{self.name} does not support TRUNCATE.",
            )
        parts = ["TRUNCATE TABLE", expr.table.to_sql()[0]]

        if expr.restart_identity or expr.continue_identity:
            if not self.supports_truncate_restart_identity():
                feature = (
                    "RESTART IDENTITY"
                    if expr.restart_identity
                    else "CONTINUE IDENTITY"
                )
                raise UnsupportedFeatureError(
                    self.name,
                    feature,
                    f"{self.name} does not support TRUNCATE ... {feature} "
                    f"for versions < 8.4."
                )
            parts.append("RESTART IDENTITY" if expr.restart_identity else "CONTINUE IDENTITY")

        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            if not self.supports_truncate_restrict():
                raise UnsupportedFeatureError(
                    self.name,
                    "TRUNCATE RESTRICT",
                    f"{self.name} does not support TRUNCATE ... RESTRICT.",
                )
            parts.append("RESTRICT")

        return " ".join(parts), ()
