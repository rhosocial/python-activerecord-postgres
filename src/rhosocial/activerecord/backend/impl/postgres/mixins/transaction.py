# src/rhosocial/activerecord/backend/impl/postgres/mixins/transaction.py
"""PostgreSQL transaction feature support implementation."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.transaction import (
        BeginTransactionExpression,
        SetTransactionExpression,
    )


class PostgresTransactionMixin:
    """PostgreSQL transaction override implementation.

    All features are native, using version number for detection.
    """

    def supports_transaction_mode(self) -> bool:
        return True

    def supports_isolation_level_in_begin(self) -> bool:
        return True

    def supports_read_only_transaction(self) -> bool:
        return True

    def supports_deferrable_transaction(self) -> bool:
        return True

    def supports_savepoint(self) -> bool:
        return True

    def format_begin_transaction(
        self, expr: "BeginTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format BEGIN TRANSACTION statement for PostgreSQL.

        PostgreSQL syntax:
        BEGIN [ ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE } ]
              [ { READ WRITE | READ ONLY } ]
              [ { NOT DEFERRABLE | DEFERRABLE } ]

        ``[NOT] DEFERRABLE`` is an independent transaction mode in the
        grammar; it is rendered whenever requested, not only alongside
        SERIALIZABLE (``BEGIN DEFERRABLE`` without an isolation level is
        accepted by the server -- measured on PostgreSQL 16).
        """
        params = expr.get_params()
        parts = ["BEGIN"]

        isolation = params.get("isolation_level")
        if isolation:
            level_str = self.get_isolation_level_name(isolation)
            parts.append(f"ISOLATION LEVEL {level_str}")

        mode = params.get("mode")
        if mode:
            mode_name = mode.name if hasattr(mode, "name") else str(mode)
            if mode_name == "READ_ONLY":
                parts.append("READ ONLY")
            elif mode_name == "READ_WRITE":
                parts.append("READ WRITE")

        if params.get("deferrable"):
            parts.append("DEFERRABLE")
        elif params.get("not_deferrable"):
            parts.append("NOT DEFERRABLE")

        return " ".join(parts), ()

    def format_set_transaction(
        self, expr: "SetTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format SET TRANSACTION statement for PostgreSQL.

        PostgreSQL supports setting transaction characteristics for the current
        transaction or for subsequent transactions.

        Syntax:
        SET TRANSACTION { ISOLATION LEVEL { ... } | { READ WRITE | READ ONLY } | [ NOT ] DEFERRABLE } [, ...]
        SET SESSION CHARACTERISTICS AS TRANSACTION { ... }
        """
        params = expr.get_params()
        parts = []

        if params.get("session"):
            parts.append("SET SESSION CHARACTERISTICS AS TRANSACTION")
        else:
            parts.append("SET TRANSACTION")

        options = []

        isolation = params.get("isolation_level")
        if isolation:
            level_str = self.get_isolation_level_name(isolation)
            options.append(f"ISOLATION LEVEL {level_str}")

        mode = params.get("mode")
        if mode:
            mode_name = mode.name if hasattr(mode, "name") else str(mode)
            if mode_name == "READ_ONLY":
                options.append("READ ONLY")
            elif mode_name == "READ_WRITE":
                options.append("READ WRITE")

        deferrable = params.get("deferrable")
        not_deferrable = params.get("not_deferrable")
        if deferrable:
            options.append("DEFERRABLE")
        elif not_deferrable:
            options.append("NOT DEFERRABLE")

        if options:
            parts.append(" ".join(options))

        return " ".join(parts), ()
