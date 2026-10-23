# src/rhosocial/activerecord/backend/impl/postgres/mixins/transaction.py
"""PostgreSQL transaction feature support implementation."""

from typing import Any, Dict, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.transaction import (
        BeginTransactionStatement,
        SetTransactionStatement,
    )
    from rhosocial.activerecord.backend.impl.postgres.expression.transaction import (
        PostgresSetConstraintsStatement,
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

    def supports_transaction_wait(self) -> bool:
        """Whether ``WAIT`` / ``NO WAIT`` can be spelled on a transaction.

        PostgreSQL has no lock-wait clause on ``BEGIN`` or ``SET
        TRANSACTION``: ``BEGIN NO WAIT`` and ``SET TRANSACTION NO WAIT`` are
        both syntax errors (measured on 9.6 through 19beta4), so the pair is
        refused by name rather than dropped. The clause belongs to Firebird's
        grammar.
        """
        return False

    def supports_savepoint(self) -> bool:
        return True

    def _refuse_transaction_wait(self, params: Dict[str, Any]) -> None:
        """Refuse the WAIT / NO WAIT pair by name when the probe declines it.

        The pair has no spelling on this dialect, so a request must fail
        closed -- the same shape as every other probe-gated clause -- instead
        of being silently dropped from the rendered statement.
        """
        wait = params.get("wait")
        no_wait = params.get("no_wait")
        if (wait or no_wait) and not self.supports_transaction_wait():
            feature = "WAIT" if wait else "NO WAIT"
            raise UnsupportedFeatureError(
                self.name,
                feature,
                f"{self.name} has no {feature} transaction spelling; "
                f"WAIT / NO WAIT is Firebird grammar.",
            )

    def format_begin_transaction_statement(
        self, expr: "BeginTransactionStatement"
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

        ``WAIT`` / ``NO WAIT`` has no PostgreSQL spelling; the pair is refused
        by name through :meth:`supports_transaction_wait`.
        """
        params = expr.get_params()
        self._refuse_transaction_wait(params)
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

    def format_set_transaction_statement(
        self, expr: "SetTransactionStatement"
    ) -> Tuple[str, tuple]:
        """Format SET TRANSACTION statement for PostgreSQL.

        PostgreSQL supports setting transaction characteristics for the current
        transaction or for subsequent transactions.

        Syntax:
        SET TRANSACTION { ISOLATION LEVEL { ... } | { READ WRITE | READ ONLY } | [ NOT ] DEFERRABLE } [, ...]
        SET SESSION CHARACTERISTICS AS TRANSACTION { ... }

        ``WAIT`` / ``NO WAIT`` has no PostgreSQL spelling; the pair is refused
        by name through :meth:`supports_transaction_wait`.
        """
        params = expr.get_params()
        self._refuse_transaction_wait(params)
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

    def format_postgres_set_constraints_statement(
        self, expr: "PostgresSetConstraintsStatement"
    ) -> Tuple[str, tuple]:
        """Format SET CONSTRAINTS for PostgreSQL.

        Syntax::

            SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }

        The constraint name is an identifier, so it is rendered through this
        dialect's identifier quoting: the server parses it as a name, a bind
        parameter cannot stand in for it, and a name written into the text as it
        stood either broke the statement or escaped it.

        Args:
            expr: Set-constraints statement expression with the constraint name.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        constraint = self.format_identifier(params.get("constraint_name"))
        return f"SET CONSTRAINTS {constraint} DEFERRED", ()
