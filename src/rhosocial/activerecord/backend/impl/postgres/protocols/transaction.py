# src/rhosocial/activerecord/backend/impl/postgres/protocols/transaction.py
"""PostgreSQL transaction feature support protocol."""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.transaction import (
        BeginTransactionExpression,
        SetTransactionExpression,
    )


@runtime_checkable
class PostgresTransactionSupport(Protocol):
    """PostgreSQL transaction feature support protocol."""

    def supports_transaction_mode(self)-> bool: ...

    def supports_isolation_level_in_begin(self)-> bool: ...

    def supports_read_only_transaction(self)-> bool: ...

    def supports_deferrable_transaction(self)-> bool: ...

    def supports_savepoint(self)-> bool: ...

    def format_begin_transaction(
        self, expr: "BeginTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format BEGIN TRANSACTION with PostgreSQL inline options.

        PostgreSQL syntax::

            BEGIN [ ISOLATION LEVEL { ... } ] [ { READ WRITE | READ ONLY } ]
                  [ { NOT DEFERRABLE | DEFERRABLE } ]

        Args:
            expr: BeginTransactionExpression carrying isolation level and mode.

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_set_transaction(
        self, expr: "SetTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format SET TRANSACTION / SET SESSION CHARACTERISTICS statement.

        Args:
            expr: SetTransactionExpression carrying transaction characteristics.

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...
