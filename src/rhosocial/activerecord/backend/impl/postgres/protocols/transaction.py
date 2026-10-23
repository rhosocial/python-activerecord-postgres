# src/rhosocial/activerecord/backend/impl/postgres/protocols/transaction.py
"""PostgreSQL transaction feature support protocol."""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.transaction import (
        BeginTransactionStatement,
        SetTransactionStatement,
    )
    from rhosocial.activerecord.backend.impl.postgres.expression.transaction import (
        PostgresSetConstraintsStatement,
    )


@runtime_checkable
class PostgresTransactionSupport(Protocol):
    """PostgreSQL transaction feature support protocol."""

    def supports_transaction_mode(self)-> bool: ...

    def supports_isolation_level_in_begin(self)-> bool: ...

    def supports_read_only_transaction(self)-> bool: ...

    def supports_deferrable_transaction(self)-> bool: ...

    def supports_transaction_wait(self)-> bool: ...

    def supports_savepoint(self)-> bool: ...

    def format_begin_transaction_statement(
        self, expr: "BeginTransactionStatement"
    ) -> Tuple[str, tuple]:
        """Format BEGIN TRANSACTION with PostgreSQL inline options.

        PostgreSQL syntax::

            BEGIN [ ISOLATION LEVEL { ... } ] [ { READ WRITE | READ ONLY } ]
                  [ { NOT DEFERRABLE | DEFERRABLE } ]

        Args:
            expr: BeginTransactionStatement carrying isolation level and mode.

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_set_transaction_statement(
        self, expr: "SetTransactionStatement"
    ) -> Tuple[str, tuple]:
        """Format SET TRANSACTION / SET SESSION CHARACTERISTICS statement.

        Args:
            expr: SetTransactionStatement carrying transaction characteristics.

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_postgres_set_constraints_statement(
        self, expr: "PostgresSetConstraintsStatement"
    ) -> Tuple[str, tuple]:
        """Format ``SET CONSTRAINTS <name> DEFERRED``.

        The constraint name is an identifier and is rendered through the
        dialect's identifier quoting.

        Args:
            expr: Set-constraints statement carrying the constraint name.

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...
