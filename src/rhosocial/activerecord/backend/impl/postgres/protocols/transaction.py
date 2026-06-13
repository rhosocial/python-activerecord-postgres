# src/rhosocial/activerecord/backend/impl/postgres/protocols/transaction.py
"""PostgreSQL transaction feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresTransactionSupport(Protocol):
    """PostgreSQL transaction feature support protocol."""

    def supports_transaction_mode(self)-> bool: ...

    def supports_isolation_level_in_begin(self)-> bool: ...

    def supports_read_only_transaction(self)-> bool: ...

    def supports_deferrable_transaction(self)-> bool: ...

    def supports_savepoint(self)-> bool: ...
