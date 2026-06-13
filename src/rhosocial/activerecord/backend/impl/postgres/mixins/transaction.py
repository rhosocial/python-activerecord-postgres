# src/rhosocial/activerecord/backend/impl/postgres/mixins/transaction.py
"""PostgreSQL transaction feature support implementation."""


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
