# src/rhosocial/activerecord/backend/impl/postgres/mixins/returning.py
"""PostgreSQL returning feature support implementation."""


class PostgresReturningMixin:
    """PostgreSQL returning override implementation.

    All features are native, using version number for detection.
    """

    def supports_returning_insert(self) -> bool:
        return self.version >= (8, 2, 0)

    def supports_returning_update(self) -> bool:
        return self.version >= (8, 2, 0)

    def supports_returning_delete(self) -> bool:
        return self.version >= (8, 2, 0)
