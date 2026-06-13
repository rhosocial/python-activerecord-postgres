# src/rhosocial/activerecord/backend/impl/postgres/mixins/upsert.py
"""PostgreSQL upsert feature support implementation."""


class PostgresUpsertMixin:
    """PostgreSQL upsert override implementation.

    All features are native, using version number for detection.
    """

    def supports_upsert(self) -> bool:
        return self.version >= (9, 5, 0)
