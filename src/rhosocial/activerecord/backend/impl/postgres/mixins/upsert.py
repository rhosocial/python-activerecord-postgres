# src/rhosocial/activerecord/backend/impl/postgres/mixins/upsert.py
"""PostgreSQL upsert feature support implementation."""


class PostgresUpsertMixin:
    """PostgreSQL upsert override implementation.

    All features are native, using version number for detection.
    """

    def supports_upsert(self) -> bool:
        return self.version >= (9, 5, 0)

    def supports_on_conflict_clause(self) -> bool:
        return True

    def supports_multiple_on_conflict_clauses(self) -> bool:
        """PostgreSQL grammar allows only a single ON CONFLICT clause per INSERT."""
        return False
