# src/rhosocial/activerecord/backend/impl/postgres/mixins/ilike.py
"""PostgreSQL ilike feature support implementation."""


class PostgresILIKEMixin:
    """PostgreSQL ilike override implementation.

    All features are native, using version number for detection.
    """

    def supports_ilike(self) -> bool:
        return True
