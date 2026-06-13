# src/rhosocial/activerecord/backend/impl/postgres/mixins/collation.py
"""PostgreSQL collation feature support implementation."""


class PostgresCollationMixin:
    """PostgreSQL collation override implementation.

    All features are native, using version number for detection.
    """

    def supports_collate_expression(self) -> bool:
        return True
