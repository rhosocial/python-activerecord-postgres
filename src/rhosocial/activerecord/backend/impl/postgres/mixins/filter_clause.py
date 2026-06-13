# src/rhosocial/activerecord/backend/impl/postgres/mixins/filter_clause.py
"""PostgreSQL filter clause feature support implementation."""


class PostgresFilterMixin:
    """PostgreSQL filter clause override implementation.

    All features are native, using version number for detection.
    """

    def supports_filter_clause(self) -> bool:
        return self.version >= (9, 4, 0)
