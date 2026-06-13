# src/rhosocial/activerecord/backend/impl/postgres/mixins/ordered_set_aggregation.py
"""PostgreSQL ordered set aggregation feature support implementation."""


class PostgresOrderedSetAggMixin:
    """PostgreSQL ordered set aggregation override implementation.

    All features are native, using version number for detection.
    """

    def supports_ordered_set_aggregation(self) -> bool:
        return self.version >= (9, 4, 0)
