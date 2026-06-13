# src/rhosocial/activerecord/backend/impl/postgres/mixins/grouping.py
"""PostgreSQL grouping feature support implementation."""


class PostgresGroupingMixin:
    """PostgreSQL grouping override implementation.

    All features are native, using version number for detection.
    """

    def supports_rollup(self) -> bool:
        return self.version >= (9, 5, 0)

    def supports_cube(self) -> bool:
        return self.version >= (9, 5, 0)

    def supports_grouping_sets(self) -> bool:
        return self.version >= (9, 5, 0)
