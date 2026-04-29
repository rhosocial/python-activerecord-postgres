# src/rhosocial/activerecord/backend/impl/postgres/mixins/parallel_query.py
"""
PostgreSQL parallel query mixin.

Implements the PostgresParallelQuerySupport protocol.
"""


class PostgresParallelQueryMixin:
    """Mixin for PostgreSQL parallel query support.

    Provides version-aware capability detection for PostgreSQL
    parallel query execution features.
    """

    def supports_parallel_query(self) -> bool:
        """Whether parallel query execution is supported (PostgreSQL 9.6+)."""
        return self.version >= (9, 6, 0)

    def supports_parallel_append(self) -> bool:
        """Whether parallel append is supported (PostgreSQL 10+)."""
        return self.version >= (10, 0, 0)

    def supports_parallel_index_scan(self) -> bool:
        """Whether parallel index scan is supported (PostgreSQL 10+)."""
        return self.version >= (10, 0, 0)

    def supports_parallel_index_only_scan(self) -> bool:
        """Whether parallel index-only scan is supported (PostgreSQL 11+)."""
        return self.version >= (11, 0, 0)

    def supports_parallel_hash_join(self) -> bool:
        """Whether parallel hash join is supported (PostgreSQL 11+)."""
        return self.version >= (11, 0, 0)

    def supports_parallel_gather_merge(self) -> bool:
        """Whether Gather Merge is supported (PostgreSQL 10+)."""
        return self.version >= (10, 0, 0)
