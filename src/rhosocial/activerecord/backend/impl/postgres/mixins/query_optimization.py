# src/rhosocial/activerecord/backend/impl/postgres/mixins/query_optimization.py
"""PostgreSQL query optimization mixin implementation.

This module provides the PostgresQueryOptimizationMixin class which
implements query optimization feature detection for PostgreSQL backends.
"""


class PostgresQueryOptimizationMixin:
    """PostgreSQL query optimization implementation.

    All features are native, using version number for detection.
    """

    def supports_jit(self) -> bool:
        """JIT compilation is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_incremental_sort(self) -> bool:
        """Incremental sort is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    def supports_memoize(self) -> bool:
        """Memoize is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_async_foreign_scan(self) -> bool:
        """Async foreign scan is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    # =========================================================================
    # Parallel Query Support (PostgreSQL 9.6+)
    # =========================================================================

    def supports_parallel_query(self) -> bool:
        """Parallel query execution is supported since PostgreSQL 9.6.

        Includes:
        - Parallel sequential scan
        - Parallel joins
        - Parallel aggregates

        Returns:
            True if parallel query is supported
        """
        return self.version >= (9, 6, 0)

    def supports_parallel_append(self) -> bool:
        """Parallel Append merge is supported since PostgreSQL 10.

        Allows Append nodes to run in parallel, improving
        UNION ALL and inheritance queries.

        Returns:
            True if parallel append is supported
        """
        return self.version >= (10, 0, 0)

    def supports_parallel_index_scan(self) -> bool:
        """Parallel index scan is supported since PostgreSQL 10.

        Returns:
            True if parallel index scan is supported
        """
        return self.version >= (10, 0, 0)

    def supports_parallel_index_only_scan(self) -> bool:
        """Parallel index-only scan is supported since PostgreSQL 11.

        Returns:
            True if parallel index-only scan is supported
        """
        return self.version >= (11, 0, 0)

    def supports_parallel_hash_join(self) -> bool:
        """Parallel hash join is supported since PostgreSQL 11.

        Returns:
            True if parallel hash join is supported
        """
        return self.version >= (11, 0, 0)

    def supports_parallel_gather_merge(self) -> bool:
        """Gather Merge node for parallel query is supported since PostgreSQL 10.

        Returns:
            True if Gather Merge is supported
        """
        return self.version >= (10, 0, 0)
