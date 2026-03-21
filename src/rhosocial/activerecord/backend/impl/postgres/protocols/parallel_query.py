# src/rhosocial/activerecord/backend/impl/postgres/protocols/parallel_query.py
"""PostgreSQL parallel query execution protocol definition.

This module defines the protocol for parallel query execution features
that are native to PostgreSQL and do not require any extension.
"""
from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresParallelQuerySupport(Protocol):
    """PostgreSQL parallel query execution protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL parallel query features:
    - Parallel sequential scan (PG 9.6+)
    - Parallel append (PG 10+)
    - Parallel index scan (PG 10+)
    - Parallel index-only scan (PG 11+)
    - Parallel hash join (PG 11+)
    - Gather Merge node (PG 10+)

    Official Documentation:
    - Parallel Query: https://www.postgresql.org/docs/current/parallel-query.html

    Version Requirements:
    - Parallel query: PostgreSQL 9.6+
    - Parallel append: PostgreSQL 10+
    - Parallel index scan: PostgreSQL 10+
    - Parallel index-only scan: PostgreSQL 11+
    - Parallel hash join: PostgreSQL 11+
    """

    def supports_parallel_query(self) -> bool:
        """Whether parallel query execution is supported.

        Native feature, PostgreSQL 9.6+.
        Enables parallel sequential scan, joins, and aggregates.
        """
        ...

    def supports_parallel_append(self) -> bool:
        """Whether parallel Append node is supported.

        Native feature, PostgreSQL 10+.
        Improves UNION ALL and inheritance queries.
        """
        ...

    def supports_parallel_index_scan(self) -> bool:
        """Whether parallel index scan is supported.

        Native feature, PostgreSQL 10+.
        Enables parallel index scans for B-tree indexes.
        """
        ...

    def supports_parallel_index_only_scan(self) -> bool:
        """Whether parallel index-only scan is supported.

        Native feature, PostgreSQL 11+.
        Enables parallel index-only scans.
        """
        ...

    def supports_parallel_hash_join(self) -> bool:
        """Whether parallel hash join is supported.

        Native feature, PostgreSQL 11+.
        Enables parallel hash join execution.
        """
        ...

    def supports_parallel_gather_merge(self) -> bool:
        """Whether Gather Merge node is supported.

        Native feature, PostgreSQL 10+.
        Enables efficient parallel sort merging.
        """
        ...
