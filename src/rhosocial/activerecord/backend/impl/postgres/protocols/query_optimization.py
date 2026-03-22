# src/rhosocial/activerecord/backend/impl/postgres/protocols/query_optimization.py
"""PostgreSQL query optimization features protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresQueryOptimizationSupport(Protocol):
    """PostgreSQL query optimization features protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL query optimization features:
    - JIT compilation (PG 11+)
    - Incremental sort (PG 13+)
    - Memoize execution node (PG 14+)
    - Async foreign scan (PG 14+)

    Official Documentation:
    - JIT: https://www.postgresql.org/docs/current/jit.html
    - Incremental Sort: https://www.postgresql.org/docs/current/runtime-config-query.html#GUC-ENABLE-INCREMENTAL-SORT
    - Memoize: https://www.postgresql.org/docs/current/runtime-config-query.html#GUC-ENABLE-MEMOIZE
    - Async Foreign Scan: https://www.postgresql.org/docs/current/postgres-fdw.html

    Version Requirements:
    - JIT: PostgreSQL 11+ (requires LLVM)
    - Incremental sort: PostgreSQL 13+
    - Memoize: PostgreSQL 14+
    - Async foreign scan: PostgreSQL 14+
    """

    def supports_jit(self) -> bool:
        """Whether JIT compilation is supported.

        Native feature, PostgreSQL 11+ (requires LLVM).
        Enables Just-In-Time compilation for query execution.
        """
        ...

    def supports_incremental_sort(self) -> bool:
        """Whether incremental sorting is supported.

        Native feature, PostgreSQL 13+.
        Enables incremental sort optimization for already-sorted data.
        """
        ...

    def supports_memoize(self) -> bool:
        """Whether Memoize execution node is supported.

        Native feature, PostgreSQL 14+.
        Caches results from inner side of nested loop joins.
        """
        ...

    def supports_async_foreign_scan(self) -> bool:
        """Whether asynchronous foreign table scan is supported.

        Native feature, PostgreSQL 14+.
        Enables parallel execution of foreign table scans.
        """
        ...
