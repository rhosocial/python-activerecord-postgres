# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/index.py
"""PostgreSQL index enhancements protocol.

This module defines the protocol for PostgreSQL-specific index features
that extend beyond standard SQL.
"""
from typing import Protocol, runtime_checkable, Optional, Tuple, List, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expressions import ReindexExpression


@runtime_checkable
class PostgresIndexSupport(Protocol):
    """PostgreSQL index enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL index features beyond standard SQL:
    - B-tree deduplication (PG 13+)
    - BRIN multivalue/bloom filter (PG 14+)
    - GiST INCLUDE (PG 12+)
    - SP-GiST INCLUDE (PG 14+)
    - Hash index WAL logging (PG 10+)
    - Parallel index build (PG 11+)
    - REINDEX CONCURRENTLY (PG 12+)

    Official Documentation:
    - B-tree Deduplication: https://www.postgresql.org/docs/current/btree-implementation.html#BTREE-DEDUPLICATION
    - BRIN Indexes: https://www.postgresql.org/docs/current/brin.html
    - GiST Indexes: https://www.postgresql.org/docs/current/gist.html
    - Hash Indexes: https://www.postgresql.org/docs/current/hash-index.html
    - REINDEX: https://www.postgresql.org/docs/current/sql-reindex.html
    - CREATE INDEX: https://www.postgresql.org/docs/current/sql-createindex.html

    Version Requirements:
    - Hash index WAL: PostgreSQL 10+
    - Parallel CREATE INDEX: PostgreSQL 11+
    - GiST INCLUDE: PostgreSQL 12+
    - REINDEX CONCURRENTLY: PostgreSQL 12+
    - B-tree deduplication: PostgreSQL 13+
    - BRIN multivalue: PostgreSQL 14+
    - BRIN bloom: PostgreSQL 14+
    - SP-GiST INCLUDE: PostgreSQL 14+
    """

    def supports_safe_hash_index(self) -> bool:
        """Whether hash indexes are crash-safe (WAL-logged).

        Native feature, PostgreSQL 10+.
        Hash indexes are now fully WAL-logged and crash-safe.
        """
        ...

    def supports_parallel_create_index(self) -> bool:
        """Whether parallel B-tree index build is supported.

        Native feature, PostgreSQL 11+.
        Enables parallel workers for B-tree index creation.
        """
        ...

    def supports_gist_include(self) -> bool:
        """Whether GiST indexes support INCLUDE clause.

        Native feature, PostgreSQL 12+.
        Allows including non-key columns in GiST indexes.
        """
        ...

    def supports_reindex_concurrently(self) -> bool:
        """Whether REINDEX CONCURRENTLY is supported.

        Native feature, PostgreSQL 12+.
        Enables non-blocking index rebuilding.
        """
        ...

    def supports_btree_deduplication(self) -> bool:
        """Whether B-tree deduplication is supported.

        Native feature, PostgreSQL 13+.
        Reduces index size for columns with many duplicate values.
        """
        ...

    def supports_brin_multivalue(self) -> bool:
        """Whether BRIN indexes support multiple min/max values.

        Native feature, PostgreSQL 14+.
        Allows BRIN indexes to store multiple value ranges per block.
        """
        ...

    def supports_brin_bloom(self) -> bool:
        """Whether BRIN bloom filter indexes are supported.

        Native feature, PostgreSQL 14+.
        Enables bloom filter ops for BRIN indexes.
        """
        ...

    def supports_spgist_include(self) -> bool:
        """Whether SP-GiST indexes support INCLUDE clause.

        Native feature, PostgreSQL 14+.
        Allows including non-key columns in SP-GiST indexes.
        """
        ...

    def format_reindex_statement(
        self, expr: "ReindexExpression"
    ) -> Tuple[str, tuple]:
        """Format REINDEX statement with PostgreSQL-specific options.

        Args:
            expr: ReindexExpression containing all REINDEX options

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...

    def format_create_index_pg_statement(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
        schema: Optional[str] = None,
        unique: bool = False,
        index_type: str = 'btree',
        concurrently: bool = False,
        if_not_exists: bool = False,
        include_columns: Optional[List[str]] = None,
        with_options: Optional[Dict[str, Any]] = None,
        tablespace: Optional[str] = None,
        where_clause: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement with PostgreSQL-specific options.

        This method provides a convenient way to create indexes with
        PostgreSQL-specific options without using expression objects.

        Args:
            index_name: Index name
            table_name: Table name
            columns: Column names for index key
            schema: Schema name
            unique: Create UNIQUE index
            index_type: Index type ('btree', 'hash', 'gist', 'gin', 'spgist', 'brin')
            concurrently: Use CONCURRENTLY (PG 11+)
            if_not_exists: Add IF NOT EXISTS
            include_columns: Non-key columns for INCLUDE (PG 12+ for GiST, PG 14+ for SP-GiST)
            with_options: Storage parameters as dict
            tablespace: Tablespace name
            where_clause: WHERE clause for partial index

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
