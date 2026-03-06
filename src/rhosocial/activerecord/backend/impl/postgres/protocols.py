# src/rhosocial/activerecord/backend/impl/postgres/protocols.py
"""PostgreSQL dialect-specific protocol definitions.

This module defines protocols for features exclusive to PostgreSQL,
which are not part of the SQL standard and not supported by other
mainstream databases.
"""
from typing import Protocol, runtime_checkable, Dict, Optional, Tuple, Any, List
from dataclasses import dataclass


@dataclass
class PostgresExtensionInfo:
    """PostgreSQL extension information.

    Attributes:
        name: Extension name
        installed: Whether the extension is installed (enabled in database)
        version: Extension version number
        schema: Schema where the extension is installed
    """
    name: str
    installed: bool = False
    version: Optional[str] = None
    schema: Optional[str] = None


@runtime_checkable
class PostgresExtensionSupport(Protocol):
    """PostgreSQL extension detection protocol.

    PostgreSQL supports installing additional functionality modules via CREATE EXTENSION.
    Common extensions include:
    - PostGIS: Spatial database functionality
    - pgvector: Vector similarity search
    - pg_trgm: Trigram similarity
    - hstore: Key-value pair storage
    - uuid-ossp: UUID generation functions

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'xxx';
    - Programmatic detection: dialect.is_extension_installed('xxx')

    Version requirement: PostgreSQL 9.1+ supports CREATE EXTENSION
    """

    def detect_extensions(self, connection) -> Dict[str, 'PostgresExtensionInfo']:
        """Detect all installed extensions.

        Queries pg_extension system table to get extension information.
        This method should be called within introspect_and_adapt().

        Args:
            connection: Database connection object

        Returns:
            Dictionary mapping extension names to extension info
        """
        ...

    def get_extension_info(self, name: str) -> Optional['PostgresExtensionInfo']:
        """Get information for a specific extension.

        Args:
            name: Extension name

        Returns:
            Extension info, or None if not detected or doesn't exist
        """
        ...

    def is_extension_installed(self, name: str) -> bool:
        """Check if an extension is installed.

        Args:
            name: Extension name

        Returns:
            True if extension is installed and enabled
        """
        ...

    def get_extension_version(self, name: str) -> Optional[str]:
        """Get extension version.

        Args:
            name: Extension name

        Returns:
            Version string, or None if not installed
        """
        ...


@runtime_checkable
class PostgresMaterializedViewSupport(Protocol):
    """PostgreSQL materialized view extended features protocol.

    PostgreSQL's materialized view support extends beyond SQL standard, including:
    - CONCURRENTLY refresh (requires unique index)
    - TABLESPACE storage
    - WITH (storage_options) storage parameters

    Version requirements:
    - Basic materialized view: PostgreSQL 9.3+
    - CONCURRENTLY refresh: PostgreSQL 9.4+
    - TABLESPACE: PostgreSQL 9.3+

    Note: These features don't require additional plugins, they're part of
    PostgreSQL official distribution.

    Documentation: https://www.postgresql.org/docs/current/sql-creatematerializedview.html
    """

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """Whether CONCURRENTLY refresh for materialized views is supported.

        PostgreSQL 9.4+ supports the CONCURRENTLY option.
        When using CONCURRENTLY, the materialized view must have at least one UNIQUE index.
        """
        ...


@runtime_checkable
class PostgresTableSupport(Protocol):
    """PostgreSQL table extended features protocol.

    PostgreSQL's table support includes exclusive features:
    - INHERITS table inheritance
    - TABLESPACE table-level storage
    - ON COMMIT control for temporary tables

    Version requirements:
    - INHERITS: All versions
    - TABLESPACE: All versions
    - ON COMMIT: PostgreSQL 8.0+

    Note: These features don't require additional plugins, they're part of
    PostgreSQL official distribution.
    """

    def supports_table_inheritance(self) -> bool:
        """Whether table inheritance (INHERITS) is supported.

        PostgreSQL supports table inheritance, where child tables inherit
        all columns from parent tables.
        Syntax: CREATE TABLE child (...) INHERITS (parent);
        """
        ...


@runtime_checkable
class PostgresPgvectorSupport(Protocol):
    """pgvector vector similarity search protocol.

    Feature Source: Extension support (requires pgvector extension)

    pgvector provides AI vector similarity search functionality:
    - vector data type
    - Vector similarity search (<-> operator)
    - IVFFlat index
    - HNSW index (requires 0.5.0+)

    Extension Information:
    - Extension name: vector
    - Install command: CREATE EXTENSION vector;
    - Minimum version: 0.1.0
    - Recommended version: 0.5.0+ (supports HNSW index)
    - Repository: https://github.com/pgvector/pgvector
    - Documentation: https://github.com/pgvector/pgvector#usage

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'vector';
    - Programmatic detection: dialect.is_extension_installed('vector')

    Notes:
    - Maximum vector dimension: 2000
    - HNSW index requires version 0.5.0+
    - Ensure extension is installed before use: CREATE EXTENSION vector;
    """

    def supports_pgvector_type(self) -> bool:
        """Whether pgvector vector data type is supported.

        Requires pgvector extension.
        Supports vectors with specified dimensions: vector(N), N max 2000.
        """
        ...

    def supports_pgvector_similarity_search(self) -> bool:
        """Whether pgvector vector similarity search is supported.

        Requires pgvector extension.
        Supports <-> (Euclidean distance) operator.
        """
        ...

    def supports_pgvector_ivfflat_index(self) -> bool:
        """Whether pgvector IVFFlat vector index is supported.

        Requires pgvector extension.
        IVFFlat is an inverted file-based vector index, suitable for medium-scale data.
        """
        ...

    def supports_pgvector_hnsw_index(self) -> bool:
        """Whether pgvector HNSW vector index is supported.

        Requires pgvector 0.5.0+.
        HNSW is a Hierarchical Navigable Small World index, suitable for
        large-scale high-dimensional data.
        """
        ...


@runtime_checkable
class PostgresPostGISSupport(Protocol):
    """PostGIS spatial functionality protocol.

    Feature Source: Extension support (requires PostGIS extension)

    PostGIS provides complete spatial database functionality:
    - geometry and geography data types
    - Spatial indexes (GiST)
    - Spatial analysis functions
    - Coordinate system transformations

    Extension Information:
    - Extension name: postgis
    - Install command: CREATE EXTENSION postgis;
    - Minimum version: 2.0
    - Recommended version: 3.0+
    - Website: https://postgis.net/
    - Documentation: https://postgis.net/docs/

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'postgis';
    - Programmatic detection: dialect.is_extension_installed('postgis')

    Notes:
    - PostGIS needs to be installed at database level
    - Installation requires superuser privileges
    - Features vary across versions
    """

    def supports_postgis_geometry_type(self) -> bool:
        """Whether PostGIS geometry type is supported.

        Requires PostGIS extension.
        geometry type is used for planar coordinate systems.
        """
        ...

    def supports_postgis_geography_type(self) -> bool:
        """Whether PostGIS geography type is supported.

        Requires PostGIS extension.
        geography type is used for spherical coordinate systems (lat/lon).
        """
        ...

    def supports_postgis_spatial_index(self) -> bool:
        """Whether PostGIS spatial indexing is supported.

        Requires PostGIS extension.
        PostgreSQL uses GiST index to support spatial queries.
        """
        ...

    def supports_postgis_spatial_functions(self) -> bool:
        """Whether PostGIS spatial analysis functions are supported.

        Requires PostGIS extension.
        Includes functions like: ST_Distance, ST_Within, ST_Contains, etc.
        """
        ...


@runtime_checkable
class PostgresPgTrgmSupport(Protocol):
    """pg_trgm trigram functionality protocol.

    Feature Source: Extension support (requires pg_trgm extension)

    pg_trgm provides trigram-based text similarity search:
    - Similarity calculation
    - Fuzzy search
    - Similarity indexing

    Extension Information:
    - Extension name: pg_trgm
    - Install command: CREATE EXTENSION pg_trgm;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/pgtrgm.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'pg_trgm';
    - Programmatic detection: dialect.is_extension_installed('pg_trgm')
    """

    def supports_pg_trgm_similarity(self) -> bool:
        """Whether pg_trgm trigram similarity calculation is supported.

        Requires pg_trgm extension.
        Supports similarity functions: similarity(), show_trgm(), etc.
        """
        ...

    def supports_pg_trgm_index(self) -> bool:
        """Whether pg_trgm trigram indexing is supported.

        Requires pg_trgm extension.
        Supports creating GiST or GIN trigram indexes on text columns.
        """
        ...


@runtime_checkable
class PostgresHstoreSupport(Protocol):
    """hstore key-value storage functionality protocol.

    hstore provides key-value pair data type:
    - hstore data type
    - Key-value operators
    - Index support

    Dependency requirements:
    - Extension name: hstore
    - Install command: CREATE EXTENSION hstore;
    - Minimum version: 1.0

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'hstore';
    - Programmatic detection: dialect.is_extension_installed('hstore')

    Documentation: https://www.postgresql.org/docs/current/hstore.html
    """

    def supports_hstore_type(self) -> bool:
        """Whether hstore data type is supported.

        Requires hstore extension.
        hstore is used to store key-value pair collections.
        """
        ...

    def supports_hstore_operators(self) -> bool:
        """Whether hstore operators are supported.

        Requires hstore extension.
        Supports operators like ->, ->>, @>, ?, etc.
        """
        ...


# =============================================================================
# Native Feature Protocols (PostgreSQL Built-in Features)
# =============================================================================


@runtime_checkable
class PostgresPartitionSupport(Protocol):
    """PostgreSQL partitioning enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL provides advanced partitioning features beyond the SQL standard:
    - HASH partitioning (PG 11+)
    - DEFAULT partition (PG 11+)
    - Partition key update row movement (PG 11+)
    - Concurrent DETACH (PG 14+)
    - Partition bounds expression (PG 12+)
    - Partitionwise join/aggregate (PG 11+)

    Official Documentation:
    - Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html
    - HASH Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-HASH
    - Concurrent DETACH: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION-CONCURRENTLY

    Version Requirements:
    - HASH partitioning: PostgreSQL 11+
    - DEFAULT partition: PostgreSQL 11+
    - Row movement: PostgreSQL 11+
    - Concurrent DETACH: PostgreSQL 14+
    - Partition bounds expression: PostgreSQL 12+
    - Partitionwise join: PostgreSQL 11+
    - Partitionwise aggregate: PostgreSQL 11+
    """

    def supports_hash_partitioning(self) -> bool:
        """Whether HASH partitioning is supported.

        Native feature, PostgreSQL 11+.
        Enables HASH partitioning method for tables.
        """
        ...

    def supports_default_partition(self) -> bool:
        """Whether DEFAULT partition is supported.

        Native feature, PostgreSQL 11+.
        Enables DEFAULT partition to catch non-matching rows.
        """
        ...

    def supports_partition_key_update(self) -> bool:
        """Whether partition key update automatically moves rows.

        Native feature, PostgreSQL 11+.
        When updating partition key, rows automatically move to correct partition.
        """
        ...

    def supports_concurrent_detach(self) -> bool:
        """Whether CONCURRENTLY DETACH PARTITION is supported.

        Native feature, PostgreSQL 14+.
        Enables non-blocking partition detachment.
        """
        ...

    def supports_partition_bounds_expression(self) -> bool:
        """Whether partition bounds can use expressions.

        Native feature, PostgreSQL 12+.
        Allows non-constant expressions in partition bounds.
        """
        ...

    def supports_partitionwise_join(self) -> bool:
        """Whether partitionwise join optimization is supported.

        Native feature, PostgreSQL 11+.
        Enables join optimization for partitioned tables.
        """
        ...

    def supports_partitionwise_aggregate(self) -> bool:
        """Whether partitionwise aggregate optimization is supported.

        Native feature, PostgreSQL 11+.
        Enables aggregate optimization for partitioned tables.
        """
        ...


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


@runtime_checkable
class PostgresVacuumSupport(Protocol):
    """PostgreSQL VACUUM enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL VACUUM features:
    - Parallel VACUUM (PG 13+)
    - INDEX_CLEANUP AUTO (PG 14+)
    - PROCESS_TOAST control (PG 14+)

    Official Documentation:
    - VACUUM: https://www.postgresql.org/docs/current/sql-vacuum.html
    - Parallel VACUUM: https://www.postgresql.org/docs/current/sql-vacuum.html#VACUUM-PARALLEL
    - Routine Vacuuming: https://www.postgresql.org/docs/current/routine-vacuuming.html

    Version Requirements:
    - Parallel VACUUM: PostgreSQL 13+
    - INDEX_CLEANUP AUTO: PostgreSQL 14+
    - PROCESS_TOAST: PostgreSQL 14+
    """

    def supports_parallel_vacuum(self) -> bool:
        """Whether parallel VACUUM for indexes is supported.

        Native feature, PostgreSQL 13+.
        Enables parallel workers for VACUUM index processing.
        """
        ...

    def supports_index_cleanup_auto(self) -> bool:
        """Whether INDEX_CLEANUP AUTO option is supported.

        Native feature, PostgreSQL 14+.
        Automatic index cleanup decision based on bloat.
        """
        ...

    def supports_vacuum_process_toast(self) -> bool:
        """Whether PROCESS_TOAST option is supported.

        Native feature, PostgreSQL 14+.
        Allows skipping TOAST table processing.
        """
        ...


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


@runtime_checkable
class PostgresDataTypeSupport(Protocol):
    """PostgreSQL data type enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL data type features:
    - Multirange types (PG 14+)
    - Domain arrays (PG 11+)
    - Composite domains (PG 11+)
    - JSONB subscript (PG 14+)
    - Numeric Infinity (PG 14+)
    - Nondeterministic ICU collation (PG 12+)
    - xid8 type (PG 13+)

    Official Documentation:
    - Multirange: https://www.postgresql.org/docs/current/rangetypes.html
    - Domains: https://www.postgresql.org/docs/current/sql-createdomain.html
    - JSONB: https://www.postgresql.org/docs/current/datatype-json.html
    - ICU Collation: https://www.postgresql.org/docs/current/collation.html#COLLATION-NONDETERMINISTIC
    - xid8: https://www.postgresql.org/docs/current/datatype-oid.html

    Version Requirements:
    - Domain arrays: PostgreSQL 11+
    - Composite domains: PostgreSQL 11+
    - ICU collation: PostgreSQL 12+
    - xid8: PostgreSQL 13+
    - Multirange: PostgreSQL 14+
    - JSONB subscript: PostgreSQL 14+
    - Numeric Infinity: PostgreSQL 14+
    """

    def supports_multirange_type(self) -> bool:
        """Whether multirange data types are supported.

        Native feature, PostgreSQL 14+.
        Enables representation of non-contiguous ranges.
        """
        ...

    def supports_domain_arrays(self) -> bool:
        """Whether arrays of domain types are supported.

        Native feature, PostgreSQL 11+.
        Allows creating arrays over domain types.
        """
        ...

    def supports_composite_domains(self) -> bool:
        """Whether domains over composite types are supported.

        Native feature, PostgreSQL 11+.
        Allows creating domains over composite types.
        """
        ...

    def supports_jsonb_subscript(self) -> bool:
        """Whether JSONB subscript notation is supported.

        Native feature, PostgreSQL 14+.
        Enables jsonb['key'] subscript syntax.
        """
        ...

    def supports_numeric_infinity(self) -> bool:
        """Whether NUMERIC type supports Infinity values.

        Native feature, PostgreSQL 14+.
        Enables Infinity/-Infinity in NUMERIC type.
        """
        ...

    def supports_nondeterministic_collation(self) -> bool:
        """Whether nondeterministic ICU collations are supported.

        Native feature, PostgreSQL 12+.
        Enables case/accent insensitive collations.
        """
        ...

    def supports_xid8_type(self) -> bool:
        """Whether xid8 (64-bit transaction ID) type is supported.

        Native feature, PostgreSQL 13+.
        Provides 64-bit transaction identifiers.
        """
        ...


@runtime_checkable
class PostgresSQLSyntaxSupport(Protocol):
    """PostgreSQL SQL syntax enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL SQL syntax features:
    - Generated columns (PG 12+)
    - CTE SEARCH/CYCLE (PG 14+)
    - FETCH WITH TIES (PG 13+)

    Official Documentation:
    - Generated Columns: https://www.postgresql.org/docs/current/ddl-generated-columns.html
    - CTE SEARCH/CYCLE: https://www.postgresql.org/docs/current/queries-with.html
    - FETCH WITH TIES: https://www.postgresql.org/docs/current/sql-select.html#SQL-LIMIT

    Version Requirements:
    - Generated columns: PostgreSQL 12+
    - FETCH WITH TIES: PostgreSQL 13+
    - CTE SEARCH/CYCLE: PostgreSQL 14+
    """

    def supports_generated_columns(self) -> bool:
        """Whether generated columns are supported.

        Native feature, PostgreSQL 12+.
        Enables GENERATED ALWAYS AS columns.
        """
        ...

    def supports_cte_search_cycle(self) -> bool:
        """Whether CTE SEARCH and CYCLE clauses are supported.

        Native feature, PostgreSQL 14+.
        Enables SQL-standard SEARCH and CYCLE in CTEs.
        """
        ...

    def supports_fetch_with_ties(self) -> bool:
        """Whether FETCH FIRST WITH TIES is supported.

        Native feature, PostgreSQL 13+.
        Includes tied rows in result set.
        """
        ...


@runtime_checkable
class PostgresLogicalReplicationSupport(Protocol):
    """PostgreSQL logical replication enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL logical replication features:
    - Commit timestamp tracking (PG 10+)
    - Streaming transactions (PG 14+)
    - Two-phase decoding (PG 14+)
    - Binary replication (PG 14+)

    Official Documentation:
    - Logical Replication: https://www.postgresql.org/docs/current/logical-replication.html
    - Commit Timestamp: https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-COMMIT-TIMESTAMP
    - Streaming Replication: https://www.postgresql.org/docs/current/protocol-replication.html

    Version Requirements:
    - Commit timestamp: PostgreSQL 10+
    - Streaming transactions: PostgreSQL 14+
    - Two-phase decoding: PostgreSQL 14+
    - Binary replication: PostgreSQL 14+
    """

    def supports_commit_timestamp(self) -> bool:
        """Whether commit timestamp tracking is supported.

        Native feature, PostgreSQL 10+.
        Enables tracking transaction commit timestamps.
        """
        ...

    def supports_streaming_transactions(self) -> bool:
        """Whether streaming in-progress transactions is supported.

        Native feature, PostgreSQL 14+.
        Enables streaming large transactions before commit.
        """
        ...

    def supports_two_phase_decoding(self) -> bool:
        """Whether two-phase commit decoding is supported.

        Native feature, PostgreSQL 14+.
        Enables decoding of prepared transactions.
        """
        ...

    def supports_binary_replication(self) -> bool:
        """Whether binary transfer mode for replication is supported.

        Native feature, PostgreSQL 14+.
        Enables binary data transfer in logical replication.
        """
        ...


# =============================================================================
# DDL Feature Protocols (PostgreSQL-specific DDL extensions)
# =============================================================================


@runtime_checkable
class PostgresTriggerSupport(Protocol):
    """PostgreSQL trigger DDL protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL trigger features beyond SQL:1999 standard:
    - EXECUTE FUNCTION syntax (instead of EXECUTE proc_name)
    - REFERENCING clause for OLD/NEW row references
    - WHEN condition with arbitrary expressions
    - FOR EACH STATEMENT triggers
    - INSTEAD OF triggers for views
    - Multiple events with OR syntax
    - UPDATE OF column_list syntax
    - Constraint triggers with DEFERRABLE

    Official Documentation:
    - CREATE TRIGGER: https://www.postgresql.org/docs/current/sql-createtrigger.html
    - Trigger Functions: https://www.postgresql.org/docs/current/triggers.html

    Version Requirements:
    - Basic triggers: All versions
    - Constraint triggers: PostgreSQL 9.1+
    - Transition tables: PostgreSQL 10+
    """

    def supports_trigger_referencing(self) -> bool:
        """Whether REFERENCING clause is supported.

        Native feature, PostgreSQL 10+.
        Allows referencing OLD/NEW tables in triggers.
        """
        ...

    def supports_trigger_when(self) -> bool:
        """Whether WHEN condition is supported.

        Native feature, all versions.
        Allows conditional trigger execution.
        """
        ...

    def supports_statement_trigger(self) -> bool:
        """Whether FOR EACH STATEMENT triggers are supported.

        Native feature, all versions.
        Triggers execute once per statement, not per row.
        """
        ...

    def supports_instead_of_trigger(self) -> bool:
        """Whether INSTEAD OF triggers are supported.

        Native feature, all versions.
        Used for views to make them updatable.
        """
        ...

    def supports_trigger_if_not_exists(self) -> bool:
        """Whether CREATE TRIGGER IF NOT EXISTS is supported.

        Native feature, PostgreSQL 9.5+.
        """
        ...

    def format_create_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TRIGGER statement.

        PostgreSQL uses 'EXECUTE FUNCTION func_name()' syntax.
        """
        ...

    def format_drop_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format DROP TRIGGER statement."""
        ...


@runtime_checkable
class PostgresCommentSupport(Protocol):
    """PostgreSQL COMMENT ON protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL supports commenting on database objects:
    - COMMENT ON TABLE
    - COMMENT ON COLUMN
    - COMMENT ON VIEW
    - COMMENT ON INDEX
    - COMMENT ON FUNCTION
    - COMMENT ON SCHEMA
    - etc.

    Official Documentation:
    - COMMENT: https://www.postgresql.org/docs/current/sql-comment.html

    Version Requirements:
    - All versions
    """

    def format_comment_statement(
        self,
        object_type: str,
        object_name: str,
        comment: Any
    ) -> Tuple[str, tuple]:
        """Format COMMENT ON statement.

        Args:
            object_type: Object type (TABLE, COLUMN, VIEW, etc.)
            object_name: Object name (e.g., 'table_name' or 'table_name.column_name')
            comment: Comment text, or None to drop comment

        Returns:
            Tuple of (SQL string, parameters)
        """
        ...


# =============================================================================
# Extension Feature Protocols (Additional PostgreSQL Extensions)
# =============================================================================


@runtime_checkable
class PostgresLtreeSupport(Protocol):
    """ltree label tree protocol.

    Feature Source: Extension support (requires ltree extension)

    ltree provides label tree data type:
    - ltree data type for label paths
    - lquery for label path patterns
    - ltxtquery for full-text queries
    - Index support (GiST, B-tree)

    Extension Information:
    - Extension name: ltree
    - Install command: CREATE EXTENSION ltree;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/ltree.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'ltree';
    - Programmatic detection: dialect.is_extension_installed('ltree')
    """

    def supports_ltree_type(self) -> bool:
        """Whether ltree data type is supported.

        Requires ltree extension.
        Stores label paths like 'Top.Science.Astronomy'.
        """
        ...

    def supports_ltree_operators(self) -> bool:
        """Whether ltree operators are supported.

        Requires ltree extension.
        Supports operators: <@, @>, ~, ? and more.
        """
        ...

    def supports_ltree_index(self) -> bool:
        """Whether ltree indexes are supported.

        Requires ltree extension.
        Supports GiST and B-tree indexes on ltree.
        """
        ...


@runtime_checkable
class PostgresIntarraySupport(Protocol):
    """intarray integer array protocol.

    Feature Source: Extension support (requires intarray extension)

    intarray provides integer array operations:
    - Integer array operators (&&, @>, <@, @@, ~~)
    - Integer array functions (uniq, sort, idx, subarray)
    - GiST index support

    Extension Information:
    - Extension name: intarray
    - Install command: CREATE EXTENSION intarray;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/intarray.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'intarray';
    - Programmatic detection: dialect.is_extension_installed('intarray')
    """

    def supports_intarray_operators(self) -> bool:
        """Whether intarray operators are supported.

        Requires intarray extension.
        Supports operators: &&, @>, <@, @@, ~~.
        """
        ...

    def supports_intarray_functions(self) -> bool:
        """Whether intarray functions are supported.

        Requires intarray extension.
        Supports functions: uniq(), sort(), idx(), subarray().
        """
        ...

    def supports_intarray_index(self) -> bool:
        """Whether intarray indexes are supported.

        Requires intarray extension.
        Supports GiST indexes for integer arrays.
        """
        ...


@runtime_checkable
class PostgresEarthdistanceSupport(Protocol):
    """earthdistance earth distance calculation protocol.

    Feature Source: Extension support (requires earthdistance extension)

    earthdistance provides earth distance calculations:
    - earth data type for surface points
    - Great-circle distance calculation
    - Distance operators (<@> for miles)

    Extension Information:
    - Extension name: earthdistance
    - Install command: CREATE EXTENSION earthdistance;
    - Minimum version: 1.0
    - Dependencies: cube extension
    - Documentation: https://www.postgresql.org/docs/current/earthdistance.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'earthdistance';
    - Programmatic detection: dialect.is_extension_installed('earthdistance')
    """

    def supports_earthdistance_type(self) -> bool:
        """Whether earth data type is supported.

        Requires earthdistance extension.
        Represents points on Earth's surface.
        """
        ...

    def supports_earthdistance_operators(self) -> bool:
        """Whether earthdistance operators are supported.

        Requires earthdistance extension.
        Supports <@> operator for distance in miles.
        """
        ...


@runtime_checkable
class PostgresTablefuncSupport(Protocol):
    """tablefunc table functions protocol.

    Feature Source: Extension support (requires tablefunc extension)

    tablefunc provides table functions:
    - crosstab() for pivot tables
    - crosstabN() for predefined column crosstabs
    - connectby() for hierarchical tree queries
    - normal_rand() for normal distribution random numbers

    Extension Information:
    - Extension name: tablefunc
    - Install command: CREATE EXTENSION tablefunc;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/tablefunc.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'tablefunc';
    - Programmatic detection: dialect.is_extension_installed('tablefunc')
    """

    def supports_tablefunc_crosstab(self) -> bool:
        """Whether crosstab functions are supported.

        Requires tablefunc extension.
        Enables pivot table operations.
        """
        ...

    def supports_tablefunc_connectby(self) -> bool:
        """Whether connectby function is supported.

        Requires tablefunc extension.
        Enables hierarchical tree queries.
        """
        ...

    def supports_tablefunc_normal_rand(self) -> bool:
        """Whether normal_rand function is supported.

        Requires tablefunc extension.
        Generates normal distribution random numbers.
        """
        ...


@runtime_checkable
class PostgresPgStatStatementsSupport(Protocol):
    """pg_stat_statements query statistics protocol.

    Feature Source: Extension support (requires pg_stat_statements extension)

    pg_stat_statements provides query execution statistics:
    - pg_stat_statements view for query stats
    - Execution time tracking
    - Shared block I/O statistics
    - Query plan identification

    Extension Information:
    - Extension name: pg_stat_statements
    - Install command: CREATE EXTENSION pg_stat_statements;
    - Minimum version: 1.0
    - Requires preload: shared_preload_libraries = 'pg_stat_statements' in postgresql.conf
    - Documentation: https://www.postgresql.org/docs/current/pgstatstatements.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
    - Programmatic detection: dialect.is_extension_installed('pg_stat_statements')

    Notes:
    - Requires shared_preload_libraries configuration
    - Requires PostgreSQL server restart
    """

    def supports_pg_stat_statements_view(self) -> bool:
        """Whether pg_stat_statements view is available.

        Requires pg_stat_statements extension.
        Provides query execution statistics.
        """
        ...

    def supports_pg_stat_statements_reset(self) -> bool:
        """Whether pg_stat_statements_reset() function is available.

        Requires pg_stat_statements extension.
        Resets query statistics.
        """
        ...

    def reset_pg_stat_statements(self) -> bool:
        """Reset pg_stat_statements statistics.

        Returns:
            True if reset was successful
        """
        ...


# =============================================================================
# Type DDL Protocols (PostgreSQL-specific Type DDL)
# =============================================================================


@runtime_checkable
class PostgresTypeSupport(Protocol):
    """PostgreSQL type DDL protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL supports user-defined types:
    - ENUM types (CREATE TYPE ... AS ENUM)
    - Composite types (CREATE TYPE ... AS)
    - Range types
    - Base types (with C functions)

    This protocol focuses on ENUM type DDL support.

    Official Documentation:
    - CREATE TYPE: https://www.postgresql.org/docs/current/sql-createtype.html
    - DROP TYPE: https://www.postgresql.org/docs/current/sql-droptype.html

    Version Requirements:
    - All versions support ENUM types
    """

    def supports_create_type(self) -> bool:
        """Whether CREATE TYPE is supported.

        Native feature, all versions.
        """
        ...

    def supports_drop_type(self) -> bool:
        """Whether DROP TYPE is supported.

        Native feature, all versions.
        """
        ...

    def supports_type_if_not_exists(self) -> bool:
        """Whether CREATE TYPE IF NOT EXISTS is supported.

        PostgreSQL does NOT support IF NOT EXISTS for CREATE TYPE.
        Always returns False.
        """
        ...

    def supports_type_if_exists(self) -> bool:
        """Whether DROP TYPE IF EXISTS is supported.

        Native feature, all versions.
        """
        ...

    def supports_type_cascade(self) -> bool:
        """Whether DROP TYPE CASCADE is supported.

        Native feature, all versions.
        """
        ...

    def format_create_type_enum_statement(
        self,
        name: str,
        values: List[str],
        schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE TYPE ... AS ENUM statement.

        Args:
            name: Type name
            values: List of enum values
            schema: Optional schema name

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...

    def format_drop_type_statement(
        self,
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False
    ) -> Tuple[str, tuple]:
        """Format DROP TYPE statement.

        Args:
            name: Type name
            schema: Optional schema name
            if_exists: Whether to add IF EXISTS
            cascade: Whether to add CASCADE

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
