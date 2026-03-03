# src/rhosocial/activerecord/backend/impl/postgres/protocols.py
"""PostgreSQL dialect-specific protocol definitions.

This module defines protocols for features exclusive to PostgreSQL,
which are not part of the SQL standard and not supported by other
mainstream databases.
"""
from typing import Protocol, runtime_checkable, Dict, Optional
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
class PostgresVectorSupport(Protocol):
    """pgvector vector functionality protocol.

    pgvector provides AI vector similarity search functionality:
    - vector data type
    - Vector similarity search (<-> operator)
    - IVFFlat index
    - HNSW index (requires 0.5.0+)

    Dependency requirements:
    - Extension name: vector
    - Install command: CREATE EXTENSION vector;
    - Minimum version: 0.1.0
    - Recommended version: 0.5.0+ (supports HNSW index)

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'vector';
    - Programmatic detection: dialect.is_extension_installed('vector')

    Notes:
    - Maximum vector dimension: 2000
    - HNSW index requires version 0.5.0+
    - Ensure extension is installed before use: CREATE EXTENSION vector;

    Documentation: https://github.com/pgvector/pgvector
    """

    def supports_vector_type(self) -> bool:
        """Whether vector data type is supported.

        Requires pgvector extension.
        Supports vectors with specified dimensions: vector(N), N max 2000.
        """
        ...

    def supports_vector_similarity_search(self) -> bool:
        """Whether vector similarity search is supported.

        Requires pgvector extension.
        Supports <-> (Euclidean distance) operator.
        """
        ...

    def supports_ivfflat_index(self) -> bool:
        """Whether IVFFlat vector index is supported.

        Requires pgvector extension.
        IVFFlat is an inverted file-based vector index, suitable for medium-scale data.
        """
        ...

    def supports_hnsw_index(self) -> bool:
        """Whether HNSW vector index is supported.

        Requires pgvector 0.5.0+.
        HNSW is a Hierarchical Navigable Small World index, suitable for
        large-scale high-dimensional data.
        """
        ...


@runtime_checkable
class PostgresSpatialSupport(Protocol):
    """PostGIS spatial functionality protocol.

    PostGIS provides complete spatial database functionality:
    - geometry and geography data types
    - Spatial indexes (GiST)
    - Spatial analysis functions
    - Coordinate system transformations

    Dependency requirements:
    - Extension name: postgis
    - Install command: CREATE EXTENSION postgis;
    - Minimum version: 2.0
    - Recommended version: 3.0+

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'postgis';
    - Programmatic detection: dialect.is_extension_installed('postgis')

    Notes:
    - PostGIS needs to be installed at database level
    - Installation requires superuser privileges
    - Features vary across versions

    Documentation: https://postgis.net/docs/
    """

    def supports_geometry_type(self) -> bool:
        """Whether geometry type is supported.

        Requires PostGIS extension.
        geometry type is used for planar coordinate systems.
        """
        ...

    def supports_geography_type(self) -> bool:
        """Whether geography type is supported.

        Requires PostGIS extension.
        geography type is used for spherical coordinate systems (lat/lon).
        """
        ...

    def supports_spatial_index(self) -> bool:
        """Whether spatial indexing is supported.

        Requires PostGIS extension.
        PostgreSQL uses GiST index to support spatial queries.
        """
        ...

    def supports_spatial_functions(self) -> bool:
        """Whether spatial analysis functions are supported.

        Requires PostGIS extension.
        Includes functions like: ST_Distance, ST_Within, ST_Contains, etc.
        """
        ...


@runtime_checkable
class PostgresTrigramSupport(Protocol):
    """pg_trgm trigram functionality protocol.

    pg_trgm provides trigram-based text similarity search:
    - Similarity calculation
    - Fuzzy search
    - Similarity indexing

    Dependency requirements:
    - Extension name: pg_trgm
    - Install command: CREATE EXTENSION pg_trgm;
    - Minimum version: 1.0

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'pg_trgm';
    - Programmatic detection: dialect.is_extension_installed('pg_trgm')

    Documentation: https://www.postgresql.org/docs/current/pgtrgm.html
    """

    def supports_trigram_similarity(self) -> bool:
        """Whether trigram similarity calculation is supported.

        Requires pg_trgm extension.
        Supports similarity functions: similarity(), show_trgm(), etc.
        """
        ...

    def supports_trigram_index(self) -> bool:
        """Whether trigram indexing is supported.

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
