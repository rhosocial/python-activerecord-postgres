# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgvector.py
"""pgvector extension protocol definition.

This module defines the protocol for pgvector vector similarity search
functionality in PostgreSQL.
"""
from typing import Protocol, runtime_checkable


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
