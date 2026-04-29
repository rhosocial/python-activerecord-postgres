# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgvector.py
"""pgvector extension protocol definition.

This module defines the protocol for pgvector vector similarity search
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/pgvector.py`` instead of the removed format_* methods.
For DDL index creation, use ``format_create_vector_index_statement``
or ``format_create_hnsw_index_statement``.
"""

from typing import List, Optional, Protocol, Tuple, runtime_checkable


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
    """

    def supports_pgvector_type(self) -> bool:
        """Whether pgvector vector data type is supported."""
        ...

    def supports_pgvector_similarity_search(self) -> bool:
        """Whether pgvector vector similarity search is supported."""
        ...

    def supports_pgvector_ivfflat_index(self) -> bool:
        """Whether pgvector IVFFlat vector index is supported."""
        ...

    def supports_pgvector_hnsw_index(self) -> bool:
        """Whether pgvector HNSW vector index is supported."""
        ...

    def format_create_vector_index_statement(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        index_type: str = "hnsw",
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
        lists: Optional[int] = None,
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for vector column."""
        ...

    def format_create_hnsw_index_statement(
        self,
        table_name: str,
        column_name: str,
        index_name: Optional[str] = None,
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
    ) -> str:
        """Format CREATE INDEX statement with HNSW index for vector column."""
        ...
