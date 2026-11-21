# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgvector.py
"""pgvector extension protocol — capability probes only.

Distance operators come from ``functions/pgvector.py`` and index DDL from
``functions.vector_search.create_vector_index``; both return expression nodes.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgvectorSupport(Protocol):
    """pgvector vector similarity search protocol."""

    def supports_pgvector_type(self) -> bool: ...

    def supports_pgvector_similarity_search(self) -> bool: ...

    def supports_pgvector_ivfflat_index(self) -> bool: ...

    def supports_pgvector_hnsw_index(self) -> bool: ...
