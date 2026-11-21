# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgvector.py
"""pgvector capability probes.

Distance operators are built by ``functions/pgvector.py`` and index DDL by
``functions.vector_search.create_vector_index``. Both return expression nodes
whose rendering goes through the one-to-one formatter on the dialect.
"""


class PostgresPgvectorMixin:
    """pgvector vector similarity search implementation."""

    def supports_pgvector_type(self) -> bool:
        """Check if pgvector type is available."""
        return self.check_extension_feature("vector", "type")

    def supports_pgvector_similarity_search(self) -> bool:
        """Check if pgvector supports similarity search."""
        return self.check_extension_feature("vector", "similarity_search")

    def supports_pgvector_ivfflat_index(self) -> bool:
        """Check if pgvector supports IVFFlat index."""
        return self.check_extension_feature("vector", "ivfflat_index")

    def supports_pgvector_hnsw_index(self) -> bool:
        """Check if pgvector supports HNSW index (requires 0.5.0+)."""
        return self.check_extension_feature("vector", "hnsw_index")
