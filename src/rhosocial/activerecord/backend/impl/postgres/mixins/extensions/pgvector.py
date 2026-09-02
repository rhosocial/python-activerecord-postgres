# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgvector.py
"""
PostgreSQL pgvector vector similarity search mixin.

This module provides functionality to check pgvector extension features,
including vector type, similarity search, and index types.

For SQL expression generation, use the function factories in
``functions/pgvector.py`` instead of the removed format_* methods.
For DDL index creation, use ``format_create_vector_index_statement``
or ``format_create_hnsw_index_statement``.
"""

from typing import Optional, Tuple


class PostgresPgvectorMixin:
    """pgvector vector similarity search implementation."""

    def supports_pgvector_type(self) -> bool:
        """Check if pgvector supports vector type."""
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

    def format_create_vector_index_statement(
        self,
        index: str,
        table: str,
        column_name: str,
        index_type: str = "hnsw",
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
        lists: Optional[int] = None,
        schema: Optional[str] = None,
        opclass: str = "vector_cosine_ops",
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for vector column.

        Args:
            index: Name of the index
            table: Table name
            column_name: Vector column name
            index_type: Index type - 'hnsw' or 'ivfflat'
            m: HNSW M parameter (max connections per layer)
            ef_construction: HNSW ef_construction parameter
            lists: IVFFlat number of lists
            schema: Optional schema name
            opclass: pgvector operator class for the index; defaults to
                ``vector_cosine_ops``. Use ``vector_l2_ops`` or ``vector_ip_ops``
                for L2/inner-product distance indexes.

        Returns:
            Tuple of (SQL statement, parameters)
        """
        full_table = f"{schema}.{table}" if schema else table

        if index_type.lower() == "hnsw":
            with_clauses = []
            if m is not None:
                with_clauses.append(f"m = {m}")
            if ef_construction is not None:
                with_clauses.append(f"ef_construction = {ef_construction}")
            with_clause = f" WITH ({', '.join(with_clauses)})" if with_clauses else ""
            sql = (
                f"CREATE INDEX {index} ON {full_table} "
                f"USING hnsw ({column_name} {opclass}){with_clause}"
            )
        else:  # ivfflat
            lists_clause = f" WITH (lists = {lists})" if lists else ""
            sql = (
                f"CREATE INDEX {index} ON {full_table} "
                f"USING ivfflat ({column_name} {opclass}){lists_clause}"
            )

        return (sql, ())

    def format_create_hnsw_index_statement(
        self,
        table: str,
        column_name: str,
        index: Optional[str] = None,
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
        opclass: str = "vector_cosine_ops",
    ) -> str:
        """Format CREATE INDEX statement with HNSW index for vector column.

        Args:
            table: Table name
            column_name: Vector column name
            index: Optional index name (auto-generated if not provided)
            m: HNSW M parameter (max connections per layer)
            ef_construction: HNSW ef_construction parameter
            opclass: pgvector operator class for the index; defaults to
                ``vector_cosine_ops``.

        Returns:
            SQL CREATE INDEX statement
        """
        idx_name = index or f"idx_{table}_{column_name}_hnsw"
        with_clauses = []
        if m is not None:
            with_clauses.append(f"m = {m}")
        if ef_construction is not None:
            with_clauses.append(f"ef_construction = {ef_construction}")
        with_clause = f" WITH ({', '.join(with_clauses)})" if with_clauses else ""
        return (
            f"CREATE INDEX {idx_name} ON {table} "
            f"USING hnsw ({column_name} {opclass}){with_clause}"
        )
