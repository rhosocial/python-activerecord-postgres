# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgvector.py
"""
pgvector vector similarity search implementation.

This module provides the PostgresPgvectorMixin class that adds support for
pgvector extension features including vector type, similarity search, and
index types (IVFFlat and HNSW).
"""

from typing import TYPE_CHECKING, List, Tuple, Optional

if TYPE_CHECKING:
    pass  # No type imports needed for this mixin


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

    def format_vector_literal(self, values: List[float], dimensions: Optional[int] = None) -> str:
        """Format a vector literal value.

        Args:
            values: List of float values representing the vector
            dimensions: Optional number of dimensions (for type cast)

        Returns:
            SQL vector literal string

        Example:
            >>> format_vector_literal([1.0, 2.0, 3.0])
            "[1.0, 2.0, 3.0]"
            >>> format_vector_literal([1.0, 2.0, 3.0], dimensions=3)
            "[1.0, 2.0, 3.0]::vector(3)"
        """
        values_str = ", ".join(str(v) for v in values)
        literal = f"[{values_str}]"
        if dimensions is not None:
            literal += f"::vector({dimensions})"
        return literal

    def format_vector_similarity_expression(self, column: str, query_vector: str, distance_metric: str = "l2") -> str:
        """Format a vector similarity/distance expression.

        Args:
            column: The vector column name
            query_vector: The query vector literal or parameter
            distance_metric: Distance metric - 'l2' (Euclidean), 'ip' (inner product), or 'cosine'

        Returns:
            SQL distance expression

        Example:
            >>> format_vector_similarity_expression('embedding', '[1,2,3]', 'l2')
            "embedding <-> '[1,2,3]'"
            >>> format_vector_similarity_expression('embedding', '[1,2,3]', 'cosine')
            "embedding <=> '[1,2,3]'"
        """
        operators = {
            "l2": "<->",  # Euclidean distance
            "ip": "<#>",  # Inner product (negative)
            "cosine": "<=>",  # Cosine distance
        }
        op = operators.get(distance_metric, "<->")
        return f"{column} {op} '{query_vector}'"

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
        """Format CREATE INDEX statement for vector column.

        Args:
            index_name: Name of the index
            table_name: Table name
            column_name: Vector column name
            index_type: Index type - 'hnsw' or 'ivfflat'
            m: HNSW M parameter (max connections per layer)
            ef_construction: HNSW ef_construction parameter
            lists: IVFFlat number of lists
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_create_vector_index_statement(
            ...     'idx_embedding', 'items', 'embedding', 'hnsw', m=16, ef_construction=64
            ... )
            ("CREATE INDEX idx_embedding ON items USING hnsw (embedding vector_cosine_ops) "
             "WITH (m = 16, ef_construction = 64)", ())
        """
        full_table = f"{schema}.{table_name}" if schema else table_name

        if index_type.lower() == "hnsw":
            with_clauses = []
            if m is not None:
                with_clauses.append(f"m = {m}")
            if ef_construction is not None:
                with_clauses.append(f"ef_construction = {ef_construction}")
            with_clause = f" WITH ({', '.join(with_clauses)})" if with_clauses else ""
            sql = (
                f"CREATE INDEX {index_name} ON {full_table} "
                f"USING hnsw ({column_name} vector_cosine_ops){with_clause}"
            )
        else:  # ivfflat
            lists_clause = f" WITH (lists = {lists})" if lists else ""
            sql = (
                f"CREATE INDEX {index_name} ON {full_table} "
                f"USING ivfflat ({column_name} vector_cosine_ops){lists_clause}"
            )

        return (sql, ())

    def format_vector_cosine_similarity(self, column: str, query_vector: str) -> str:
        """Format cosine similarity expression (1 - cosine_distance).

        Args:
            column: The vector column name
            query_vector: The query vector literal

        Returns:
            SQL similarity expression

        Example:
            >>> format_vector_cosine_similarity('embedding', '[1,2,3]')
            "1 - (embedding <=> '[1,2,3]')"
        """
        return f"1 - ({column} <=> '{query_vector}')"

    def format_create_hnsw_index_statement(
        self,
        table_name: str,
        column_name: str,
        index_name: Optional[str] = None,
        m: Optional[int] = None,
        ef_construction: Optional[int] = None,
    ) -> str:
        """Format CREATE INDEX statement with HNSW index for vector column.

        Args:
            table_name: Table name
            column_name: Vector column name
            index_name: Optional index name (auto-generated if not provided)
            m: HNSW M parameter (max connections per layer)
            ef_construction: HNSW ef_construction parameter

        Returns:
            SQL CREATE INDEX statement

        Example:
            >>> format_create_hnsw_index_statement('items', 'embedding', m=16, ef_construction=64)
            "CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
        """
        idx_name = index_name or f"idx_{table_name}_{column_name}_hnsw"
        with_clauses = []
        if m is not None:
            with_clauses.append(f"m = {m}")
        if ef_construction is not None:
            with_clauses.append(f"ef_construction = {ef_construction}")
        with_clause = f" WITH ({', '.join(with_clauses)})" if with_clauses else ""
        return (
            f"CREATE INDEX {idx_name} ON {table_name} "
            f"USING hnsw ({column_name} vector_cosine_ops){with_clause}"
        )

    def format_vector_l2_distance(self, left: str, right: str) -> str:
        """Format L2 (Euclidean) distance expression.

        Args:
            left: Left vector expression
            right: Right vector expression

        Returns:
            SQL L2 distance expression

        Example:
            >>> format_vector_l2_distance('embedding', '[1,2,3]')
            "embedding <-> '[1,2,3]'"
        """
        return f"{left} <-> '{right}'"

    def format_vector_inner_product(self, left: str, right: str) -> str:
        """Format inner product expression.

        Args:
            left: Left vector expression
            right: Right vector expression

        Returns:
            SQL inner product expression

        Example:
            >>> format_vector_inner_product('embedding', '[1,2,3]')
            "embedding <#> '[1,2,3]'"
        """
        return f"{left} <#> '{right}'"
