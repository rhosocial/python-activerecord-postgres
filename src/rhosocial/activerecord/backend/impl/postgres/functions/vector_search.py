# src/rhosocial/activerecord/backend/impl/postgres/functions/vector_search.py
"""pgvector factories — argument collection only.

Validation and SQL assembly live in ``PostgresPgvectorMixin`` formatters.
"""

from typing import List, Optional, Union

from rhosocial.activerecord.backend.expression.bases import BaseExpression

from ..expression.ddl import (
    PostgresVectorIndexExpression,
    PostgresVectorSearchExpression,
)


def vector_search(
    dialect,
    table: str,
    query_vector,
    metric: str = "cosine",
    top_k: int = 10,
    *,
    vector_column: str = "embedding",
    columns: Optional[List[Union[str, BaseExpression]]] = None,
    where=None,
    include_distance: bool = True,
    include_similarity: bool = False,
) -> PostgresVectorSearchExpression:
    """Collect arguments for a pgvector similarity search."""
    return PostgresVectorSearchExpression(
        dialect, table, query_vector, metric, top_k,
        vector_column=vector_column, columns=columns, where=where,
        include_distance=include_distance, include_similarity=include_similarity,
    )


def create_vector_index(
    dialect,
    table_name: str,
    column_name: str = "embedding",
    metric: str = "cosine",
    index_type: str = "hnsw",
    *,
    index_name: Optional[str] = None,
    m: Optional[int] = None,
    ef_construction: Optional[int] = None,
    lists: Optional[int] = None,
    if_not_exists: bool = True,
) -> PostgresVectorIndexExpression:
    """Collect arguments for a pgvector index."""
    return PostgresVectorIndexExpression(
        dialect, table_name, column_name, metric, index_type,
        index_name=index_name, m=m, ef_construction=ef_construction,
        lists=lists, if_not_exists=if_not_exists,
    )


__all__ = ["vector_search", "create_vector_index"]
