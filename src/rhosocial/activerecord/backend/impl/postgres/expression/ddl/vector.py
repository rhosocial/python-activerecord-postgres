# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/vector.py
"""pgvector query/index nodes — argument bags only, no validation."""

from typing import Any, List, Optional

from rhosocial.activerecord.backend.expression.bases import BaseExpression


class PostgresVectorIndexExpression(BaseExpression):
    def __init__(
        self, dialect, table_name, column_name="embedding", metric="cosine",
        index_type="hnsw", index_name=None, m=None, ef_construction=None,
        lists=None, if_not_exists=True,
    ):
        super().__init__(dialect)
        self.table_name = table_name
        self.column_name = column_name
        self.metric = metric
        self.index_type = index_type
        self.index_name = index_name
        self.m = m
        self.ef_construction = ef_construction
        self.lists = lists
        self.if_not_exists = if_not_exists

    @property
    def format_method(self) -> str:
        return "format_postgres_vector_index_expression"


class PostgresVectorSearchExpression(BaseExpression):
    def __init__(
        self, dialect, table, query_vector, metric="cosine", top_k=10,
        vector_column="embedding", columns=None, where=None,
        include_distance=True, include_similarity=False,
    ):
        super().__init__(dialect)
        self.table = table
        self.query_vector = query_vector
        self.metric = metric
        self.top_k = top_k
        self.vector_column = vector_column
        self.columns: Optional[List[Any]] = columns
        self.where = where
        self.include_distance = include_distance
        self.include_similarity = include_similarity

    @property
    def format_method(self) -> str:
        return "format_postgres_vector_search_expression"


class PostgresVectorDistanceExpression(BaseExpression):
    """A distance operator chosen at render time from the raw metric name."""

    def __init__(self, dialect, column, query_vector, metric="cosine"):
        super().__init__(dialect)
        self.column = column
        self.query_vector = query_vector
        self.metric = metric

    @property
    def format_method(self) -> str:
        return "format_postgres_vector_distance_expression"


__all__ = [
    "PostgresVectorDistanceExpression",
    "PostgresVectorIndexExpression",
    "PostgresVectorSearchExpression",
]
