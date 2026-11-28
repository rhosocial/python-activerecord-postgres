# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgvector.py
"""pgvector capability probes and query/index renderers.

The nodes only carry arguments; validation and SQL assembly happen here.
"""

import warnings
from typing import Tuple

from rhosocial.activerecord.backend.expression import Column, Parameter, QueryStatement
from rhosocial.activerecord.backend.expression.core import AllColumnsWildcard, Subquery
from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.expression.operators import (
    ArithmeticOperation,
    BinaryExpression,
)
from rhosocial.activerecord.backend.expression.query_parts import (
    LimitOffsetClause,
    OrderByClause,
)

from ...expression.ddl import (
    PostgresCreateIndexExpression,
    PostgresIndexType,
    PostgresVectorIndexExpression,
    PostgresVectorSearchExpression,
    PostgresVectorDistanceExpression,
)
from ...functions.pgvector import (
    vector_cosine_distance,
    vector_inner_product,
    vector_l2_distance,
)

_DISTANCE = {
    "cosine": vector_cosine_distance,
    "l2": vector_l2_distance,
    "ip": vector_inner_product,
}
_DISTANCE_ALIAS = {
    "cosine": "cosine_distance", "l2": "l2_distance", "ip": "inner_product",
}
_SIMILARITY_ALIAS = {"cosine": "cosine_similarity", "l2": "l2_similarity"}
_OPCLASS = {
    "cosine": "vector_cosine_ops", "l2": "vector_l2_ops", "ip": "vector_ip_ops",
}
_DISTANCE_OPERATOR = {"cosine": "<=>", "l2": "<->", "ip": "<#>"}


class PostgresPgvectorMixin:
    """pgvector vector similarity search implementation."""

    def supports_pgvector_type(self) -> bool:
        return self.check_extension_feature("vector", "type")

    def supports_pgvector_similarity_search(self) -> bool:
        return self.check_extension_feature("vector", "similarity_search")

    def supports_pgvector_ivfflat_index(self) -> bool:
        return self.check_extension_feature("vector", "ivfflat_index")

    def supports_pgvector_hnsw_index(self) -> bool:
        return self.check_extension_feature("vector", "hnsw_index")

    def format_postgres_vector_distance_expression(
        self, expr: PostgresVectorDistanceExpression
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression import bases, core

        from ...functions.pgvector import PostgresVector, _typed_vector

        metric = (
            expr.metric.strip().lower()
            if isinstance(expr.metric, str)
            else expr.metric
        )
        if metric not in _DISTANCE_OPERATOR:
            raise ValueError(
                f"Unsupported vector metric {expr.metric!r}; expected cosine/l2/ip"
            )
        column_expr = (
            Column(self, expr.column)
            if isinstance(expr.column, str)
            else expr.column
        )
        if isinstance(expr.query_vector, bases.BaseExpression):
            query_expr = expr.query_vector
        elif isinstance(expr.query_vector, PostgresVector):
            query_expr = _typed_vector(
                self,
                expr.query_vector.to_postgres_string(),
                expr.query_vector.dimensions,
            )
        elif isinstance(expr.query_vector, list):
            vec = PostgresVector(values=expr.query_vector)
            query_expr = _typed_vector(
                self, vec.to_postgres_string(), vec.dimensions
            )
        else:
            query_expr = core.Parameter(self, expr.query_vector)
        return BinaryExpression(
            self, _DISTANCE_OPERATOR[metric], column_expr, query_expr
        ).to_sql()

    def format_postgres_vector_index_expression(
        self, expr: PostgresVectorIndexExpression
    ) -> Tuple[str, tuple]:
        metric = (
            expr.metric.strip().lower()
            if isinstance(expr.metric, str)
            else expr.metric
        )
        if metric not in _DISTANCE:
            raise ValueError(
                f"Unsupported vector metric {expr.metric!r}; expected cosine/l2/ip"
            )
        method = PostgresIndexType.normalize(expr.index_type).value
        if method not in ("hnsw", "ivfflat"):
            raise ValueError(
                f"Unsupported vector index type {expr.index_type!r}; "
                "expected 'hnsw' or 'ivfflat'"
            )
        name = expr.index_name or f"idx_{expr.table_name}_{expr.column_name}_{method}"
        with_options = {}
        if method == "hnsw":
            if expr.m is not None:
                with_options["m"] = expr.m
            if expr.ef_construction is not None:
                with_options["ef_construction"] = expr.ef_construction
        elif expr.lists is not None:
            with_options["lists"] = expr.lists
        return PostgresCreateIndexExpression(
            self,
            index=Index(self, name),
            table=Table(self, expr.table_name),
            columns=[expr.column_name],
            index_type=method.upper(),
            if_not_exists=expr.if_not_exists,
            opclasses={expr.column_name: _OPCLASS[metric]},
            with_options=with_options,
        ).to_sql()

    def format_postgres_vector_search_expression(
        self, expr: PostgresVectorSearchExpression
    ) -> Tuple[str, tuple]:
        metric = (
            expr.metric.strip().lower()
            if isinstance(expr.metric, str)
            else expr.metric
        )
        if metric not in _DISTANCE:
            raise ValueError(
                f"Unsupported vector metric {expr.metric!r}; expected cosine/l2/ip"
            )
        if expr.top_k <= 0:
            raise ValueError("top_k must be positive")

        vector_col = Column(self, expr.vector_column)
        distance = _DISTANCE[metric](self, vector_col, expr.query_vector)
        select = (
            [AllColumnsWildcard(self)]
            if expr.columns is None
            else [
                Column(self, c) if isinstance(c, str) else c for c in expr.columns
            ]
        )
        if expr.include_distance:
            select.append(
                Subquery(self, distance).as_(_DISTANCE_ALIAS[metric])
            )
        if expr.include_similarity:
            if metric not in _SIMILARITY_ALIAS:
                warnings.warn(
                    f"include_similarity is defined for cosine and l2, not "
                    f"{expr.metric!r}; no similarity column was added",
                    stacklevel=2,
                )
            else:
                similarity = ArithmeticOperation(
                    self, "sub", Parameter(self, 1), Subquery(self, distance)
                )
                select.append(similarity.as_(_SIMILARITY_ALIAS[metric]))

        return QueryStatement(
            dialect=self,
            select=select,
            from_=Table(self, expr.table),
            where=expr.where,
            order_by=OrderByClause(self, [distance]),
            limit_offset=LimitOffsetClause(self, limit=expr.top_k),
        ).to_sql()
