# src/rhosocial/activerecord/backend/impl/postgres/functions/vector_search.py
"""
PostgreSQL pgvector similarity-search helpers.

High-level helpers built on top of the pgvector operator factories in
``functions/pgvector.py``:

- ``vector_search``: assemble a complete similarity-search
  ``QueryExpression`` (SELECT distance/similarity, WHERE filters,
  ORDER BY distance, LIMIT top_k).
- ``create_vector_index``: assemble a ``CreateIndexExpression`` for
  HNSW/IVFFlat with metric-to-operator-class mapping.

They target the RAG knowledge-base pattern:
    index(chunk/tenant/meta/vector) + search(query_vector, top_k, filters)

Requires the pgvector extension: CREATE EXTENSION IF NOT EXISTS vector;
"""

from typing import List, Optional, Union, Dict, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import (
    Column,
    Literal,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.core import (
    BaseExpression,
    WildcardExpression,
)
from rhosocial.activerecord.backend.expression.operators import BinaryArithmeticExpression
from rhosocial.activerecord.backend.expression.query_parts import (
    LimitOffsetClause,
    OrderByClause,
)
from rhosocial.activerecord.backend.expression.query_parts import (
    SQLPredicate,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.statements.ddl_index import (
    CreateIndexExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

from .pgvector import (
    vector_cosine_distance,
    vector_inner_product,
    vector_l2_distance,
)

__all__ = ["vector_search", "create_vector_index"]

VECTOR_METRICS = ("cosine", "l2", "ip")

_METRIC_DISTANCE_FN: Dict[str, object] = {
    "cosine": vector_cosine_distance,
    "l2": vector_l2_distance,
    "ip": vector_inner_product,
}

_METRIC_DISTANCE_ALIAS = {
    "cosine": "cosine_distance",
    "l2": "l2_distance",
    "ip": "inner_product",
}

_METRIC_SIMILARITY_ALIAS = {
    "cosine": "cosine_similarity",
    "l2": "l2_similarity",
}

_METRIC_OPCLASS = {
    "cosine": "vector_cosine_ops",
    "l2": "vector_l2_ops",
    "ip": "vector_ip_ops",
}


def _validate_metric(metric: str) -> None:
    if metric not in VECTOR_METRICS:
        raise ValueError(
            f"Unsupported vector metric '{metric}'; expected one of {VECTOR_METRICS}"
        )


def vector_search(
    dialect: "SQLDialectBase",
    table: str,
    query_vector,
    metric: str = "cosine",
    top_k: int = 10,
    *,
    vector_column: str = "embedding",
    columns: Optional[List[Union[str, BaseExpression]]] = None,
    where: Optional[Union[SQLPredicate, WhereClause]] = None,
    include_distance: bool = True,
    include_similarity: bool = False,
) -> QueryExpression:
    """Build a pgvector similarity-search query.

    Args:
        dialect: The SQL dialect instance
        table: Table name to search
        query_vector: Query vector (``PostgresVector``, ``List[float]``, str,
            or expression)
        metric: Distance metric - 'cosine' | 'l2' | 'ip'
        top_k: Number of nearest neighbors to return (LIMIT)
        vector_column: Vector column name (default 'embedding')
        columns: Select list; strings become columns, expressions pass through.
            Defaults to ``SELECT *``.
        where: Optional filter predicate (e.g. tenant/permission scoping)
        include_distance: Include an aliased distance column in SELECT
        include_similarity: Include ``1 - distance`` for cosine/l2 in SELECT

    Returns:
        A ready-to-execute ``QueryExpression``.
    """
    _validate_metric(metric)
    if top_k <= 0:
        raise ValueError("top_k must be positive")

    vector_col = Column(dialect, vector_column)
    distance_expr = _METRIC_DISTANCE_FN[metric](dialect, vector_col, query_vector)

    if columns is None:
        select: List[BaseExpression] = [WildcardExpression(dialect)]
    else:
        select = [
            Column(dialect, c) if isinstance(c, str) else c for c in columns
        ]

    if include_distance:
        select.append(distance_expr.as_(_METRIC_DISTANCE_ALIAS[metric]))
    if include_similarity and metric in _METRIC_SIMILARITY_ALIAS:
        similarity = BinaryArithmeticExpression(
            dialect, "-", Literal(dialect, 1), distance_expr
        )
        select.append(similarity.as_(_METRIC_SIMILARITY_ALIAS[metric]))

    return QueryExpression(
        dialect=dialect,
        select=select,
        from_=TableExpression(dialect, table),
        where=where,
        order_by=OrderByClause(dialect, [distance_expr]),
        limit_offset=LimitOffsetClause(dialect, limit=top_k),
    )


def create_vector_index(
    dialect: "SQLDialectBase",
    table: str,
    column_name: str = "embedding",
    metric: str = "cosine",
    index_type: str = "hnsw",
    *,
    index: Optional[str] = None,
    m: Optional[int] = None,
    ef_construction: Optional[int] = None,
    lists: Optional[int] = None,
    if_not_exists: bool = True,
) -> CreateIndexExpression:
    """Build a pgvector index creation expression.

    Args:
        dialect: The SQL dialect instance
        table: Table name
        column_name: Vector column name (default 'embedding')
        metric: Distance metric - 'cosine' | 'l2' | 'ip' (maps to opclass)
        index_type: Index type - 'hnsw' | 'ivfflat'
        index: Optional index name (auto-generated if not provided)
        m: HNSW max connections per layer
        ef_construction: HNSW ef_construction
        lists: IVFFlat number of lists
        if_not_exists: Add IF NOT EXISTS

    Returns:
        A ready-to-execute ``CreateIndexExpression``.

    Note:
        IVFFlat requires the table to contain data before the index can be
        created (HNSW has no such requirement). Create the index after
        inserting rows when using ``index_type='ivfflat'``.
    """
    _validate_metric(metric)
    index_type_l = index_type.lower()
    if index_type_l not in ("hnsw", "ivfflat"):
        raise ValueError(f"Unsupported vector index type '{index_type}'; "
                         "expected 'hnsw' or 'ivfflat'")

    idx_name = index or f"idx_{table}_{column_name}_{index_type_l}"
    with_options: Dict[str, int] = {}
    if index_type_l == "hnsw":
        if m is not None:
            with_options["m"] = m
        if ef_construction is not None:
            with_options["ef_construction"] = ef_construction
    else:
        if lists is not None:
            with_options["lists"] = lists

    return CreateIndexExpression(
        dialect=dialect,
        index=idx_name,
        table=table,
        columns=[column_name],
        index_type=index_type_l.upper(),
        if_not_exists=if_not_exists,
        dialect_options={
            "opclasses": {column_name: _METRIC_OPCLASS[metric]},
            "with": with_options,
        },
    )
