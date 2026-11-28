# src/rhosocial/activerecord/backend/impl/postgres/functions/pgvector.py
"""
PostgreSQL pgvector function factories.

This module provides SQL expression generators for pgvector distance
operators and type construction. All functions return Expression objects
that integrate with the Expression/Dialect architecture.

pgvector Documentation: https://github.com/pgvector/pgvector

Distance operators:
- <-> : L2 (Euclidean) distance
- <=> : Cosine distance
- <#> : Inner product (negative)

NOTE: The operator factories treat plain string arguments as literal values,
NOT column references. Pass ``Column(dialect, name)`` (or use ``vector_distance``)
to reference a vector column.

The vector type requires the pgvector extension:
    CREATE EXTENSION IF NOT EXISTS vector;
"""

from typing import List, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import (
    ArithmeticOperation,
    BinaryExpression,
    RawSQL,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.pgvector import PostgresVector
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresVectorType,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


_METRIC_OPERATORS = {
    "cosine": "<=>",
    "l2": "<->",
    "ip": "<#>",
}

def _typed_vector(
    dialect: "SQLDialectBase",
    text: str,
    dimensions: Optional[int],
) -> "bases.BaseExpression":
    """The vector text as a value node, cast to the vector type it spells.

    ``Parameter`` is the value node: it carries the value and the
    bind-or-inline flag, and the casting operation is not part of it
    (expression-dialect §4.3 -- a value node declares no result type).
    The cast is therefore its own node, built here once instead of being
    chained onto the literal at each call site.
    """
    target = (
        PostgresVectorType(dialect, dimensions)
        if dimensions is not None
        else PostgresVectorType(dialect)
    )
    return core.CastExpression(dialect, core.Parameter(dialect, text), target)




# === Distance Operators ===

def vector_l2_distance(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryExpression:
    """L2 (Euclidean) distance operator: left <-> right.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (vector, string literal, or expression); plain
            strings are literal values, use ``Column(dialect, name)`` for columns
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryExpression for the distance calculation

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> d = PostgresDialect()
        >>> expr = vector_l2_distance(d, "embedding", [1.0, 2.0, 3.0])
    """
    if isinstance(left, bases.BaseExpression):
        left_expr = left
    elif isinstance(left, PostgresVector):
        left_expr = _typed_vector(
            dialect, left.to_postgres_string(), left.dimensions
        )
    elif isinstance(left, list):
        vec = PostgresVector(values=left)
        left_expr = _typed_vector(
            dialect, vec.to_postgres_string(), vec.dimensions
        )
    else:
        left_expr = core.Parameter(dialect, left)

    if isinstance(right, bases.BaseExpression):
        right_expr = right
    elif isinstance(right, PostgresVector):
        right_expr = _typed_vector(
            dialect, right.to_postgres_string(), right.dimensions
        )
    elif isinstance(right, list):
        vec = PostgresVector(values=right)
        right_expr = _typed_vector(
            dialect, vec.to_postgres_string(), vec.dimensions
        )
    else:
        right_expr = core.Parameter(dialect, right)

    return BinaryExpression(dialect, "<->", left_expr, right_expr)


def vector_cosine_distance(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Cosine distance operator: left <=> right.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (vector, string literal, or expression); plain
            strings are literal values, use ``Column(dialect, name)`` for columns
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryExpression for the cosine distance
    """
    if isinstance(left, bases.BaseExpression):
        left_expr = left
    elif isinstance(left, PostgresVector):
        left_expr = _typed_vector(
            dialect, left.to_postgres_string(), left.dimensions
        )
    elif isinstance(left, list):
        vec = PostgresVector(values=left)
        left_expr = _typed_vector(
            dialect, vec.to_postgres_string(), vec.dimensions
        )
    else:
        left_expr = core.Parameter(dialect, left)

    if isinstance(right, bases.BaseExpression):
        right_expr = right
    elif isinstance(right, PostgresVector):
        right_expr = _typed_vector(
            dialect, right.to_postgres_string(), right.dimensions
        )
    elif isinstance(right, list):
        vec = PostgresVector(values=right)
        right_expr = _typed_vector(
            dialect, vec.to_postgres_string(), vec.dimensions
        )
    else:
        right_expr = core.Parameter(dialect, right)

    return BinaryExpression(dialect, "<=>", left_expr, right_expr)


def vector_inner_product(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryExpression:
    """Inner product (negative) operator: left <#> right.

    Note: pgvector returns the negative inner product, so values are
    sorted in descending order for nearest-neighbor search.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (vector, string literal, or expression); plain
            strings are literal values, use ``Column(dialect, name)`` for columns
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryExpression for the inner product
    """
    if isinstance(left, bases.BaseExpression):
        left_expr = left
    elif isinstance(left, PostgresVector):
        left_expr = _typed_vector(
            dialect, left.to_postgres_string(), left.dimensions
        )
    elif isinstance(left, list):
        vec = PostgresVector(values=left)
        left_expr = _typed_vector(
            dialect, vec.to_postgres_string(), vec.dimensions
        )
    else:
        left_expr = core.Parameter(dialect, left)

    if isinstance(right, bases.BaseExpression):
        right_expr = right
    elif isinstance(right, PostgresVector):
        right_expr = _typed_vector(
            dialect, right.to_postgres_string(), right.dimensions
        )
    elif isinstance(right, list):
        vec = PostgresVector(values=right)
        right_expr = _typed_vector(
            dialect, vec.to_postgres_string(), vec.dimensions
        )
    else:
        right_expr = core.Parameter(dialect, right)

    return BinaryExpression(dialect, "<#>", left_expr, right_expr)


def vector_distance(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    query_vector: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    metric: str = "cosine",
):
    """Collect arguments for a vector distance operator; the formatter dispatches.

    Unlike the raw operator factories, a plain string ``column`` is wrapped in
    ``Column`` (a column reference), so passing a column name is safe.

    Args:
        dialect: The SQL dialect instance
        column: Vector column name (string) or column expression
        query_vector: The query vector (``PostgresVector``, ``List[float]``,
            str, or expression)
        metric: Distance metric - 'cosine' | 'l2' | 'ip'

    Returns:
        BinaryExpression for the requested distance
    """
    from ..expression.ddl import PostgresVectorDistanceExpression

    return PostgresVectorDistanceExpression(
        dialect, column, query_vector, metric
    )


# === Similarity ===

def vector_cosine_similarity(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    query_vector: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> ArithmeticOperation:
    """Cosine similarity expression: 1 - (column <=> query_vector).

    This converts cosine distance to cosine similarity by subtracting
    from 1. A similarity of 1.0 means identical direction.

    Args:
        dialect: The SQL dialect instance
        column: The vector column name or expression
        query_vector: The query vector to compare against

    Returns:
        ArithmeticOperation for cosine similarity

    Example:
        >>> expr = vector_cosine_similarity(d, "embedding", [1.0, 2.0, 3.0])
    """
    cosine_dist = vector_cosine_distance(dialect, column, query_vector)
    # ``ArithmeticOperation`` only parenthesizes an ``ArithmeticOperation``
    # operand; a ``BinaryExpression`` carrying ``<=>`` is not one, so without
    # this wrapper it renders ``1 - col <=> x`` and the server reads that as
    # ``(1 - col) <=> x``. The text is ours (a rendered expression, never user
    # input) and the bind parameters are carried through unchanged.
    dist_sql, dist_params = cosine_dist.to_sql()
    grouped_dist = RawSQL(dialect, f"({dist_sql})", dist_params)
    one = core.Parameter(dialect, 1)
    return ArithmeticOperation(dialect, "sub", one, grouped_dist)


# === Type Construction ===

def vector_literal(
    dialect: "SQLDialectBase",
    values: List[float],
    dimensions: Optional[int] = None,
) -> "bases.BaseExpression":
    """Construct a vector literal expression with type cast.

    Args:
        dialect: The SQL dialect instance
        values: List of float values for the vector
        dimensions: Optional dimension count (inferred from values if not provided)

    Returns:
        Expression representing a typed vector literal

    Example:
        >>> expr = vector_literal(d, [1.0, 2.0, 3.0])
        >>> # Produces: '[1.0, 2.0, 3.0]'::vector(3)
    """
    vec = PostgresVector(values=values, dimensions=dimensions)
    return _typed_vector(dialect, vec.to_postgres_string(), vec.dimensions)


__all__ = [
    "vector_l2_distance",
    "vector_cosine_distance",
    "vector_inner_product",
    "vector_distance",
    "vector_cosine_similarity",
    "vector_literal",
]
