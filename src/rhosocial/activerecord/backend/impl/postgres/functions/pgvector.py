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
from rhosocial.activerecord.backend.expression.operators import BinaryArithmeticExpression
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


# === Distance Operators ===

def vector_l2_distance(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """L2 (Euclidean) distance operator: left <-> right.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (vector, string literal, or expression); plain
            strings are literal values, use ``Column(dialect, name)`` for columns
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryArithmeticExpression for the distance calculation

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> d = PostgresDialect()
        >>> expr = vector_l2_distance(d, "embedding", [1.0, 2.0, 3.0])
    """
    if isinstance(left, bases.BaseExpression):
        left_expr = left
    elif isinstance(left, PostgresVector):
        literal = core.Literal(dialect, left.to_postgres_string())
        left_expr = literal.cast(
            PostgresVectorType(dialect, left.dimensions)
            if left.dimensions is not None else PostgresVectorType(dialect))
    elif isinstance(left, list):
        vec = PostgresVector(values=left)
        left_expr = core.Literal(dialect, vec.to_postgres_string()).cast(
            PostgresVectorType(dialect, vec.dimensions))
    else:
        left_expr = core.Literal(dialect, left)

    if isinstance(right, bases.BaseExpression):
        right_expr = right
    elif isinstance(right, PostgresVector):
        literal = core.Literal(dialect, right.to_postgres_string())
        right_expr = literal.cast(
            PostgresVectorType(dialect, right.dimensions)
            if right.dimensions is not None else PostgresVectorType(dialect))
    elif isinstance(right, list):
        vec = PostgresVector(values=right)
        right_expr = core.Literal(dialect, vec.to_postgres_string()).cast(
            PostgresVectorType(dialect, vec.dimensions))
    else:
        right_expr = core.Literal(dialect, right)

    return BinaryArithmeticExpression(dialect, "<->", left_expr, right_expr)


def vector_cosine_distance(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Cosine distance operator: left <=> right.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (vector, string literal, or expression); plain
            strings are literal values, use ``Column(dialect, name)`` for columns
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryArithmeticExpression for the cosine distance
    """
    if isinstance(left, bases.BaseExpression):
        left_expr = left
    elif isinstance(left, PostgresVector):
        literal = core.Literal(dialect, left.to_postgres_string())
        left_expr = literal.cast(
            PostgresVectorType(dialect, left.dimensions)
            if left.dimensions is not None else PostgresVectorType(dialect))
    elif isinstance(left, list):
        vec = PostgresVector(values=left)
        left_expr = core.Literal(dialect, vec.to_postgres_string()).cast(
            PostgresVectorType(dialect, vec.dimensions))
    else:
        left_expr = core.Literal(dialect, left)

    if isinstance(right, bases.BaseExpression):
        right_expr = right
    elif isinstance(right, PostgresVector):
        literal = core.Literal(dialect, right.to_postgres_string())
        right_expr = literal.cast(
            PostgresVectorType(dialect, right.dimensions)
            if right.dimensions is not None else PostgresVectorType(dialect))
    elif isinstance(right, list):
        vec = PostgresVector(values=right)
        right_expr = core.Literal(dialect, vec.to_postgres_string()).cast(
            PostgresVectorType(dialect, vec.dimensions))
    else:
        right_expr = core.Literal(dialect, right)

    return BinaryArithmeticExpression(dialect, "<=>", left_expr, right_expr)


def vector_inner_product(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Inner product (negative) operator: left <#> right.

    Note: pgvector returns the negative inner product, so values are
    sorted in descending order for nearest-neighbor search.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (vector, string literal, or expression); plain
            strings are literal values, use ``Column(dialect, name)`` for columns
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryArithmeticExpression for the inner product
    """
    if isinstance(left, bases.BaseExpression):
        left_expr = left
    elif isinstance(left, PostgresVector):
        literal = core.Literal(dialect, left.to_postgres_string())
        left_expr = literal.cast(
            PostgresVectorType(dialect, left.dimensions)
            if left.dimensions is not None else PostgresVectorType(dialect))
    elif isinstance(left, list):
        vec = PostgresVector(values=left)
        left_expr = core.Literal(dialect, vec.to_postgres_string()).cast(
            PostgresVectorType(dialect, vec.dimensions))
    else:
        left_expr = core.Literal(dialect, left)

    if isinstance(right, bases.BaseExpression):
        right_expr = right
    elif isinstance(right, PostgresVector):
        literal = core.Literal(dialect, right.to_postgres_string())
        right_expr = literal.cast(
            PostgresVectorType(dialect, right.dimensions)
            if right.dimensions is not None else PostgresVectorType(dialect))
    elif isinstance(right, list):
        vec = PostgresVector(values=right)
        right_expr = core.Literal(dialect, vec.to_postgres_string()).cast(
            PostgresVectorType(dialect, vec.dimensions))
    else:
        right_expr = core.Literal(dialect, right)

    return BinaryArithmeticExpression(dialect, "<#>", left_expr, right_expr)


def vector_distance(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    query_vector: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    metric: str = "cosine",
) -> BinaryArithmeticExpression:
    """Distance operator for a vector column, dispatching by metric.

    Unlike the raw operator factories, a plain string ``column`` is wrapped in
    ``Column`` (a column reference), so passing a column name is safe.

    Args:
        dialect: The SQL dialect instance
        column: Vector column name (string) or column expression
        query_vector: The query vector (``PostgresVector``, ``List[float]``,
            str, or expression)
        metric: Distance metric - 'cosine' | 'l2' | 'ip'

    Returns:
        BinaryArithmeticExpression for the requested distance
    """
    if metric not in _METRIC_OPERATORS:
        raise ValueError(
            f"Unsupported vector metric '{metric}'; expected one of "
            f"{tuple(_METRIC_OPERATORS)}"
        )
    column_expr = (
        core.Column(dialect, column) if isinstance(column, str) else column
    )
    if isinstance(query_vector, bases.BaseExpression):
        query_expr = query_vector
    elif isinstance(query_vector, PostgresVector):
        literal = core.Literal(dialect, query_vector.to_postgres_string())
        query_expr = literal.cast(
            PostgresVectorType(dialect, query_vector.dimensions)
            if query_vector.dimensions is not None else PostgresVectorType(dialect))
    elif isinstance(query_vector, list):
        vec = PostgresVector(values=query_vector)
        query_expr = core.Literal(dialect, vec.to_postgres_string()).cast(
            PostgresVectorType(dialect, vec.dimensions))
    else:
        query_expr = core.Literal(dialect, query_vector)

    return BinaryArithmeticExpression(
        dialect, _METRIC_OPERATORS[metric],
        column_expr, query_expr,
    )


# === Similarity ===

def vector_cosine_similarity(
    dialect: "SQLDialectBase",
    column: Union[str, "bases.BaseExpression"],
    query_vector: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Cosine similarity expression: 1 - (column <=> query_vector).

    This converts cosine distance to cosine similarity by subtracting
    from 1. A similarity of 1.0 means identical direction.

    Args:
        dialect: The SQL dialect instance
        column: The vector column name or expression
        query_vector: The query vector to compare against

    Returns:
        BinaryArithmeticExpression for cosine similarity

    Example:
        >>> expr = vector_cosine_similarity(d, "embedding", [1.0, 2.0, 3.0])
    """
    cosine_dist = vector_cosine_distance(dialect, column, query_vector)
    one = core.Literal(dialect, 1)
    return BinaryArithmeticExpression(dialect, "-", one, cosine_dist)


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
    literal = core.Literal(dialect, vec.to_postgres_string())
    if vec.dimensions is not None:
        return literal.cast(PostgresVectorType(dialect, vec.dimensions))
    return literal.cast(PostgresVectorType(dialect))


__all__ = [
    "vector_l2_distance",
    "vector_cosine_distance",
    "vector_inner_product",
    "vector_distance",
    "vector_cosine_similarity",
    "vector_literal",
]
