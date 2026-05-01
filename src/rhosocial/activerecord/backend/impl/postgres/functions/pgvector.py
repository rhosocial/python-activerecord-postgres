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

The vector type requires the pgvector extension:
    CREATE EXTENSION IF NOT EXISTS vector;
"""

from typing import List, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core
from rhosocial.activerecord.backend.expression.operators import BinaryArithmeticExpression
from rhosocial.activerecord.backend.impl.postgres.types.pgvector import PostgresVector

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports PostgresVector objects, List[float], strings, and
    existing BaseExpression objects.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, PostgresVector):
        literal = core.Literal(dialect, expr.to_postgres_string())
        if expr.dimensions is not None:
            return literal.cast(f"vector({expr.dimensions})")
        return literal.cast("vector")
    elif isinstance(expr, list):
        vec = PostgresVector(values=expr)
        literal = core.Literal(dialect, vec.to_postgres_string())
        return literal.cast(f"vector({vec.dimensions})")
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# === Distance Operators ===

def vector_l2_distance(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """L2 (Euclidean) distance operator: left <-> right.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (column name, vector, or expression)
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryArithmeticExpression for the distance calculation

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> d = PostgresDialect()
        >>> expr = vector_l2_distance(d, "embedding", [1.0, 2.0, 3.0])
    """
    left_expr = _convert_to_expression(dialect, left)
    right_expr = _convert_to_expression(dialect, right)
    return BinaryArithmeticExpression(dialect, "<->", left_expr, right_expr)


def vector_cosine_distance(
    dialect: "SQLDialectBase",
    left: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
    right: Union[PostgresVector, List[float], str, "bases.BaseExpression"],
) -> BinaryArithmeticExpression:
    """Cosine distance operator: left <=> right.

    Args:
        dialect: The SQL dialect instance
        left: Left operand (column name, vector, or expression)
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryArithmeticExpression for the cosine distance
    """
    left_expr = _convert_to_expression(dialect, left)
    right_expr = _convert_to_expression(dialect, right)
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
        left: Left operand (column name, vector, or expression)
        right: Right operand (vector literal, or expression)

    Returns:
        BinaryArithmeticExpression for the inner product
    """
    left_expr = _convert_to_expression(dialect, left)
    right_expr = _convert_to_expression(dialect, right)
    return BinaryArithmeticExpression(dialect, "<#>", left_expr, right_expr)


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
    return _convert_to_expression(dialect, vec)


__all__ = [
    "vector_l2_distance",
    "vector_cosine_distance",
    "vector_inner_product",
    "vector_cosine_similarity",
    "vector_literal",
]
