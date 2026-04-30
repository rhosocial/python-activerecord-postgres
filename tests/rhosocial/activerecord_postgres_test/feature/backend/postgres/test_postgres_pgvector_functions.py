# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_pgvector_functions.py
"""
Tests for PostgreSQL pgvector function factories.

Functions: vector_l2_distance, vector_cosine_distance, vector_inner_product,
           vector_cosine_similarity, vector_literal
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.pgvector import (
    vector_l2_distance,
    vector_cosine_distance,
    vector_inner_product,
    vector_cosine_similarity,
    vector_literal,
)
from rhosocial.activerecord.backend.impl.postgres.types.pgvector import PostgresVector
from rhosocial.activerecord.backend.expression.operators import BinaryArithmeticExpression
from rhosocial.activerecord.backend.expression import bases


class TestPgvectorDistanceFunctions:
    """Tests for pgvector distance operator functions."""

    def test_vector_l2_distance_strings(self, postgres_dialect: PostgresDialect):
        """Test L2 distance with string inputs."""
        result = vector_l2_distance(postgres_dialect, "embedding", "[1.0,2.0,3.0]")
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<->" in sql

    def test_vector_cosine_distance_strings(self, postgres_dialect: PostgresDialect):
        """Test cosine distance with string inputs."""
        result = vector_cosine_distance(postgres_dialect, "embedding", "[1.0,2.0,3.0]")
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<=>" in sql

    def test_vector_inner_product_strings(self, postgres_dialect: PostgresDialect):
        """Test inner product with string inputs."""
        result = vector_inner_product(postgres_dialect, "embedding", "[1.0,2.0,3.0]")
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<#>" in sql

    def test_vector_l2_distance_with_postgresvector(self, postgres_dialect: PostgresDialect):
        """Test L2 distance with PostgresVector input."""
        vec = PostgresVector([1.0, 2.0, 3.0])
        result = vector_l2_distance(postgres_dialect, "embedding", vec)
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<->" in sql
        assert "vector(3)" in sql

    def test_vector_cosine_distance_with_list(self, postgres_dialect: PostgresDialect):
        """Test cosine distance with List[float] input."""
        result = vector_cosine_distance(postgres_dialect, "embedding", [1.0, 2.0, 3.0])
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<=>" in sql
        assert "vector(3)" in sql

    def test_vector_inner_product_with_list(self, postgres_dialect: PostgresDialect):
        """Test inner product with List[float] input."""
        result = vector_inner_product(postgres_dialect, "embedding", [1.0, 2.0, 3.0])
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<#>" in sql
        assert "vector(3)" in sql

    def test_vector_cosine_similarity(self, postgres_dialect: PostgresDialect):
        """Test cosine similarity expression (1 - cosine_distance)."""
        result = vector_cosine_similarity(postgres_dialect, "embedding", [1.0, 2.0, 3.0])
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "-" in sql
        assert "<=>" in sql


class TestPgvectorLiteralFunction:
    """Tests for vector_literal function."""

    def test_vector_literal_with_values(self, postgres_dialect: PostgresDialect):
        """Test constructing vector literal from values."""
        result = vector_literal(postgres_dialect, [1.0, 2.0, 3.0])
        assert isinstance(result, bases.BaseExpression)
        sql, params = result.to_sql()
        assert "vector(3)" in sql

    def test_vector_literal_with_dimensions(self, postgres_dialect: PostgresDialect):
        """Test constructing vector literal with explicit dimensions."""
        result = vector_literal(postgres_dialect, [1.0, 2.0, 3.0], dimensions=3)
        sql, params = result.to_sql()
        assert "vector(3)" in sql

    def test_vector_literal_returns_expression(self, postgres_dialect: PostgresDialect):
        """Test that vector_literal returns a BaseExpression."""
        result = vector_literal(postgres_dialect, [1.0, 2.0])
        assert isinstance(result, bases.BaseExpression)

    def test_all_functions_return_expression_objects(self, postgres_dialect: PostgresDialect):
        """Test that all pgvector functions return Expression objects (not strings)."""
        vec = [1.0, 2.0, 3.0]
        assert isinstance(vector_l2_distance(postgres_dialect, "col", vec), bases.BaseExpression)
        assert isinstance(vector_cosine_distance(postgres_dialect, "col", vec), bases.BaseExpression)
        assert isinstance(vector_inner_product(postgres_dialect, "col", vec), bases.BaseExpression)
        assert isinstance(vector_cosine_similarity(postgres_dialect, "col", vec), bases.BaseExpression)
        assert isinstance(vector_literal(postgres_dialect, vec), bases.BaseExpression)
