# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_cube.py
"""
Unit tests for PostgreSQL cube extension functions.

Tests for:
- cube_literal
- cube_dimension
- cube_size
- cube_union
- cube_inter
- cube_contains
- cube_distance
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.expression.operators import (
    BinaryExpression,
    BinaryArithmeticExpression,
)
from rhosocial.activerecord.backend.impl.postgres.functions.cube import (
    cube_literal,
    cube_dimension,
    cube_size,
    cube_union,
    cube_inter,
    cube_contains,
    cube_distance,
)


class TestPostgresCubeFunctions:
    """Tests for cube extension function factories."""

    def test_cube_literal(self):
        """cube_literal should return FunctionCall with cube."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_literal(dialect, [1, 2])
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cube" in sql.lower()
        # Coordinates are parameterized; verify they appear in params
        assert any("1" in str(p) and "2" in str(p) for p in params)

    def test_cube_literal_string(self):
        """cube_literal with string should return FunctionCall with cube."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_literal(dialect, "(1,2,3),(4,5,6)")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cube" in sql.lower()

    def test_cube_dimension(self):
        """cube_dimension should return FunctionCall with cube."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_dimension(dialect, 3, 1.0)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cube" in sql.lower()

    def test_cube_size(self):
        """cube_size should return FunctionCall with cube_size."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_size(dialect, "c1")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "cube_size" in sql.lower()

    def test_cube_union(self):
        """cube_union should return BinaryExpression with union operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_union(dialect, "c1", "c2")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "union" in sql.lower()

    def test_cube_inter(self):
        """cube_inter should return BinaryExpression with inter operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_inter(dialect, "c1", "c2")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "inter" in sql.lower()

    def test_cube_contains(self):
        """cube_contains should return BinaryExpression with @> operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_contains(dialect, "c1", "c2")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql

    def test_cube_distance(self):
        """cube_distance should return BinaryArithmeticExpression with <-> operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = cube_distance(dialect, "c1", "c2")
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<->" in sql
