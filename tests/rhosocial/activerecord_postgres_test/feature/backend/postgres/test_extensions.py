# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_extensions.py
"""Unit tests for PostgreSQL extension mixins and functions.

Tests for:
- hstore extension mixin (supports_* methods)
- ltree extension functions
- intarray extension functions
- earthdistance extension functions
- pg_trgm extension functions
- pgvector extension functions
- PostGIS extension functions
"""
import pytest
from unittest.mock import MagicMock

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core, operators
from rhosocial.activerecord.backend.expression.operators import BinaryArithmeticExpression
from rhosocial.activerecord.backend.impl.postgres.functions.ltree import (
    ltree_literal,
    lquery_literal,
    ltxtquery_literal,
    ltree_ancestor,
    ltree_descendant,
    ltree_matches,
    ltree_subpath,
    ltree_nlevel,
)
from rhosocial.activerecord.backend.impl.postgres.functions.earthdistance import (
    earth_distance,
    ll_to_earth,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_trgm import (
    similarity,
    similarity_operator,
    show_trgm,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pgvector import (
    vector_l2_distance,
    vector_cosine_distance,
    vector_literal,
)
from rhosocial.activerecord.backend.impl.postgres.functions.postgis import (
    st_geom_from_text,
    st_geog_from_text,
)
from rhosocial.activerecord.backend.impl.postgres.functions.intarray import (
    intarray_contains,
    intarray_contained_by,
    intarray_overlaps,
)


class TestHstoreMixin:
    """Test hstore extension mixin."""

    def test_supports_hstore_type(self):
        """Test hstore type support check."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.supports_hstore_type()
        assert isinstance(result, bool)

    def test_supports_hstore_operators(self):
        """Test hstore operators support check."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.supports_hstore_operators()
        assert isinstance(result, bool)


class TestLtreeFunctions:
    """Test ltree extension functions."""

    def test_ltree_literal(self):
        """Test ltree literal function."""
        dialect = PostgresDialect((14, 0, 0))
        result = ltree_literal(dialect, 'Top.Science.Astronomy')
        assert isinstance(result, core.Literal)
        sql, params = result.to_sql()
        assert 'Top.Science.Astronomy' in params

    def test_lquery_literal(self):
        """Test lquery literal function."""
        dialect = PostgresDialect((14, 0, 0))
        result = lquery_literal(dialect, '*.Astronomy.*')
        sql, params = result.to_sql()
        assert "lquery" in sql.lower()

    def test_ltree_ancestor(self):
        """Test ltree ancestor check."""
        dialect = PostgresDialect((14, 0, 0))
        result = ltree_ancestor(dialect, 'path', 'Top.Science.Astronomy')
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql

    def test_ltree_descendant(self):
        """Test ltree descendant check."""
        dialect = PostgresDialect((14, 0, 0))
        result = ltree_descendant(dialect, 'path', 'Top.Science')
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "<@" in sql

    def test_ltree_matches(self):
        """Test ltree lquery match."""
        dialect = PostgresDialect((14, 0, 0))
        result = ltree_matches(dialect, 'path', '*.Astronomy.*')
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "~" in sql

    def test_ltree_index_statement(self):
        """Test ltree index statement (DDL, still on mixin)."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_ltree_index_statement('idx_path', 'categories', 'path')
        assert "CREATE INDEX idx_path ON categories USING gist (path)" in sql

    def test_ltree_index_statement_with_schema(self):
        """Test ltree index statement with schema."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_ltree_index_statement('idx_path', 'categories', 'path', schema='public')
        assert "ON public.categories" in sql

    def test_ltree_index_statement_btree(self):
        """Test ltree index statement with btree."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_ltree_index_statement('idx_path', 'categories', 'path', index_type='btree')
        assert "USING btree" in sql

    def test_ltree_subpath_with_length(self):
        """Test ltree subpath with length."""
        dialect = PostgresDialect((14, 0, 0))
        result = ltree_subpath(dialect, 'path', 0, 2)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "subpath" in sql.lower()

    def test_ltree_subpath_without_length(self):
        """Test ltree subpath without length."""
        dialect = PostgresDialect((14, 0, 0))
        result = ltree_subpath(dialect, 'path', 1)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "subpath" in sql.lower()

    def test_ltree_nlevel(self):
        """Test ltree nlevel function."""
        dialect = PostgresDialect((14, 0, 0))
        result = ltree_nlevel(dialect, 'path')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "nlevel" in sql.lower()


class TestIntarrayFunctions:
    """Test intarray extension functions."""

    def test_intarray_contains(self):
        """Test intarray @> operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = intarray_contains(dialect, 'arr', 'ARRAY[1,2]')
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql

    def test_intarray_contained_by(self):
        """Test intarray <@ operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = intarray_contained_by(dialect, 'arr', 'ARRAY[1,2,3]')
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "<@" in sql

    def test_intarray_overlaps(self):
        """Test intarray && operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = intarray_overlaps(dialect, 'arr', 'ARRAY[1,2]')
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "&&" in sql


class TestEarthdistanceFunctions:
    """Test earthdistance extension functions."""

    def test_earth_distance(self):
        """Test earth distance calculation."""
        dialect = PostgresDialect((14, 0, 0))
        result = earth_distance(dialect, 'point1', 'point2')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "earth_distance" in sql.lower()

    def test_ll_to_earth(self):
        """Test ll_to_earth function."""
        dialect = PostgresDialect((14, 0, 0))
        result = ll_to_earth(dialect, 40.7128, -74.0060)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "ll_to_earth" in sql.lower()


class TestPgTrgmFunctions:
    """Test pg_trgm extension functions."""

    def test_similarity_function(self):
        """Test similarity function."""
        dialect = PostgresDialect((14, 0, 0))
        result = similarity(dialect, 'col', 'pattern')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "similarity" in sql.lower()

    def test_similarity_operator(self):
        """Test similarity operator (%)."""
        dialect = PostgresDialect((14, 0, 0))
        result = similarity_operator(dialect, 'col', 'pattern')
        assert isinstance(result, operators.BinaryExpression)
        sql, params = result.to_sql()
        assert "%" in sql

    def test_show_trgm(self):
        """Test show_trgm function."""
        dialect = PostgresDialect((14, 0, 0))
        result = show_trgm(dialect, 'text')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "show_trgm" in sql.lower()


class TestPgvectorFunctions:
    """Test pgvector extension functions."""

    def test_vector_literal(self):
        """Test vector literal function."""
        dialect = PostgresDialect((14, 0, 0))
        result = vector_literal(dialect, [1.0, 2.0, 3.0])
        sql, params = result.to_sql()
        assert "vector" in sql.lower()

    def test_vector_cosine_distance(self):
        """Test vector cosine distance."""
        dialect = PostgresDialect((14, 0, 0))
        result = vector_cosine_distance(dialect, 'embedding', '[1,2,3]')
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<=>" in sql

    def test_vector_l2_distance(self):
        """Test vector L2 distance."""
        dialect = PostgresDialect((14, 0, 0))
        result = vector_l2_distance(dialect, 'embedding', '[1,2,3]')
        assert isinstance(result, BinaryArithmeticExpression)
        sql, params = result.to_sql()
        assert "<->" in sql


class TestPostGISFunctions:
    """Test PostGIS extension functions."""

    def test_st_geom_from_text(self):
        """Test geometry from text."""
        dialect = PostgresDialect((14, 0, 0))
        result = st_geom_from_text(dialect, 'POINT(0 0)', srid=4326)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "st_geomfromtext" in sql.lower()

    def test_st_geog_from_text(self):
        """Test geography from text."""
        dialect = PostgresDialect((14, 0, 0))
        result = st_geog_from_text(dialect, 'POINT(0 0)')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "st_geogfromtext" in sql.lower()
