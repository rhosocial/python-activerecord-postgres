# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_extensions.py
"""Unit tests for PostgreSQL extension mixins.

Tests for:
- hstore extension
- ltree extension
- intarray extension
"""
import pytest
from unittest.mock import MagicMock

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


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


class TestLtreeMixin:
    """Test ltree extension mixin."""

    def test_format_ltree_literal(self):
        """Test ltree literal formatting."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_literal('Top.Science.Astronomy')
        assert "'Top.Science.Astronomy'" in result

    def test_format_lquery_literal(self):
        """Test lquery literal formatting."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_lquery_literal('*.Astronomy.*')
        assert "'*.Astronomy.*'::lquery" in result

    def test_format_ltree_operator_ltree(self):
        """Test ltree operator with ltree value."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_operator('path', '@>', 'Top.Science')
        assert "path @> 'Top.Science'" in result

    def test_format_ltree_operator_lquery(self):
        """Test ltree operator with lquery value."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_operator('path', '~', '*.Astronomy.*', 'lquery')
        assert "path ~ '*.Astronomy.*'::lquery" in result

    def test_format_ltree_operator_ltxtquery(self):
        """Test ltree operator with ltxtquery value."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_operator('path', '@', 'Astronomy & Stars', 'ltxtquery')
        assert "path @ 'Astronomy & Stars'::ltxtquery" in result

    def test_format_ltree_is_ancestor(self):
        """Test ltree ancestor check."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_is_ancestor('path', 'Top.Science.Astronomy')
        assert "path @> 'Top.Science.Astronomy'" in result

    def test_format_ltree_is_descendant(self):
        """Test ltree descendant check."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_is_descendant('path', 'Top.Science')
        assert "path <@ 'Top.Science'" in result

    def test_format_ltree_matches(self):
        """Test ltree lquery match."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_matches('path', '*.Astronomy.*')
        assert "path ~ '*.Astronomy.*'::lquery" in result

    def test_format_ltree_index_statement(self):
        """Test ltree index statement."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_ltree_index_statement('idx_path', 'categories', 'path')
        assert "CREATE INDEX idx_path ON categories USING gist (path)" in sql

    def test_format_ltree_index_statement_with_schema(self):
        """Test ltree index statement with schema."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_ltree_index_statement('idx_path', 'categories', 'path', schema='public')
        assert "ON public.categories" in sql

    def test_format_ltree_index_statement_btree(self):
        """Test ltree index statement with btree."""
        dialect = PostgresDialect((14, 0, 0))
        sql, params = dialect.format_ltree_index_statement('idx_path', 'categories', 'path', index_type='btree')
        assert "USING btree" in sql

    def test_format_ltree_subpath_with_length(self):
        """Test ltree subpath with length."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_subpath('path', 0, 2)
        assert "subpath(path, 0, 2)" in result

    def test_format_ltree_subpath_without_length(self):
        """Test ltree subpath without length."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_subpath('path', 1)
        assert "subpath(path, 1)" in result

    def test_format_ltree_nlevel(self):
        """Test ltree nlevel function."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_ltree_nlevel('path')
        assert "nlevel(path)" in result


class TestIntarrayMixin:
    """Test intarray extension mixin."""

    def test_format_intarray_operator_contains(self):
        """Test intarray @> operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_intarray_operator('arr', '@>', 'ARRAY[1,2]')
        assert "arr @> ARRAY[1,2]" in result

    def test_format_intarray_operator_contained(self):
        """Test intarray <@ operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_intarray_operator('arr', '<@', 'ARRAY[1,2,3]')
        assert "arr <@ ARRAY[1,2,3]" in result

    def test_format_intarray_operator_overlap(self):
        """Test intarray && operator."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_intarray_operator('arr', '&&', 'ARRAY[1,2]')
        assert "arr && ARRAY[1,2]" in result


class TestEarthdistanceMixin:
    """Test earthdistance extension mixin."""

    def test_format_earth_distance(self):
        """Test earth distance calculation."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_earth_distance('point1', 'point2')
        assert "earth_distance" in result


class TestPgTrgmMixin:
    """Test pg_trgm extension mixin."""

    def test_format_similarity(self):
        """Test similarity function."""
        dialect = PostgresDialect((14, 0, 0))
        # Test with function form (use_operator=False)
        result = dialect.format_similarity_expression('col', 'pattern', use_operator=False)
        assert "similarity" in result

    def test_format_similarity_operator(self):
        """Test similarity operator."""
        dialect = PostgresDialect((14, 0, 0))
        # Test with operator form (default)
        result = dialect.format_similarity_expression('col', 'pattern')
        assert "col % 'pattern'" in result

    def test_format_show_trgm(self):
        """Test show_trgm function."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_show_trgm('text')
        assert "show_trgm" in result


class TestPgvectorMixin:
    """Test pgvector extension mixin."""

    def test_format_vector_literal(self):
        """Test vector literal formatting."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_vector_literal([1.0, 2.0, 3.0])
        assert "[1.0, 2.0, 3.0]" in result

    def test_format_vector_cosine_distance(self):
        """Test vector cosine distance."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_vector_similarity_expression(
            'embedding', '[1,2,3]', distance_metric='cosine'
        )
        assert "embedding <=> '[1,2,3]'" in result


class TestPostGISMixin:
    """Test PostGIS extension mixin."""

    def test_format_geometry_from_text(self):
        """Test geometry from text."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_geometry_literal('POINT(0 0)', 4326)
        assert "ST_GeomFromText" in result

    def test_format_geography_from_text(self):
        """Test geography from text."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.format_geometry_literal(
            'POINT(0 0)', 4326, geometry_type='geography'
        )
        assert "ST_GeogFromText" in result
