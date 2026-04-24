# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_cube.py
"""
Unit tests for PostgreSQL cube extension mixin.

Tests for PostgresCubeMixin format methods:
- format_cube
- format_cube_contains
- format_cube_distance
- format_cube_size
- format_cube_union
- format_cube_inter
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresCubeMixin:
    """Tests for PostgresCubeMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_cube(self):
        """format_cube should return SQL containing cube."""
        result = self.dialect.format_cube([1, 2])
        assert "cube" in result
        assert "1" in result
        assert "2" in result

    def test_format_cube_contains(self):
        """format_cube_contains should return SQL containing @>."""
        result = self.dialect.format_cube_contains("c1", "c2")
        assert "@>" in result

    def test_format_cube_distance(self):
        """format_cube_distance should return SQL containing <->."""
        result = self.dialect.format_cube_distance("c1", "c2")
        assert "<->" in result

    def test_format_cube_size(self):
        """format_cube_size should return SQL containing cube_size."""
        result = self.dialect.format_cube_size("c1")
        assert "cube_size" in result

    def test_format_cube_union(self):
        """format_cube_union should return SQL containing union."""
        result = self.dialect.format_cube_union("c1", "c2")
        assert "union" in result

    def test_format_cube_inter(self):
        """format_cube_inter should return SQL containing inter."""
        result = self.dialect.format_cube_inter("c1", "c2")
        assert "inter" in result
