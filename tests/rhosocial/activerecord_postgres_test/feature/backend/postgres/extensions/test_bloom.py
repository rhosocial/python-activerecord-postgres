# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_bloom.py
"""
Unit tests for PostgreSQL bloom extension mixin.

Tests for PostgresBloomMixin format methods:
- format_bloom_index
- format_bloom_access_method
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresBloomMixin:
    """Tests for PostgresBloomMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_bloom_index(self):
        """format_bloom_index should return SQL containing bloom."""
        result = self.dialect.format_bloom_index("idx_name", "table_name", ["col1", "col2"])
        assert "bloom" in result
        assert "idx_name" in result
        assert "table_name" in result

    def test_format_bloom_index_with_fill_factor(self):
        """format_bloom_index with fill_factor should include fillfactor option."""
        result = self.dialect.format_bloom_index(
            "idx_name", "table_name", ["col1", "col2"], fill_factor=90
        )
        assert "bloom" in result
        assert "fillfactor" in result

    def test_format_bloom_access_method(self):
        """format_bloom_access_method should return SQL containing bloom."""
        result = self.dialect.format_bloom_access_method()
        assert "bloom" in result
