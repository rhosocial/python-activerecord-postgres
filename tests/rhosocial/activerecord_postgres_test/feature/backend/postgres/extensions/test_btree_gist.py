# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_btree_gist.py
"""
Unit tests for PostgreSQL btree_gist extension mixin.

Tests for PostgresBtreeGistMixin format methods:
- format_gist_index
- format_btree_gist_operator_class
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresBtreeGistMixin:
    """Tests for PostgresBtreeGistMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_gist_index(self):
        """format_gist_index should return SQL containing gist."""
        result = self.dialect.format_gist_index("idx_name", "table_name", ["column"])
        assert "gist" in result
        assert "idx_name" in result
        assert "table_name" in result

    def test_format_gist_index_with_include(self):
        """format_gist_index with include should include additional columns."""
        result = self.dialect.format_gist_index(
            "idx_name", "table_name", ["column"], include=["extra_col"]
        )
        assert "gist" in result
        assert "INCLUDE" in result

    def test_format_btree_gist_operator_class(self):
        """format_btree_gist_operator_class should return operator class name."""
        result = self.dialect.format_btree_gist_operator_class("int4")
        assert result == "int4_ops"
