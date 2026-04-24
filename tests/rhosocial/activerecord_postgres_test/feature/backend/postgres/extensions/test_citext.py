# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_citext.py
"""
Unit tests for PostgreSQL citext extension mixin.

Tests for PostgresCitextMixin format methods:
- format_citext_column
- format_citext_literal
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresCitextMixin:
    """Tests for PostgresCitextMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_citext_column(self):
        """format_citext_column should return column definition containing CITEXT."""
        result = self.dialect.format_citext_column("name")
        assert "CITEXT" in result
        assert "name" in result

    def test_format_citext_column_with_length(self):
        """format_citext_column with length should include length constraint."""
        result = self.dialect.format_citext_column("name", length=255)
        assert "CITEXT(255)" in result
        assert "name" in result

    def test_format_citext_literal(self):
        """format_citext_literal should return quoted citext literal."""
        result = self.dialect.format_citext_literal("Hello")
        assert "citext" in result
        assert "'Hello'" in result

    def test_format_citext_literal_escape(self):
        """format_citext_literal should escape single quotes."""
        result = self.dialect.format_citext_literal("It's")
        assert "''" in result
        assert "citext" in result
