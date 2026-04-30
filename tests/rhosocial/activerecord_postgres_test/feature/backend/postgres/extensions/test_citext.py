# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_citext.py
"""
Unit tests for PostgreSQL citext extension.

Tests for:
- format_citext_column (DDL, still on mixin)
- citext_literal (function factory)
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.citext import (
    citext_literal,
)


class TestPostgresCitextMixin:
    """Tests for citext DDL methods (still on mixin)."""

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


class TestCitextFunctions:
    """Tests for citext function factories."""

    def test_citext_literal(self):
        """citext_literal should return expression with citext cast."""
        dialect = PostgresDialect((14, 0, 0))
        result = citext_literal(dialect, "Hello")
        sql, params = result.to_sql()
        assert "citext" in sql.lower()

    def test_citext_literal_escape(self):
        """citext_literal should handle special characters."""
        dialect = PostgresDialect((14, 0, 0))
        result = citext_literal(dialect, "It's")
        sql, params = result.to_sql()
        assert "citext" in sql.lower()
