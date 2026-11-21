# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/mixins/test_citext.py
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

    def test_citext_column_factory(self):
        """citext column DDL is an expression node from the factory."""
        import pytest

        from rhosocial.activerecord.backend.impl.postgres.functions import citext_column

        sql, _ = citext_column(self.dialect, "name").to_sql()
        assert "CITEXT" in sql
        assert "name" in sql

        # PostgreSQL's citext has no length modifier.
        with pytest.raises(ValueError, match="length"):
            citext_column(self.dialect, "name", length=255).to_sql()


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
