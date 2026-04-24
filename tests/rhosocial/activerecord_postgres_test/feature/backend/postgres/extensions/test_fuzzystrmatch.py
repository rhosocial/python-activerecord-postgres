# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_fuzzystrmatch.py
"""
Unit tests for PostgreSQL fuzzystrmatch extension mixin.

Tests for PostgresFuzzystrmatchMixin format methods:
- format_levenshtein
- format_soundex
- format_dmetaphone
- format_difference
- format_metaphone
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresFuzzystrmatchMixin:
    """Tests for PostgresFuzzystrmatchMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_levenshtein(self):
        """format_levenshtein should return SQL containing levenshtein."""
        result = self.dialect.format_levenshtein("kitten", "sitting")
        assert "levenshtein" in result

    def test_format_soundex(self):
        """format_soundex should return SQL containing soundex."""
        result = self.dialect.format_soundex("hello")
        assert "soundex" in result

    def test_format_dmetaphone(self):
        """format_dmetaphone should return SQL containing dmetaphone."""
        result = self.dialect.format_dmetaphone("word")
        assert "dmetaphone" in result

    def test_format_difference(self):
        """format_difference should return tuple with SQL containing difference."""
        sql, params = self.dialect.format_difference("'hello'", "'helo'")
        assert "difference" in sql
        assert params == ()

    def test_format_metaphone(self):
        """format_metaphone should return tuple with SQL containing metaphone."""
        sql, params = self.dialect.format_metaphone("'word'", 6)
        assert "metaphone" in sql
        assert params == ()
