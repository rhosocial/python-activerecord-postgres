# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_fuzzystrmatch.py
"""
Unit tests for PostgreSQL fuzzystrmatch extension functions.

Tests for:
- levenshtein
- levenshtein_less_equal
- soundex
- difference
- metaphone
- dmetaphone
- dmetaphone_alt
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.fuzzystrmatch import (
    levenshtein,
    levenshtein_less_equal,
    soundex,
    difference,
    metaphone,
    dmetaphone,
    dmetaphone_alt,
)


class TestPostgresFuzzystrmatchFunctions:
    """Tests for fuzzystrmatch function factories."""

    def test_levenshtein(self):
        """levenshtein should return FunctionCall with levenshtein."""
        dialect = PostgresDialect((14, 0, 0))
        result = levenshtein(dialect, "kitten", "sitting")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "levenshtein" in sql.lower()

    def test_levenshtein_less_equal(self):
        """levenshtein_less_equal should return FunctionCall."""
        dialect = PostgresDialect((14, 0, 0))
        result = levenshtein_less_equal(dialect, "kitten", "sitting", 3)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "levenshtein_less_equal" in sql.lower()

    def test_soundex(self):
        """soundex should return FunctionCall with soundex."""
        dialect = PostgresDialect((14, 0, 0))
        result = soundex(dialect, "hello")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "soundex" in sql.lower()

    def test_difference(self):
        """difference should return FunctionCall with difference."""
        dialect = PostgresDialect((14, 0, 0))
        result = difference(dialect, "hello", "helo")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "difference" in sql.lower()

    def test_metaphone(self):
        """metaphone should return FunctionCall with metaphone."""
        dialect = PostgresDialect((14, 0, 0))
        result = metaphone(dialect, "word", 6)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "metaphone" in sql.lower()

    def test_dmetaphone(self):
        """dmetaphone should return FunctionCall with dmetaphone."""
        dialect = PostgresDialect((14, 0, 0))
        result = dmetaphone(dialect, "word")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "dmetaphone" in sql.lower()

    def test_dmetaphone_alt(self):
        """dmetaphone_alt should return FunctionCall with dmetaphone_alt."""
        dialect = PostgresDialect((14, 0, 0))
        result = dmetaphone_alt(dialect, "word")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "dmetaphone_alt" in sql.lower()
