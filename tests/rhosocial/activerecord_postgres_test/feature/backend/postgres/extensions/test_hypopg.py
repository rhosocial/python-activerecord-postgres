# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_hypopg.py
"""
Unit tests for PostgreSQL hypopg extension functions.

Tests for:
- hypopg_create_index
- hypopg_reset
- hypopg_show_indexes
- hypopg_estimate_size
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.hypopg import (
    hypopg_create_index,
    hypopg_reset,
    hypopg_show_indexes,
    hypopg_estimate_size,
)


class TestPostgresHypoPgFunctions:
    """Tests for hypopg extension function factories."""

    def test_hypopg_create_index(self):
        """hypopg_create_index should return FunctionCall with hypopg_create_index."""
        dialect = PostgresDialect((14, 0, 0))
        result = hypopg_create_index(dialect, "CREATE INDEX ON users USING btree (email)")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "hypopg_create_index" in sql.lower()

    def test_hypopg_reset(self):
        """hypopg_reset should return FunctionCall with hypopg_reset."""
        dialect = PostgresDialect((14, 0, 0))
        result = hypopg_reset(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "hypopg_reset" in sql.lower()

    def test_hypopg_show_indexes(self):
        """hypopg_show_indexes should return FunctionCall with hypopg_index_detail."""
        dialect = PostgresDialect((14, 0, 0))
        result = hypopg_show_indexes(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "hypopg" in sql.lower()

    def test_hypopg_estimate_size(self):
        """hypopg_estimate_size should return FunctionCall with hypopg_relation_size."""
        dialect = PostgresDialect((14, 0, 0))
        result = hypopg_estimate_size(dialect, 1)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "hypopg_relation_size" in sql.lower()
