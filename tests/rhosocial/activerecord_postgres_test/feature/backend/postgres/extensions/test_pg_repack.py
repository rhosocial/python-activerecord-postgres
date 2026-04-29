# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_repack.py
"""
Unit tests for PostgreSQL pg_repack extension functions.

Tests for:
- repack_table
- repack_index
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_repack import (
    repack_table,
    repack_index,
)


class TestPgRepackMixin:
    """Test pg_repack extension functions."""

    def test_repack_table(self):
        """repack_table should return FunctionCall with repack.repack_table."""
        dialect = PostgresDialect((14, 0, 0))
        result = repack_table(dialect, 'users')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "repack" in sql.lower()

    def test_repack_index(self):
        """repack_index should return FunctionCall with repack.repack_index."""
        dialect = PostgresDialect((14, 0, 0))
        result = repack_index(dialect, 'idx_name')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "repack" in sql.lower()
