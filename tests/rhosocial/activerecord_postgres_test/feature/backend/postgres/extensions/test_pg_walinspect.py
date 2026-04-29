# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_walinspect.py
"""
Unit tests for PostgreSQL pg_walinspect extension functions.

Tests for:
- pg_get_wal_records_info
- pg_get_wal_blocks_info
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_walinspect import (
    pg_get_wal_records_info,
    pg_get_wal_blocks_info,
)


class TestPgWalinspectMixin:
    """Test pg_walinspect extension functions."""

    def test_pg_get_wal_records_info(self):
        """pg_get_wal_records_info should return FunctionCall with pg_get_wal_records_info."""
        dialect = PostgresDialect((14, 0, 0))
        result = pg_get_wal_records_info(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pg_get_wal_records_info" in sql.lower()

    def test_pg_get_wal_blocks_info(self):
        """pg_get_wal_blocks_info should return FunctionCall with pg_get_wal_blocks_info."""
        dialect = PostgresDialect((14, 0, 0))
        result = pg_get_wal_blocks_info(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pg_get_wal_blocks_info" in sql.lower()
