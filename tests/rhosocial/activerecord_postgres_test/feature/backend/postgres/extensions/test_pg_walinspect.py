# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_walinspect.py
"""
Unit tests for PostgreSQL pg_walinspect extension functions.

Tests for:
- pg_get_wal_records_info
- pg_get_wal_blocks_info

Note: Both functions require start_lsn and end_lsn parameters (pg_lsn type).
pg_logical_emit_message has been removed from this module as it is a built-in
PostgreSQL function, not part of the pg_walinspect extension.
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
        """pg_get_wal_records_info should return FunctionCall with start and end LSN."""
        dialect = PostgresDialect((14, 0, 0))
        result = pg_get_wal_records_info(dialect, '0/16E9130', '0/16E9200')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pg_get_wal_records_info" in sql.lower()
        # Should have 2 parameters (start_lsn, end_lsn)
        assert sql.count("%s") == 2

    def test_pg_get_wal_blocks_info(self):
        """pg_get_wal_blocks_info should return FunctionCall with start and end LSN."""
        dialect = PostgresDialect((14, 0, 0))
        result = pg_get_wal_blocks_info(dialect, '0/16E9130', '0/16E9200')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pg_get_wal_blocks_info" in sql.lower()
        # Should have 2 parameters (start_lsn, end_lsn)
        assert sql.count("%s") == 2
