# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_surgery.py
"""
Unit tests for PostgreSQL pg_surgery extension functions.

Tests for:
- pg_surgery_heap_freeze
- pg_surgery_heap_page_header
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_surgery import (
    pg_surgery_heap_freeze,
    pg_surgery_heap_page_header,
)


class TestPgSurgeryMixin:
    """Test pg_surgery extension functions."""

    def test_pg_surgery_heap_freeze(self):
        """pg_surgery_heap_freeze should return FunctionCall with freeze_heap."""
        dialect = PostgresDialect((14, 0, 0))
        result = pg_surgery_heap_freeze(dialect, 'users')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "freeze_heap" in sql.lower()

    def test_pg_surgery_heap_page_header(self):
        """pg_surgery_heap_page_header should return FunctionCall with set_heap_tuple_frozen."""
        dialect = PostgresDialect((14, 0, 0))
        result = pg_surgery_heap_page_header(dialect, 'users', 0, 1)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "set_heap_tuple_frozen" in sql.lower()
