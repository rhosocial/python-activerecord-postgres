# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_surgery.py
"""
Unit tests for PostgreSQL pg_surgery extension functions.

Tests for:
- heap_force_freeze
- heap_force_kill
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_surgery import (
    heap_force_freeze,
    heap_force_kill,
)


class TestPgSurgeryMixin:
    """Test pg_surgery extension functions.

    Note: The actual PostgreSQL functions are:
    - heap_force_freeze(reloid regclass, tids tid[])
    - heap_force_kill(reloid regclass, tids tid[])
    """

    def test_heap_force_freeze(self):
        """heap_force_freeze should return FunctionCall with heap_force_freeze."""
        dialect = PostgresDialect((14, 0, 0))
        result = heap_force_freeze(dialect, 'users', "'{(0,1)}'::tid[]")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "heap_force_freeze" in sql.lower()

    def test_heap_force_kill(self):
        """heap_force_kill should return FunctionCall with heap_force_kill."""
        dialect = PostgresDialect((14, 0, 0))
        result = heap_force_kill(dialect, 'users', "'{(0,1)}'::tid[]")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "heap_force_kill" in sql.lower()
