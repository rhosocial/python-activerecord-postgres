# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_partman.py
"""
Unit tests for PostgreSQL pg_partman extension functions.

Tests for:
- create_parent
- run_maintenance
- format_auto_partition_config (mixin method)
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_partman import (
    create_parent,
    run_maintenance,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pg_partman import PostgresPgPartmanMixin


class TestPgPartmanMixin:
    """Test pg_partman extension functions and mixin."""

    def test_create_parent(self):
        """create_parent should return FunctionCall with create_parent."""
        dialect = PostgresDialect((14, 0, 0))
        result = create_parent(dialect, 'public.events', 'created_at', '1 day')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "create_parent" in sql.lower()

    def test_create_parent_with_all_params(self):
        """create_parent with all parameters should include partition_type and premake."""
        dialect = PostgresDialect((14, 0, 0))
        result = create_parent(
            dialect, 'public.events', 'created_at', '1 day',
            partition_type='native', premake=6,
        )
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "create_parent" in sql.lower()
        # Should have 5 parameters
        assert sql.count("%s") == 5

    def test_run_maintenance(self):
        """run_maintenance should return FunctionCall with run_maintenance."""
        dialect = PostgresDialect((14, 0, 0))
        result = run_maintenance(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "run_maintenance" in sql.lower()

    def test_run_maintenance_with_table(self):
        """run_maintenance with table name should include the parameter."""
        dialect = PostgresDialect((14, 0, 0))
        result = run_maintenance(dialect, 'public.events')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "run_maintenance" in sql.lower()
        assert sql.count("%s") == 1

    def test_format_auto_partition_config(self):
        """Test auto partition config formatting."""
        mixin = PostgresPgPartmanMixin()
        result = mixin.format_auto_partition_config('events')
        assert "part_config" in result
