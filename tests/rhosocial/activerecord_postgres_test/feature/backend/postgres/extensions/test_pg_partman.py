# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_partman.py
"""
Unit tests for PostgreSQL pg_partman extension functions.

Tests for:
- create_time_partition
- create_id_partition
- run_maintenance
- format_auto_partition_config (mixin method)
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pg_partman import (
    create_time_partition,
    create_id_partition,
    run_maintenance,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pg_partman import PostgresPgPartmanMixin


class TestPgPartmanMixin:
    """Test pg_partman extension functions and mixin."""

    def test_create_time_partition(self):
        """create_time_partition should return FunctionCall with create_time_based_partition_set."""
        dialect = PostgresDialect((14, 0, 0))
        result = create_time_partition(dialect, 'events')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "create_time_based_partition_set" in sql.lower()

    def test_create_id_partition(self):
        """create_id_partition should return FunctionCall with create_id_based_partition_set."""
        dialect = PostgresDialect((14, 0, 0))
        result = create_id_partition(dialect, 'events')
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "create_id_based_partition_set" in sql.lower()

    def test_run_maintenance(self):
        """run_maintenance should return FunctionCall with run_maintenance."""
        dialect = PostgresDialect((14, 0, 0))
        result = run_maintenance(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "run_maintenance" in sql.lower()

    def test_format_auto_partition_config(self):
        """Test auto partition config formatting."""
        mixin = PostgresPgPartmanMixin()
        result = mixin.format_auto_partition_config('events')
        assert "part_config" in result
