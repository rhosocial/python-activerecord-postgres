# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_partman.py
"""Unit tests for PostgreSQL pg_partman extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pg_partman import PostgresPgPartmanMixin


class TestPgPartmanMixin:
    """Test pg_partman extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresPgPartmanMixin()

    def test_format_create_partition_time(self):
        """Test time-based partition creation formatting."""
        result = self.mixin.format_create_partition_time('events')
        assert "create_time_based_partition_set" in result

    def test_format_create_partition_id(self):
        """Test ID-based partition creation formatting."""
        result = self.mixin.format_create_partition_id('events')
        assert "create_id_based_partition_set" in result

    def test_format_run_maintenance(self):
        """Test partition maintenance execution formatting."""
        result = self.mixin.format_run_maintenance()
        assert "run_maintenance" in result

    def test_format_auto_partition_config(self):
        """Test auto partition config formatting."""
        result = self.mixin.format_auto_partition_config('events')
        assert "part_config" in result
