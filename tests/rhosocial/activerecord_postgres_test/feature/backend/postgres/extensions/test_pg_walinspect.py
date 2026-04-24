# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_walinspect.py
"""Unit tests for PostgreSQL pg_walinspect extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pg_walinspect import PostgresPgWalinspectMixin


class TestPgWalinspectMixin:
    """Test pg_walinspect extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresPgWalinspectMixin()

    def test_format_pg_get_wal_records_info(self):
        """Test WAL records info query formatting."""
        result = self.mixin.format_pg_get_wal_records_info()
        assert "pg_get_wal_records_info" in result

    def test_format_pg_get_wal_blocks_info(self):
        """Test WAL blocks info query formatting."""
        result = self.mixin.format_pg_get_wal_blocks_info()
        assert "pg_get_wal_blocks_info" in result
