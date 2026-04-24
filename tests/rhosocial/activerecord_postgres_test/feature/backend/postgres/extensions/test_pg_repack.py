# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_repack.py
"""Unit tests for PostgreSQL pg_repack extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pg_repack import PostgresPgRepackMixin


class TestPgRepackMixin:
    """Test pg_repack extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresPgRepackMixin()

    def test_format_pg_repack_table(self):
        """Test pg_repack table rebuild formatting."""
        result = self.mixin.format_pg_repack_table('users')
        assert "repack" in result

    def test_format_pg_repack_index(self):
        """Test pg_repack index rebuild formatting."""
        result = self.mixin.format_pg_repack_index('idx_name')
        assert "repack" in result
