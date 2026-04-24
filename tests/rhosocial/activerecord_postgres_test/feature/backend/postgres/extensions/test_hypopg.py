# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_hypopg.py
"""Unit tests for PostgreSQL hypopg extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.hypopg import PostgresHypoPgMixin


class TestHypoPgMixin:
    """Test hypopg extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresHypoPgMixin()

    def test_format_hypopg_create_index(self):
        """Test hypothetical index creation formatting."""
        result = self.mixin.format_hypopg_create_index('idx_test', 'users', ['name'])
        assert "hypopg.create_index" in result

    def test_format_hypopg_reset(self):
        """Test hypothetical index reset formatting."""
        result = self.mixin.format_hypopg_reset()
        assert "hypopg.reset" in result

    def test_format_hypopg_show_indexes(self):
        """Test show hypothetical indexes formatting."""
        result = self.mixin.format_hypopg_show_indexes()
        assert "hypopg" in result

    def test_format_hypopg_estimate_size(self):
        """Test hypothetical index size estimation formatting."""
        result = self.mixin.format_hypopg_estimate_size(1)
        assert "hypopg_estimate_size" in result
