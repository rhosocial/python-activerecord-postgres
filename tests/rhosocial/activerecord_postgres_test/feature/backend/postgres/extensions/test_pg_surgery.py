# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_surgery.py
"""Unit tests for PostgreSQL pg_surgery extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.pg_surgery import PostgresPgSurgeryMixin


class TestPgSurgeryMixin:
    """Test pg_surgery extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresPgSurgeryMixin()

    def test_format_pg_surgery_heap_freeze(self):
        """Test heap freeze operation formatting."""
        result = self.mixin.format_pg_surgery_heap_freeze('users')
        assert "freeze_heap" in result

    def test_format_pg_surgery_heap_page_header(self):
        """Test heap page header repair formatting."""
        result = self.mixin.format_pg_surgery_heap_page_header('users', 0, 1)
        assert "set_heap_tuple_frozen" in result
