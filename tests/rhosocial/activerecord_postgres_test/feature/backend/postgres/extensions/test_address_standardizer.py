# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_address_standardizer.py
"""Unit tests for PostgreSQL address_standardizer extension mixin."""

from rhosocial.activerecord.backend.impl.postgres.mixins.extensions.address_standardizer import PostgresAddressStandardizerMixin


class TestAddressStandardizerMixin:
    """Test address_standardizer extension mixin."""

    def setup_method(self):
        """Set up test fixture."""
        self.mixin = PostgresAddressStandardizerMixin()

    def test_format_address_standardize(self):
        """Test address standardization formatting."""
        result = self.mixin.format_address_standardize('123 Main St')
        assert "standardize" in result

    def test_format_address_parse(self):
        """Test address parsing formatting."""
        result = self.mixin.format_address_parse('123 Main St')
        assert "parse" in result
