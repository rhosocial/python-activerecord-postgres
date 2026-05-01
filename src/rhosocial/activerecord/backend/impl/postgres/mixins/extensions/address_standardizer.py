# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/address_standardizer.py
"""
PostgreSQL address_standardizer functionality mixin.

This module provides functionality to check address_standardizer extension features.

For SQL expression generation, use the function factories in
``functions/address_standardizer.py`` instead of the removed format_* methods.
"""


class PostgresAddressStandardizerMixin:
    """address_standardizer functionality implementation."""

    def supports_address_standardizer(self) -> bool:
        """Check if address_standardizer extension is available."""
        return self.is_extension_installed("address_standardizer")
