# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/address_standardizer.py
"""
address_standardizer functionality implementation.

This module provides the PostgresAddressStandardizerMixin class that adds support for
address_standardizer extension features.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PostgresAddressStandardizerMixin:
    """address_standardizer functionality implementation."""

    def supports_address_standardizer(self) -> bool:
        """Check if address_standardizer extension is available."""
        return self.is_extension_installed("address_standardizer")

    def format_address_standardize(
        self, address: str, use_tiger: bool = False
    ) -> str:
        """Format address standardization.

        Args:
            address: Address string
            use_tiger: Use TIGER data for output

        Returns:
            SQL function call
        """
        return f"SELECT standardize_address('{address}', ''::text, ''::text)"

    def format_address_parse(self, address: str) -> str:
        """Format address parsing.

        Args:
            address: Address string

        Returns:
            SQL function call
        """
        return f"SELECT parse_address('{address}', 'US')"