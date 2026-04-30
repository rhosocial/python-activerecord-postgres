# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/address_standardizer.py
"""address_standardizer extension protocol definition.

This module defines the protocol for address_standardizer
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/address_standardizer.py`` instead of the removed format_* methods.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresAddressStandardizerSupport(Protocol):
    """address_standardizer extension protocol.

    Feature Source: Extension support (requires address_standardizer extension)

    address_standardizer provides address parsing:
    - Parse US addresses
    - Standardize address components

    Extension Information:
    - Extension name: address_standardizer
    - Install command: CREATE EXTENSION address_standardizer;
    - Minimum version: 3.0
    - Documentation: https://postgis.net/address_standardizer
    """

    def supports_address_standardizer(self) -> bool:
        """Whether address_standardizer is available."""
        ...
