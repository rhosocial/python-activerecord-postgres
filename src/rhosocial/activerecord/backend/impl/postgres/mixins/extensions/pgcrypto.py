# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgcrypto.py
"""
PostgreSQL pgcrypto cryptographic functionality mixin.

This module provides functionality to check pgcrypto extension features.

For SQL expression generation, use the function factories in
``functions/pgcrypto.py`` instead of the removed format_* methods.
"""


class PostgresPgcryptoMixin:
    """pgcrypto cryptographic functionality implementation."""

    def supports_pgcrypto(self) -> bool:
        """Check if pgcrypto extension is available."""
        return self.is_extension_installed("pgcrypto")
