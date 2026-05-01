# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_repack.py
"""
PostgreSQL pg_repack online rebuild functionality mixin.

This module provides functionality to check pg_repack extension features.

For SQL expression generation, use the function factories in
``functions/pg_repack.py`` instead of the removed format_* methods.
"""


class PostgresPgRepackMixin:
    """pg_repack online rebuild functionality implementation."""

    def supports_pg_repack(self) -> bool:
        """Check if pg_repack extension is available."""
        return self.is_extension_installed("pg_repack")
