# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/hypopg.py
"""
PostgreSQL hypopg hypothetical indexes functionality mixin.

This module provides functionality to check hypopg extension features.

For SQL expression generation (e.g. hypopg_create_index, hypopg_reset,
hypopg_show_indexes, hypopg_estimate_size), use the function factories in
``functions/hypopg.py`` instead of the removed format_* methods.
"""


class PostgresHypoPgMixin:
    """hypopg hypothetical indexes functionality implementation."""

    def supports_hypopg(self) -> bool:
        """Check if hypopg extension is available."""
        return self.is_extension_installed("hypopg")
