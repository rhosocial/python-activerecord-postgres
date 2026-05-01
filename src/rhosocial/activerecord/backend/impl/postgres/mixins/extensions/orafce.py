# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/orafce.py
"""
PostgreSQL orafce Oracle compatibility functionality mixin.

This module provides functionality to check orafce extension features.

For SQL expression generation, use the function factories in
``functions/orafce.py`` instead of the removed format_* methods.
"""


class PostgresOrafceMixin:
    """orafce Oracle compatibility functionality implementation."""

    def supports_orafce(self) -> bool:
        """Check if orafce extension is available."""
        return self.is_extension_installed("orafce")
