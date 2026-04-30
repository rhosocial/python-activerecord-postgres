# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/fuzzystrmatch.py
"""
PostgreSQL fuzzystrmatch fuzzy string matching functionality mixin.

This module provides functionality to check fuzzystrmatch extension features.

For SQL expression generation, use the function factories in
``functions/fuzzystrmatch.py`` instead of the removed format_* methods.
"""


class PostgresFuzzystrmatchMixin:
    """fuzzystrmatch fuzzy string matching functionality implementation."""

    def supports_fuzzystrmatch(self) -> bool:
        """Check if fuzzystrmatch extension is available."""
        return self.is_extension_installed("fuzzystrmatch")
