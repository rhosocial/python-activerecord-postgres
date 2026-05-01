# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_surgery.py
"""
PostgreSQL pg_surgery data repair functionality mixin.

This module provides functionality to check pg_surgery extension features.

For SQL expression generation (e.g. heap_freeze, heap_page_header),
use the function factories in ``functions/pg_surgery.py``
instead of the removed format_* methods.
"""


class PostgresPgSurgeryMixin:
    """pg_surgery data repair functionality implementation."""

    def supports_pg_surgery(self) -> bool:
        """Check if pg_surgery extension is available."""
        return self.is_extension_installed("pg_surgery")
