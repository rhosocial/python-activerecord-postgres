# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_walinspect.py
"""
PostgreSQL pg_walinspect WAL inspection functionality mixin.

This module provides functionality to check pg_walinspect extension features.

For SQL expression generation (e.g. pg_get_wal_records_info,
pg_get_wal_blocks_info, pg_logical_emit_message), use the function factories
in ``functions/pg_walinspect.py`` instead of the removed format_* methods.
"""


class PostgresPgWalinspectMixin:
    """pg_walinspect WAL inspection functionality implementation."""

    def supports_pg_walinspect(self) -> bool:
        """Check if pg_walinspect extension is available."""
        return self.is_extension_installed("pg_walinspect")
