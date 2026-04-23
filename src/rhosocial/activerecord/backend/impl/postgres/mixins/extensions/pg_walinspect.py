# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_walinspect.py
"""
pg_walinspect WAL inspection functionality implementation.

This module provides the PostgresPgWalinspectMixin class that adds support for
pg_walinspect extension features.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PostgresPgWalinspectMixin:
    """pg_walinspect WAL inspection functionality implementation."""

    def supports_pg_walinspect(self) -> bool:
        """Check if pg_walinspect extension is available."""
        return self.is_extension_installed("pg_walinspect")

    def format_pg_get_wal_records_info(self) -> str:
        """Format WAL records info query.

        Returns:
            SQL function call
        """
        return "SELECT * FROM pg_get_wal_records_info()"

    def format_pg_get_wal_blocks_info(self) -> str:
        """Format WAL blocks info query.

        Returns:
            SQL function call
        """
        return "SELECT * FROM pg_get_wal_blocks_info()"

    def format_pg_logical_emit_message(
        self, transactional: bool = False, prefix: str = "test"
    ) -> str:
        """Format logical WAL message emission.

        Args:
            transactional: Use transactional mode
            prefix: Message prefix

        Returns:
            SQL SELECT statement
        """
        trans = "true" if transactional else "false"
        return f"SELECT pg_logical_emit_message({trans}, '{prefix}', '')"