# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_surgery.py
"""
pg_surgery data repair functionality implementation.

This module provides the PostgresPgSurgeryMixin class that adds support for
pg_surgery extension features.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PostgresPgSurgeryMixin:
    """pg_surgery data repair functionality implementation."""

    def supports_pg_surgery(self) -> bool:
        """Check if pg_surgery extension is available."""
        return self.is_extension_installed("pg_surgery")

    def format_pg_surgery_heap_freeze(self, table_name: str) -> str:
        """Format heap freeze operation.

        Args:
            table_name: Name of the table

        Returns:
            SQL SELECT statement
        """
        return f"SELECT pg_surgery.freeze_heap('{table_name}')"

    def format_pg_surgery_heap_page_header(
        self, table_name: str, page_offset: int, page_pid: int
    ) -> str:
        """Format heap page header repair.

        Args:
            table_name: Name of the table
            page_offset: Page offset
            page_pid: Page PID

        Returns:
            SQL SELECT statement
        """
        return (
            f"SELECT pg_surgery.set_heap_tuple_frozen("
            f"'{table_name}', {page_offset}, {page_pid})"
        )