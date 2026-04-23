# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_repack.py
"""
pg_repack online rebuild functionality implementation.

This module provides the PostgresPgRepackMixin class that adds support for
pg_repack extension features.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass


class PostgresPgRepackMixin:
    """pg_repack online rebuild functionality implementation."""

    def supports_pg_repack(self) -> bool:
        """Check if pg_repack extension is available."""
        return self.is_extension_installed("pg_repack")

    def format_pg_repack_table(self, table_name: str) -> str:
        """Format pg_repack table rebuild.

        Args:
            table_name: Name of the table

        Returns:
            SQL SELECT statement
        """
        return f"SELECT pg_repack.repack_table('{table_name}')"

    def format_pg_repack_index(self, index_name: str) -> str:
        """Format pg_repack index rebuild.

        Args:
            index_name: Name of the index

        Returns:
            SQL SELECT statement
        """
        return f"SELECT pg_repack.repack_index('{index_name}')"

    def format_pg_repack_move_tablespace(
        self, table_name: str, tablespace: str
    ) -> str:
        """Format pg_repack table move to tablespace.

        Args:
            table_name: Name of the table
            tablespace: Target tablespace

        Returns:
            SQL SELECT statement
        """
        return (
            f"SELECT pg_repack.move_tablespace("
            f"'{table_name}', '{tablespace}')"
        )