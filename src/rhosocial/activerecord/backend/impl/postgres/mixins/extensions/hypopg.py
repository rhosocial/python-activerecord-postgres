# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/hypopg.py
"""
hypopg hypothetical indexes functionality implementation.

This module provides the PostgresHypoPgMixin class that adds support for
hypopg extension features.
"""

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    pass


class PostgresHypoPgMixin:
    """hypopg hypothetical indexes functionality implementation."""

    def supports_hypopg(self) -> bool:
        """Check if hypopg extension is available."""
        return self.is_extension_installed("hypopg")

    def format_hypopg_create_index(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
        index_type: str = "btree",
    ) -> str:
        """Format a hypothetical index creation.

        Args:
            index_name: Name of the index
            table_name: Name of the table
            columns: List of columns
            index_type: Index type (btree, gist, etc.)

        Returns:
            SQL SELECT statement
        """
        col_str = ", ".join(columns)
        return (
            f"SELECT hypopg.create_index("
            f"'{index_name}', '{table_name}', ARRAY[{col_str}], '{index_type}')"
        )

    def format_hypopg_reset(self) -> str:
        """Format hypothetical index reset.

        Returns:
            SQL SELECT statement
        """
        return "SELECT hypopg.reset()"

    def format_hypopg_show_indexes(self) -> str:
        """Format show all hypothetical indexes.

        Returns:
            SQL SELECT statement
        """
        return "SELECT * FROM hypopg_index_detail()"