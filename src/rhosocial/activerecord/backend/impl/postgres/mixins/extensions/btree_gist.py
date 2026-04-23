# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/btree_gist.py
"""
btree_gist composite index functionality implementation.

This module provides the PostgresBtreeGistMixin class that adds support for
btree_gist extension features.
"""

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    pass


class PostgresBtreeGistMixin:
    """btree_gist composite index functionality implementation."""

    def supports_btree_gist(self) -> bool:
        """Check if btree_gist extension is available."""
        return self.is_extension_installed("btree_gist")

    def format_gist_index(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
        include: Optional[List[str]] = None,
    ) -> str:
        """Format a GiST index using btree_gist.

        Args:
            index_name: Name of the index
            table_name: Name of the table
            columns: List of columns
            include: Optional included columns

        Returns:
            SQL CREATE INDEX statement
        """
        col_str = ", ".join(columns)
        inc_str = f" INCLUDE ({', '.join(include)})" if include else ""
        return f"CREATE INDEX {index_name} ON {table_name} USING gist ({col_str}){inc_str}"