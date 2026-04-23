# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/btree_gin.py
"""
btree_gin composite index functionality implementation.

This module provides the PostgresBtreeGinMixin class that adds support for
btree_gin extension features.
"""

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    pass


class PostgresBtreeGinMixin:
    """btree_gin composite index functionality implementation."""

    def supports_btree_gin(self) -> bool:
        """Check if btree_gin extension is available."""
        return self.is_extension_installed("btree_gin")

    def format_gin_index(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
    ) -> str:
        """Format a GIN index using btree_gin.

        Args:
            index_name: Name of the index
            table_name: Name of the table
            columns: List of columns

        Returns:
            SQL CREATE INDEX statement
        """
        col_str = ", ".join(columns)
        return f"CREATE INDEX {index_name} ON {table_name} USING gin ({col_str})"