# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/bloom.py
"""
bloom filter index functionality implementation.

This module provides the PostgresBloomMixin class that adds support for
bloom extension features.
"""

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    pass


class PostgresBloomMixin:
    """bloom filter index functionality implementation."""

    def supports_bloom_index(self) -> bool:
        """Check if bloom index is supported."""
        return self.check_extension_feature("bloom", "index")

    def format_bloom_index(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
        fill_factor: Optional[int] = None,
    ) -> str:
        """Format a bloom index creation.

        Args:
            index_name: Name of the index
            table_name: Name of the table
            columns: List of columns to index
            fill_factor: Optional fill factor (0-100)

        Returns:
            SQL CREATE INDEX statement
        """
        col_str = ", ".join(columns)
        fill = f" WITH (fillfactor={fill_factor})" if fill_factor else ""
        return f"CREATE INDEX {index_name} ON {table_name} USING bloom ({col_str}){fill}"

    def format_bloom_access_method(self) -> str:
        """Format bloom as access method.

        Returns:
            SQL set option for bloom
        """
        return "SET default_index_access_method = 'bloom'"