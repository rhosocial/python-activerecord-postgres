"""
PostgreSQL pageinspect page-level inspection mixin.

This module provides functionality to check pageinspect extension features
and generate inspection statements.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pageinspect.html
"""

from typing import Optional, Tuple


class PostgresPageinspectMixin:
    """pageinspect page-level inspection implementation."""

    def supports_pageinspect_heap(self) -> bool:
        """Check if heap page inspection functions are available."""
        return self.check_extension_feature("pageinspect", "heap")

    def supports_pageinspect_btree(self) -> bool:
        """Check if B-tree page inspection functions are available."""
        return self.check_extension_feature("pageinspect", "btree")

    def supports_pageinspect_brin(self) -> bool:
        """Check if BRIN page inspection functions are available (PG 11+)."""
        return self.version >= (11, 0, 0) and self.check_extension_feature("pageinspect", "brin")

    def format_heap_page_statement(
        self, table_name: str, page_number: int, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format statement to inspect heap page items.

        Args:
            table_name: Name of the table
            page_number: Page number to inspect (0-based)
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = (
            f"SELECT * FROM heap_page_items(get_raw_page('{full_table}', {page_number}))"
        )
        return (sql, ())

    def format_btree_metapage_statement(
        self, index_name: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format statement to show B-tree meta page information.

        Args:
            index_name: Name of the index
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)
        """
        qualified = f"{schema}.{index_name}" if schema else index_name
        sql = f"SELECT * FROM bt_metap('{qualified}')"
        return (sql, ())