# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/ltree.py
"""
PostgreSQL ltree label tree functionality mixin.

This module provides functionality to check ltree extension features,
including type support and operators.

For SQL expression generation, use the function factories in
``functions/ltree.py`` instead of the removed format_* methods.
For DDL index creation, use ``format_ltree_index_statement``.
"""

from typing import Optional, Tuple


class PostgresLtreeMixin:
    """ltree label tree implementation."""

    def supports_ltree_type(self) -> bool:
        """Check if ltree supports ltree type."""
        return self.check_extension_feature("ltree", "type")

    def supports_ltree_operators(self) -> bool:
        """Check if ltree supports operators."""
        return self.check_extension_feature("ltree", "operators")

    def supports_ltree_index(self) -> bool:
        """Check if ltree supports index."""
        return self.check_extension_feature("ltree", "index")

    def format_ltree_index_statement(
        self, index_name: str, table_name: str, column_name: str, index_type: str = "gist", schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for ltree column.

        Args:
            index_name: Name of the index
            table_name: Table name
            column_name: ltree column name
            index_type: Index type - 'gist' (default) or 'btree'
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = f"CREATE INDEX {index_name} ON {full_table} USING {index_type} ({column_name})"
        return (sql, ())
