# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/btree_gist.py
"""
btree_gist composite index functionality implementation.

This module provides the PostgresBtreeGistMixin class that adds support for
btree_gist extension features.
"""

from typing import List, Optional

from rhosocial.activerecord.backend.expression.objects import Index, Table

from ...expression.ddl import PostgresCreateIndexExpression


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
        expr = PostgresCreateIndexExpression(
            self,
            index=Index(self, index_name),
            table=Table(self, table_name),
            columns=list(columns),
            index_type="GIST",
            include=list(include) if include else None,
        )
        return expr.to_sql()[0]

    def format_btree_gist_operator_class(self, data_type: str) -> str:
        """Format btree_gist operator class name.

        Args:
            data_type: Data type name (e.g., int4, text, timestamp)

        Returns:
            Operator class name for btree_gist (e.g., int4_ops for int4)
        """
        return f"{data_type}_ops"