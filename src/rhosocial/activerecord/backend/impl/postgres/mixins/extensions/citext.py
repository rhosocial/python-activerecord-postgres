# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/citext.py
"""
PostgreSQL citext case-insensitive text functionality mixin.

This module provides functionality to check citext extension features
and generate DDL column definitions for citext columns.

For citext literal expression generation, use the function factories in
``functions/citext.py`` instead of the removed format_citext_literal method.
"""

from typing import Optional


class PostgresCitextMixin:
    """citext case-insensitive text functionality implementation."""

    def supports_citext_type(self) -> bool:
        """Check if citext type is supported."""
        return self.check_extension_feature("citext", "type")

    def format_citext_column(self, column_name: str, length: Optional[int] = None) -> str:
        """Format a citext column definition.

        This generates a DDL column definition for use in CREATE TABLE or
        ALTER TABLE statements, not a function expression.

        Args:
            column_name: Column name
            length: Optional maximum length

        Returns:
            SQL column definition
        """
        if length:
            return f"{column_name} CITEXT({length})"
        return f"{column_name} CITEXT"
