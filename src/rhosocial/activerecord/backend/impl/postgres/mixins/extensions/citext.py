# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/citext.py
"""
citext case-insensitive text functionality implementation.

This module provides the PostgresCitextMixin class that adds support for
citext extension features.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass


class PostgresCitextMixin:
    """citext case-insensitive text functionality implementation."""

    def supports_citext_type(self) -> bool:
        """Check if citext type is supported."""
        return self.check_extension_feature("citext", "type")

    def format_citext_column(self, column_name: str, length: Optional[int] = None) -> str:
        """Format a citext column definition.

        Args:
            column_name: Column name
            length: Optional maximum length

        Returns:
            SQL column definition
        """
        if length:
            return f"{column_name} CITEXT({length})"
        return f"{column_name} CITEXT"

    def format_citext_literal(self, value: str) -> str:
        """Format a citext type literal value.

        Args:
            value: The text value

        Returns:
            SQL citext literal string

        Example:
            >>> format_citext_literal('Hello World')
            "'Hello World'::citext"
        """
        escaped = value.replace("'", "''")
        return f"'{escaped}'::citext"