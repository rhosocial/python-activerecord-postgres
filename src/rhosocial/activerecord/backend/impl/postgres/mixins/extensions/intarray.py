"""PostgreSQL intarray integer array functionality mixin.

This module provides functionality to check intarray extension features,
including operators, functions, and index support.

For SQL expression generation, use the function factories in
``functions/intarray.py`` instead of the removed format_* methods.
"""

from typing import Optional


class PostgresIntarrayMixin:
    """intarray integer array implementation."""

    def supports_intarray_operators(self) -> bool:
        """Check if intarray supports operators."""
        return self.check_extension_feature("intarray", "operators")

    def supports_intarray_functions(self) -> bool:
        """Check if intarray supports functions."""
        return self.check_extension_feature("intarray", "functions")

    def supports_intarray_index(self) -> bool:
        """Check if intarray supports index."""
        return self.check_extension_feature("intarray", "index")

    def format_intarray_index_statement(self, table_name: str, column_name: str, index_name: Optional[str] = None) -> str:
        """Format CREATE INDEX statement with GIN intarray ops.

        Args:
            table_name: Table name
            column_name: Integer array column name
            index_name: Optional index name (auto-generated if None)

        Returns:
            SQL CREATE INDEX statement

        Example:
            >>> format_intarray_index_statement('documents', 'tags', 'idx_tags')
            "CREATE INDEX idx_tags ON documents USING gin (tags gin__int_ops)"
            >>> format_intarray_index_statement('documents', 'tags')
            "CREATE INDEX idx_documents_tags ON documents USING gin (tags gin__int_ops)"
        """
        if index_name is None:
            index_name = f"idx_{table_name}_{column_name}"
        return f"CREATE INDEX {index_name} ON {table_name} USING gin ({column_name} gin__int_ops)"
