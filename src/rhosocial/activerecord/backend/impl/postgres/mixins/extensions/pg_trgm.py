# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_trgm.py
"""
PostgreSQL pg_trgm trigram functionality mixin.

This module provides functionality to check pg_trgm extension features,
including similarity functions and trigram indexes.

For SQL expression generation, use the function factories in
``functions/pg_trgm.py`` instead of the removed format_* methods.
For DDL index creation, use ``format_trgm_index_statement``.
"""

from typing import Optional, Tuple


class PostgresPgTrgmMixin:
    """pg_trgm trigram functionality implementation."""

    def supports_pg_trgm_similarity(self) -> bool:
        """Check if pg_trgm supports similarity functions."""
        return self.check_extension_feature("pg_trgm", "similarity")

    def supports_pg_trgm_index(self) -> bool:
        """Check if pg_trgm supports trigram index."""
        return self.check_extension_feature("pg_trgm", "index")

    def format_trgm_index_statement(
        self, index_name: str, table_name: str, column_name: str, index_type: str = "gin", schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for trigram index.

        Args:
            index_name: Name of the index
            table_name: Table name
            column_name: Text column name
            index_type: Index type - 'gin' (default) or 'gist'
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = f"CREATE INDEX {index_name} ON {full_table} USING {index_type} ({column_name} gin_trgm_ops)"
        return (sql, ())
