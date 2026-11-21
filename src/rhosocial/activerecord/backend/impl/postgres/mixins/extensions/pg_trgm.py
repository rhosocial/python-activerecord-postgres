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

from rhosocial.activerecord.backend.expression.objects import Table

from ...expression.ddl import PostgresIndexType


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
        full_table, _ = self.format_table_object(Table(self, table_name, schema_name=schema)
        )
        access_method = PostgresIndexType.normalize(index_type).value
        if access_method not in ("gin", "gist"):
            raise ValueError(
                f"pg_trgm index_type must be 'gin' or 'gist', got {index_type!r}"
            )
        # gin_trgm_ops is the GIN operator class; GiST has its own. Rendering
        # the GIN class on a GiST index produces "operator class ... does not
        # accept access method gist" on every server that has both.
        opclass = "gin_trgm_ops" if access_method == "gin" else "gist_trgm_ops"
        sql = (
            f"CREATE INDEX {self.format_identifier(index_name)} ON {full_table} "
            f"USING {access_method} ({self.format_identifier(column_name)} {opclass})"
        )
        return (sql, ())
