"""
PostgreSQL amcheck index integrity checking mixin.

This module provides functionality to check amcheck extension features
and generate verification statements.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/amcheck.html
"""

from typing import Optional, Tuple


class PostgresAmcheckMixin:
    """amcheck index integrity checking implementation."""

    def supports_amcheck_bt_index_check(self) -> bool:
        """Check if bt_index_check() is available."""
        return self.check_extension_feature("amcheck", "bt_index_check")

    def supports_amcheck_bt_index_parent_check(self) -> bool:
        """Check if bt_index_parent_check() is available."""
        return self.check_extension_feature("amcheck", "bt_index_parent_check")

    def supports_amcheck_heap_verification(self) -> bool:
        """Check if verify_heapam() is available (requires PostgreSQL 14+)."""
        return self.version >= (14, 0, 0) and self.check_extension_feature("amcheck", "verify_heapam")

    def format_verify_index_statement(
        self, index_name: str, schema: Optional[str] = None, parent_check: bool = False
    ) -> Tuple[str, tuple]:
        """Format statement to verify a B-tree index.

        Args:
            index_name: Name of the index to verify
            schema: Optional schema name
            parent_check: If True, use bt_index_parent_check for thorough verification

        Returns:
            Tuple of (SQL statement, parameters)
        """
        qualified = f"{schema}.{index_name}" if schema else index_name
        func = "bt_index_parent_check" if parent_check else "bt_index_check"
        sql = f"SELECT {func}('{qualified}')"
        return (sql, ())

    def format_verify_table_statement(
        self, table_name: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format statement to verify all indexes on a table.

        Generates a DO block that calls bt_index_check for each index.

        Args:
            table_name: Name of the table
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = (
            f"DO $$DECLARE idx text; BEGIN "
            f"FOR idx IN SELECT indexname FROM pg_indexes WHERE tablename = '{table_name}'"
            f"{' AND schemaname = ' + repr(schema) if schema else ''} "
            f"LOOP EXECUTE format('SELECT bt_index_check(%%I)', idx); END LOOP; END$$"
        )
        return (sql, ())