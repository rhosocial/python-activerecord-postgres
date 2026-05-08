"""
PostgreSQL pg_logicalinspect logical replication inspection mixin.

This module provides functionality to check pg_logicalinspect extension features
and generate inspection statements.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pglogicalinspect.html
"""

from typing import Tuple


class PostgresPgLogicalinspectMixin:
    """pg_logicalinspect logical replication inspection implementation."""

    def supports_pg_logicalinspect(self) -> bool:
        """Check if pg_logicalinspect functions are available."""
        return self.check_extension_feature("pg_logicalinspect", "functions")

    def format_inspect_slot_statement(self, slot_name: str, limit: int = 100) -> Tuple[str, tuple]:
        """Format statement to inspect changes in a logical replication slot.

        Args:
            slot_name: Name of the logical replication slot
            limit: Maximum number of changes to return

        Returns:
            Tuple of (SQL statement, parameters)
        """
        sql = (
            f"SELECT * FROM pg_logicalinspect_get_changes('{slot_name}', NULL, NULL, "
            f"limit => {limit})"
        )
        return (sql, ())