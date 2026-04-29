# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_partman.py
"""
PostgreSQL pg_partman partition management functionality mixin.

This module provides functionality to check pg_partman extension features
and generate partition configuration DDL statements.

For SQL expression generation of partition creation and maintenance functions,
use the function factories in ``functions/pg_partman.py`` instead of the
removed format_* methods.
For partition configuration updates, use ``format_auto_partition_config``.
"""

from typing import Optional


class PostgresPgPartmanMixin:
    """pg_partman partition management functionality implementation."""

    def supports_pg_partman(self) -> bool:
        """Check if pg_partman extension is available."""
        return self.is_extension_installed("pg_partman")

    def supports_pg_partman_auto_partition(self) -> bool:
        """Check if pg_partman supports auto partitioning."""
        return self.check_extension_feature("pg_partman", "auto_partition")

    def format_auto_partition_config(
        self,
        table_name: str,
        automatic: bool = True,
        infinite_time_partitions: bool = False,
        retention: Optional[str] = None,
    ) -> str:
        """Format auto partition configuration update.

        Args:
            table_name: Name of the partitioned table
            automatic: Enable automatic partition creation
            infinite_time_partitions: Create partitions infinitely into the future
            retention: Optional retention period (e.g., '30 days')

        Returns:
            SQL UPDATE statement for part_config
        """
        parts = []
        if not automatic:
            parts.append("automatic = false")
        if infinite_time_partitions:
            parts.append("infinite_time_partitions = true")
        if retention:
            parts.append(f"retention = '{retention}'")
        if not parts:
            parts.append("automatic = true")
        set_clause = ", ".join(parts)
        return (
            f"UPDATE partman.part_config SET {set_clause} "
            f"WHERE parent_table = '{table_name}'"
        )
