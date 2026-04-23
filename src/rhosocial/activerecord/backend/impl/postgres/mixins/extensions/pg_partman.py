# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_partman.py
"""
pg_partman partition management functionality implementation.

This module provides the PostgresPgPartmanMixin class that adds support for
pg_partman extension features.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass


class PostgresPgPartmanMixin:
    """pg_partman partition management functionality implementation."""

    def supports_pg_partman(self) -> bool:
        """Check if pg_partman extension is available."""
        return self.is_extension_installed("pg_partman")

    def supports_pg_partman_auto_partition(self) -> bool:
        """Check if pg_partman supports auto partitioning."""
        return self.check_extension_feature("pg_partman", "auto_partition")

    def format_create_partition_time(
        self,
        table_name: str,
        partition_type: str = "daily",
        interval: Optional[str] = None,
        preload: bool = True,
    ) -> str:
        """Format time-based partition creation.

        Args:
            table_name: Name of the partitioned table
            partition_type: daily, hourly, etc.
            interval: Partition interval
            preload: Preload partition data

        Returns:
            SQL SELECT statement
        """
        interval_str = f", '{interval}'" if interval else ""
        pre = "true" if preload else "false"
        return (
            f"SELECT partman.create_time_based_partition_set("
            f"'{table_name}', '{partition_type}'{interval_str}, "
            f"pre_make := {pre})"
        )

    def format_create_partition_id(
        self,
        table_name: str,
        interval: int = 10000,
        preload: bool = True,
    ) -> str:
        """Format ID-based partition creation.

        Args:
            table_name: Name of the partitioned table
            interval: Number of IDs per partition
            preload: Preload partition data

        Returns:
            SQL SELECT statement
        """
        pre = "true" if preload else "false"
        return (
            f"SELECT partman.create_id_based_partition_set("
            f"'{table_name}', p_interval := {interval}, "
            f"pre_make := {pre})"
        )

    def format_run_maintenance(self, config: Optional[str] = None) -> str:
        """Format partition maintenance execution.

        Args:
            config: Optional partition config table

        Returns:
            SQL SELECT statement
        """
        conf = f", '{config}'" if config else ""
        return f"SELECT partman.run_maintenance(){conf}"