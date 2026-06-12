# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/pg_partman.py
"""PostgreSQL pg_partman extension expressions.

pg_partman is a PostgreSQL extension for automated partition management.
This module provides expression classes that encapsulate pg_partman function
calls and config operations as BaseExpression instances.

Reference:
    https://github.com/pgpartman/pg_partman
"""

from typing import Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresPgPartmanCreateParentExpression",
    "PostgresPgPartmanRunMaintenanceExpression",
    "PostgresPgPartmanUpdateConfigExpression",
    "PostgresPgPartmanDeleteConfigExpression",
]


class PostgresPgPartmanCreateParentExpression(BaseExpression):
    """Expression for pg_partman create_parent().

    Initializes automated partitioning for a parent table. pg_partman will
    automatically create and manage child partitions based on the configured
    interval and premake count.

    Attributes:
        parent_table: Fully qualified parent table name (e.g. 'public.events').
        control: Column name used as the partition control field.
        interval: Partition interval (e.g. '1 day', '1 month', '1 year').
        partition_type: Partition method ('native' or 'range'). Defaults to 'native'.
        premake: Number of future partitions to pre-create. Defaults to 4.
        start_partition: Optional starting partition value (e.g. '2026-01-01').
        primary_key: Optional primary key override for the parent table.
        default_table: Whether to create a default table for non-matching rows.
        constraint_cols: Optional list of columns for partition constraint.
        template_table: Optional template table for new partition structure.
        epoch: Optional epoch setting for time-based partitioning ('none', 'seconds', 'milliseconds').
        jobmon: Whether to use pg_jobmon for logging. Defaults to True.
        schema: Schema where pg_partman functions reside. Defaults to 'partman'.

    Delegates to dialect.format_pg_partman_create_parent() for SQL generation.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        parent_table: str,
        control: str,
        interval: str,
        partition_type: str = "native",
        premake: int = 4,
        start_partition: Optional[str] = None,
        primary_key: Optional[str] = None,
        default_table: Optional[bool] = None,
        constraint_cols: Optional[list] = None,
        template_table: Optional[str] = None,
        epoch: Optional[str] = None,
        jobmon: Optional[bool] = None,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.parent_table = parent_table
        self.control = control
        self.interval = interval
        self.partition_type = partition_type
        self.premake = premake
        self.start_partition = start_partition
        self.primary_key = primary_key
        self.default_table = default_table
        self.constraint_cols = constraint_cols
        self.template_table = template_table
        self.epoch = epoch
        self.jobmon = jobmon
        self.schema = schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate SQL for pg_partman create_parent().

        Returns:
            Tuple of (SELECT function_call SQL, params tuple).
        """
        return self.dialect.format_pg_partman_create_parent(self)


class PostgresPgPartmanRunMaintenanceExpression(BaseExpression):
    """Expression for pg_partman run_maintenance().

    Triggers pg_partman maintenance which creates new partitions and detaches
    old ones according to each table's configuration. Can be scoped to a
    specific parent table or run globally.

    Attributes:
        parent_table: Optional parent table to scope maintenance to.
        schema: Schema where pg_partman functions reside. Defaults to 'partman'.

    Delegates to dialect.format_pg_partman_run_maintenance() for SQL generation.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        parent_table: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.parent_table = parent_table
        self.schema = schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate SQL for pg_partman run_maintenance().

        Returns:
            Tuple of (SELECT function_call SQL, params tuple).
        """
        return self.dialect.format_pg_partman_run_maintenance(self)


class PostgresPgPartmanUpdateConfigExpression(BaseExpression):
    """Expression for updating pg_partman part_config options.

    Updates the pg_partman configuration for a specific parent table.
    Only the specified options are modified; unspecified options remain
    unchanged. At least one option must be provided.

    Attributes:
        parent_table: Parent table whose config to update.
        automatic_maintenance: 'on' or 'off' for auto maintenance.
        infinite_time_partitions: Allow unlimited future partition creation.
        retention: Retention period (e.g. '3 months', '90 days').
        retention_keep_table: Keep table data after retention drop.
        retention_keep_index: Keep indexes after retention drop.
        schema: Schema where part_config table resides. Defaults to 'partman'.

    Raises:
        ValueError: If no config options are specified.

    Delegates to dialect.format_pg_partman_update_config() for SQL generation.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        parent_table: str,
        *,
        automatic_maintenance: Optional[str] = None,
        infinite_time_partitions: Optional[bool] = None,
        retention: Optional[str] = None,
        retention_keep_table: Optional[bool] = None,
        retention_keep_index: Optional[bool] = None,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.parent_table = parent_table
        self.automatic_maintenance = automatic_maintenance
        self.infinite_time_partitions = infinite_time_partitions
        self.retention = retention
        self.retention_keep_table = retention_keep_table
        self.retention_keep_index = retention_keep_index
        self.schema = schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate SQL for updating pg_partman part_config.

        Returns:
            Tuple of (UPDATE SQL, params tuple).

        Raises:
            ValueError: If no config options are specified.
        """
        return self.dialect.format_pg_partman_update_config(self)


class PostgresPgPartmanDeleteConfigExpression(BaseExpression):
    """Expression for deleting a pg_partman part_config row.

    Removes the pg_partman configuration entry for a specific parent table,
    stopping automated partition management for that table.

    Attributes:
        parent_table: Parent table whose config to delete.
        schema: Schema where part_config table resides. Defaults to 'partman'.

    Delegates to dialect.format_pg_partman_delete_config() for SQL generation.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        parent_table: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.parent_table = parent_table
        self.schema = schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate SQL for deleting a pg_partman part_config row.

        Returns:
            Tuple of (DELETE SQL, params tuple).
        """
        return self.dialect.format_pg_partman_delete_config(self)
