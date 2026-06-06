# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/pg_partman.py
"""PostgreSQL pg_partman extension expressions."""

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
    """Expression for pg_partman create_parent()."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        parent_table: str,
        control: str,
        interval: str,
        partition_type: str = "native",
        premake: int = 4,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.parent_table = parent_table
        self.control = control
        self.interval = interval
        self.partition_type = partition_type
        self.premake = premake
        self.schema = schema

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate SQL for pg_partman create_parent()."""
        return self.dialect.format_pg_partman_create_parent(self)


class PostgresPgPartmanRunMaintenanceExpression(BaseExpression):
    """Expression for pg_partman run_maintenance()."""

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
        """Generate SQL for pg_partman run_maintenance()."""
        return self.dialect.format_pg_partman_run_maintenance(self)


class PostgresPgPartmanUpdateConfigExpression(BaseExpression):
    """Expression for updating pg_partman part_config options."""

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
        """Generate SQL for updating pg_partman part_config."""
        return self.dialect.format_pg_partman_update_config(self)


class PostgresPgPartmanDeleteConfigExpression(BaseExpression):
    """Expression for deleting a pg_partman part_config row."""

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
        """Generate SQL for deleting a pg_partman part_config row."""
        return self.dialect.format_pg_partman_delete_config(self)
