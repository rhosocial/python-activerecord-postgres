# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_partman.py
"""
PostgreSQL pg_partman partition management functionality mixin.

This module provides functionality to check pg_partman extension features
and generate pg_partman expression SQL.

All SQL generation for pg_partman operations must go through expression
classes and dialect formatter methods. Do not build pg_partman SQL directly
in tests or callers.
"""

from typing import Any, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import (
        PostgresPgPartmanCreateParentExpression,
        PostgresPgPartmanRunMaintenanceExpression,
        PostgresPgPartmanUpdateConfigExpression,
    )


class PostgresPgPartmanMixin:
    """pg_partman partition management functionality implementation."""

    def supports_pg_partman(self) -> bool:
        """Check if pg_partman extension is available."""
        return self.is_extension_installed("pg_partman")

    def supports_pg_partman_auto_partition(self) -> bool:
        """Check if pg_partman supports auto partitioning."""
        return self.check_extension_feature("pg_partman", "auto_partition")

    def format_pg_partman_create_parent(
        self,
        expr: "PostgresPgPartmanCreateParentExpression",
    ) -> tuple:
        """Format pg_partman create_parent() expression."""
        from rhosocial.activerecord.backend.expression import QualifiedIdentifierExpression
        schema = expr.schema or "partman"
        function_name_sql, _ = QualifiedIdentifierExpression(
            dialect=self, schema=schema, name="create_parent"
        ).to_sql()
        placeholder = self.get_parameter_placeholder()
        return (
            f"SELECT {function_name_sql}("
            f"p_parent_table := {placeholder}::text, "
            f"p_control := {placeholder}::text, "
            f"p_interval := {placeholder}::text, "
            f"p_type := {placeholder}::text, "
            f"p_premake := {placeholder}::int"
            f")",
            (expr.parent_table, expr.control, expr.interval, expr.partition_type, expr.premake),
        )

    def format_pg_partman_run_maintenance(
        self,
        expr: "PostgresPgPartmanRunMaintenanceExpression",
    ) -> tuple:
        """Format pg_partman run_maintenance() expression."""
        from rhosocial.activerecord.backend.expression import QualifiedIdentifierExpression
        schema = expr.schema or "partman"
        function_name_sql, _ = QualifiedIdentifierExpression(
            dialect=self, schema=schema, name="run_maintenance"
        ).to_sql()
        if expr.parent_table is None:
            return f"SELECT {function_name_sql}()", ()
        placeholder = self.get_parameter_placeholder()
        return f"SELECT {function_name_sql}({placeholder}::text)", (expr.parent_table,)

    def format_pg_partman_update_config(
        self,
        expr: "PostgresPgPartmanUpdateConfigExpression",
    ) -> tuple:
        """Format pg_partman part_config update expression."""
        from rhosocial.activerecord.backend.expression import QualifiedIdentifierExpression
        assignments: List[str] = []
        params: List[Any] = []
        placeholder = self.get_parameter_placeholder()

        if expr.automatic_maintenance is not None:
            assignments.append(f"automatic_maintenance = {placeholder}")
            params.append(expr.automatic_maintenance)
        if expr.infinite_time_partitions is not None:
            assignments.append(f"infinite_time_partitions = {placeholder}")
            params.append(expr.infinite_time_partitions)
        if expr.retention is not None:
            assignments.append(f"retention = {placeholder}")
            params.append(expr.retention)
        if expr.retention_keep_table is not None:
            assignments.append(f"retention_keep_table = {placeholder}")
            params.append(expr.retention_keep_table)
        if expr.retention_keep_index is not None:
            assignments.append(f"retention_keep_index = {placeholder}")
            params.append(expr.retention_keep_index)

        if not assignments:
            raise ValueError("At least one pg_partman config option must be specified.")

        schema = expr.schema or "partman"
        config_table_sql, _ = QualifiedIdentifierExpression(
            dialect=self, schema=schema, name="part_config"
        ).to_sql()
        params.append(expr.parent_table)
        return (
            f"UPDATE {config_table_sql} "
            f"SET {', '.join(assignments)} "
            f"WHERE parent_table = {placeholder}",
            tuple(params),
        )
