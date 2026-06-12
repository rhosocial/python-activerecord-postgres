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
        PostgresPgPartmanDeleteConfigExpression,
    )


class PostgresPgPartmanMixin:
    """pg_partman partition management functionality implementation.

    pg_partman is a PostgreSQL extension for automated time- and serial-based
    partition management. This mixin provides SQL generation for its core
    functions: create_parent, run_maintenance, and config management.

    All methods follow the Expression-Dialect-Protocol pattern: expressions
    hold data, mixins generate SQL, and protocols define the interface.

    Extension detection relies on self.is_extension_installed() and
    self.check_extension_feature(), which must be provided by the host class.
    """

    def supports_pg_partman(self) -> bool:
        """Check if pg_partman extension is available.

        Queries pg_extension catalog to determine if pg_partman is installed.

        Returns:
            True if pg_partman extension is installed.
        """
        return self.is_extension_installed("pg_partman")

    def supports_pg_partman_auto_partition(self) -> bool:
        """Check if pg_partman supports auto partitioning.

        Verifies that the installed pg_partman version supports the
        auto partitioning feature set.

        Returns:
            True if auto partitioning is supported.
        """
        return self.check_extension_feature("pg_partman", "auto_partition")

    def format_pg_partman_create_parent(
        self,
        expr: "PostgresPgPartmanCreateParentExpression",
    ) -> tuple:
        """Format pg_partman create_parent() expression.

        Builds a SELECT statement that calls pg_partman's create_parent
        function with named parameters. All core parameters (parent_table,
        control, interval, type, premake) are always included. Optional
        parameters (start_partition, primary_key, default_table,
        constraint_cols, template_table, epoch, jobmon) are included only
        when set on the expression.

        Args:
            expr: PostgresPgPartmanCreateParentExpression with parent table,
                  control column, interval, type, premake, and optional params.

        Returns:
            Tuple of (SELECT function_call SQL, params tuple).
        """
        from typing import List, Any
        from rhosocial.activerecord.backend.expression import QualifiedIdentifierExpression

        schema = expr.schema or "partman"
        function_name_sql, _ = QualifiedIdentifierExpression(
            dialect=self, schema=schema, name="create_parent"
        ).to_sql()
        placeholder = self.get_parameter_placeholder()

        arg_specs: List[str] = []
        params: List[Any] = []

        # Core required parameters (always included)
        core_params = [
            ("p_parent_table", expr.parent_table, "text"),
            ("p_control", expr.control, "text"),
            ("p_interval", expr.interval, "text"),
            ("p_type", expr.partition_type, "text"),
            ("p_premake", expr.premake, "int"),
        ]
        for name, value, cast in core_params:
            arg_specs.append(f"{name} := {placeholder}::{cast}")
            params.append(value)

        # Optional parameters (included only when not None)
        optional_params = [
            ("p_start_partition", expr.start_partition, "text"),
            ("p_primary_key", expr.primary_key, "text"),
            ("p_default_table", expr.default_table, "boolean"),
            ("p_constraint_cols", expr.constraint_cols, "text[]"),
            ("p_template_table", expr.template_table, "text"),
            ("p_epoch", expr.epoch, "text"),
            ("p_jobmon", expr.jobmon, "boolean"),
        ]
        for name, value, cast in optional_params:
            if value is not None:
                arg_specs.append(f"{name} := {placeholder}::{cast}")
                params.append(value)

        return (
            f"SELECT {function_name_sql}({', '.join(arg_specs)})",
            tuple(params),
        )

    def format_pg_partman_run_maintenance(
        self,
        expr: "PostgresPgPartmanRunMaintenanceExpression",
    ) -> tuple:
        """Format pg_partman run_maintenance() expression.

        Builds a SELECT statement that calls pg_partman's run_maintenance
        function. If a parent_table is specified, the maintenance is scoped
        to that table; otherwise it runs globally.

        Args:
            expr: PostgresPgPartmanRunMaintenanceExpression with optional
                  parent_table scope.

        Returns:
            Tuple of (SELECT function_call SQL, params tuple).
        """
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
        """Format pg_partman part_config update expression.

        Builds an UPDATE statement targeting the pg_partman part_config table.
        Only columns corresponding to non-None expr attributes are included
        in the SET clause. At least one config option must be specified.

        Args:
            expr: PostgresPgPartmanUpdateConfigExpression with config values
                  to update. Only non-None attributes are applied.

        Returns:
            Tuple of (UPDATE SQL, params tuple).

        Raises:
            ValueError: If all config options are None (nothing to update).
        """
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

    def format_pg_partman_delete_config(
        self,
        expr: "PostgresPgPartmanDeleteConfigExpression",
    ) -> tuple:
        """Format pg_partman part_config delete expression.

        Builds a DELETE statement targeting the pg_partman part_config table
        for a specific parent table, removing its automated partition config.

        Args:
            expr: PostgresPgPartmanDeleteConfigExpression with parent_table
                  identifying the config to remove.

        Returns:
            Tuple of (DELETE SQL, params tuple).
        """
        from rhosocial.activerecord.backend.expression import QualifiedIdentifierExpression
        schema = expr.schema or "partman"
        config_table_sql, _ = QualifiedIdentifierExpression(
            dialect=self, schema=schema, name="part_config"
        ).to_sql()
        placeholder = self.get_parameter_placeholder()
        return (
            f"DELETE FROM {config_table_sql} WHERE parent_table = {placeholder}",
            (expr.parent_table,),
        )
