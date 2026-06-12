# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_partman.py
"""pg_partman extension protocol definition.

Defines the PostgresPgPartmanSupport protocol which declares the interface
for pg_partman automated partition management operations including
create_parent, run_maintenance, and part_config management.

Reference:
    https://github.com/pgpartman/pg_partman
"""

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ...expression.ddl import (
        PostgresPgPartmanCreateParentExpression,
        PostgresPgPartmanRunMaintenanceExpression,
        PostgresPgPartmanUpdateConfigExpression,
        PostgresPgPartmanDeleteConfigExpression,
    )


@runtime_checkable
class PostgresPgPartmanSupport(Protocol):
    """pg_partman partition management extension protocol.

    Declares the interface for pg_partman operations:
    - supports_pg_partman / supports_pg_partman_auto_partition: capability checks
    - format_pg_partman_create_parent: initialize automated partitioning
    - format_pg_partman_run_maintenance: trigger maintenance
    - format_pg_partman_update_config: update partition config
    - format_pg_partman_delete_config: remove partition config
    """

    def supports_pg_partman(self) -> bool:
        """Whether pg_partman extension is installed.

        Returns:
            True if the pg_partman extension is available in the current database.
        """
        ...

    def supports_pg_partman_auto_partition(self) -> bool:
        """Whether pg_partman supports auto partitioning.

        Returns:
            True if pg_partman's auto partitioning feature is available.
        """
        ...

    def format_pg_partman_create_parent(
        self,
        expr: "PostgresPgPartmanCreateParentExpression",
    ) -> tuple:
        """Format pg_partman create_parent() expression.

        Builds a SELECT statement that calls pg_partman's create_parent
        function with named parameters. Core parameters (parent_table,
        control, interval, type, premake) are always included; optional
        parameters (start_partition, primary_key, default_table,
        constraint_cols, template_table, epoch, jobmon) are included
        only when set.

        Args:
            expr: PostgresPgPartmanCreateParentExpression with all params.

        Returns:
            Tuple of (SELECT function_call SQL, params tuple).
        """
        ...

    def format_pg_partman_run_maintenance(
        self,
        expr: "PostgresPgPartmanRunMaintenanceExpression",
    ) -> tuple:
        """Format pg_partman run_maintenance() expression.

        Args:
            expr: PostgresPgPartmanRunMaintenanceExpression with optional
                  parent_table scope.

        Returns:
            Tuple of (SELECT function_call SQL, params tuple).
        """
        ...

    def format_pg_partman_update_config(
        self,
        expr: "PostgresPgPartmanUpdateConfigExpression",
    ) -> tuple:
        """Format pg_partman part_config update expression.

        Args:
            expr: PostgresPgPartmanUpdateConfigExpression with config options
                  to update.

        Returns:
            Tuple of (UPDATE SQL, params tuple).

        Raises:
            ValueError: If no config options are specified.
        """
        ...

    def format_pg_partman_delete_config(
        self,
        expr: "PostgresPgPartmanDeleteConfigExpression",
    ) -> tuple:
        """Format pg_partman part_config delete expression.

        Args:
            expr: PostgresPgPartmanDeleteConfigExpression identifying the
                  parent table whose config should be removed.

        Returns:
            Tuple of (DELETE SQL, params tuple).
        """
        ...
