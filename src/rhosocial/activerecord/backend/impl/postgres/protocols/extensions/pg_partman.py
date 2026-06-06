# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_partman.py
"""pg_partman extension protocol definition."""

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
    """pg_partman partition management extension protocol."""

    def supports_pg_partman(self) -> bool:
        """Whether pg_partman extension is available."""
        ...

    def supports_pg_partman_auto_partition(self) -> bool:
        """Whether pg_partman supports auto partitioning."""
        ...

    def format_pg_partman_create_parent(
        self,
        expr: "PostgresPgPartmanCreateParentExpression",
    ) -> tuple:
        """Format pg_partman create_parent() expression."""
        ...

    def format_pg_partman_run_maintenance(
        self,
        expr: "PostgresPgPartmanRunMaintenanceExpression",
    ) -> tuple:
        """Format pg_partman run_maintenance() expression."""
        ...

    def format_pg_partman_update_config(
        self,
        expr: "PostgresPgPartmanUpdateConfigExpression",
    ) -> tuple:
        """Format pg_partman part_config update expression."""
        ...

    def format_pg_partman_delete_config(
        self,
        expr: "PostgresPgPartmanDeleteConfigExpression",
    ) -> tuple:
        """Format pg_partman part_config delete expression."""
        ...
