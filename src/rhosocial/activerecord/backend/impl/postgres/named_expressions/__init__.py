# src/rhosocial/activerecord/backend/impl/postgres/named_expressions/__init__.py
"""PostgreSQL named expressions for partition auto-management.

This package provides named expression functions for PostgreSQL partition
management. Each function follows the ``named_expression`` protocol:

1. First argument is ``dialect`` (the PostgreSQL dialect instance).
2. Returns a ``BaseExpression`` (or a ``ProcedureGraph`` for multi-step workflows).
3. Is discoverable via ``list_named_expressions_in_module()``.

Usage via CLI::

    # Create next month's partition
    rhosocial-activerecord named-expr \\
        rhosocial.activerecord.backend.impl.postgres.named_expressions.partition.create_next_monthly_partition \\
        --param parent_table=orders

    # List all available partition helpers
    rhosocial-activerecord named-expr \\
        rhosocial.activerecord.backend.impl.postgres.named_expressions.partition --list

Reference:
    - Named Expression docs: https://rhosocial.ai/activerecord/named-expressions
    - PostgreSQL Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html
"""

from .partition import (
    create_next_monthly_partition,
    create_range_partition_for_month,
    create_range_partition_for_quarter,
    create_range_partitions_for_interval,
)

__all__ = [
    "create_next_monthly_partition",
    "create_range_partition_for_month",
    "create_range_partition_for_quarter",
    "create_range_partitions_for_interval",
]
