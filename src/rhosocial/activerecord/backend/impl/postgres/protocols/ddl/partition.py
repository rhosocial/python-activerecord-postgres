# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/partition.py
"""PostgreSQL partitioning enhancements protocol definitions.

This module contains the PostgresPartitionSupport protocol which defines
the interface for PostgreSQL's native partitioning features.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expressions import (
        CreatePartitionExpression,
        DetachPartitionExpression,
    )


@runtime_checkable
class PostgresPartitionSupport(Protocol):
    """PostgreSQL partitioning enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL provides advanced partitioning features beyond the SQL standard:
    - HASH partitioning (PG 11+)
    - DEFAULT partition (PG 11+)
    - Partition key update row movement (PG 11+)
    - Concurrent DETACH (PG 14+)
    - Partition bounds expression (PG 12+)
    - Partitionwise join/aggregate (PG 11+)

    Official Documentation:
    - Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html
    - HASH Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-HASH
    - Concurrent DETACH: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION-CONCURRENTLY

    Version Requirements:
    - HASH partitioning: PostgreSQL 11+
    - DEFAULT partition: PostgreSQL 11+
    - Row movement: PostgreSQL 11+
    - Concurrent DETACH: PostgreSQL 14+
    - Partition bounds expression: PostgreSQL 12+
    - Partitionwise join: PostgreSQL 11+
    - Partitionwise aggregate: PostgreSQL 11+
    """

    def supports_hash_partitioning(self) -> bool:
        """Whether HASH partitioning is supported.

        Native feature, PostgreSQL 11+.
        Enables HASH partitioning method for tables.
        """
        ...

    def supports_default_partition(self) -> bool:
        """Whether DEFAULT partition is supported.

        Native feature, PostgreSQL 11+.
        Enables DEFAULT partition to catch non-matching rows.
        """
        ...

    def supports_partition_key_update(self) -> bool:
        """Whether partition key update automatically moves rows.

        Native feature, PostgreSQL 11+.
        When updating partition key, rows automatically move to correct partition.
        """
        ...

    def supports_concurrent_detach(self) -> bool:
        """Whether CONCURRENTLY DETACH PARTITION is supported.

        Native feature, PostgreSQL 14+.
        Enables non-blocking partition detachment.
        """
        ...

    def supports_partition_bounds_expression(self) -> bool:
        """Whether partition bounds can use expressions.

        Native feature, PostgreSQL 12+.
        Allows non-constant expressions in partition bounds.
        """
        ...

    def supports_partitionwise_join(self) -> bool:
        """Whether partitionwise join optimization is supported.

        Native feature, PostgreSQL 11+.
        Enables join optimization for partitioned tables.
        """
        ...

    def supports_partitionwise_aggregate(self) -> bool:
        """Whether partitionwise aggregate optimization is supported.

        Native feature, PostgreSQL 11+.
        Enables aggregate optimization for partitioned tables.
        """
        ...

    def format_create_partition_statement(self, expr: "CreatePartitionExpression") -> Tuple[str, tuple]:
        """Format CREATE TABLE ... PARTITION OF statement from expression.

        Args:
            expr: CreatePartitionExpression with partition details

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...

    def format_detach_partition_statement(self, expr: "DetachPartitionExpression") -> Tuple[str, tuple]:
        """Format ALTER TABLE ... DETACH PARTITION statement from expression.

        Args:
            expr: DetachPartitionExpression with detach details

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
