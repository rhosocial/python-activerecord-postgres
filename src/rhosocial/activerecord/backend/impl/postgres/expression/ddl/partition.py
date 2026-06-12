# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/partition.py
"""
PostgreSQL DDL expressions: Partition operations.

PostgreSQL Documentation:
- Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html
- CREATE TABLE: https://www.postgresql.org/docs/current/sql-createtable.html
- ALTER TABLE: https://www.postgresql.org/docs/current/sql-altertable.html

Version Requirements:
- Partitioning: PostgreSQL 10+
- HASH partitioning, DEFAULT partition: PostgreSQL 11+
- Partition bounds expression: PostgreSQL 12+
- ATTACH PARTITION with CONCURRENTLY: PostgreSQL 14+
"""

from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PartitionValue",
    "PostgresCreatePartitionExpression",
    "PostgresDetachPartitionExpression",
    "PostgresAttachPartitionExpression",
    "PostgresPartitionMetadataExpression",
]


class PartitionValue(BaseExpression):
    """Represents a partition bound value in PostgreSQL DDL.

    Handles the formatting of partition boundary values used in
    FOR VALUES clauses of PARTITION OF and ATTACH PARTITION statements.

    Value handling:
    - None → NULL
    - String 'MAXVALUE', 'MINVALUE', or 'DEFAULT' (case-insensitive) → as-is
    - String/date/datetime/Decimal/numeric values → whitelist-formatted SQL literal
    - Arbitrary objects are rejected to prevent unsafe ``str(value)`` fallback

    Delegates to dialect.format_partition_value() for SQL generation.

    Attributes:
        value: The partition bound value.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> PartitionValue(dialect=dialect, value=None).to_sql()
        ('NULL', ())
        >>> PartitionValue(dialect=dialect, value='MAXVALUE').to_sql()
        ('MAXVALUE', ())
        >>> PartitionValue(dialect=dialect, value='2024-01-01').to_sql()
        ("'2024-01-01'", ())
        >>> PartitionValue(dialect=dialect, value=42).to_sql()
        ('42', ())
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        value: Any,
    ):
        super().__init__(dialect)
        if isinstance(value, bool):
            raise TypeError("partition value must not be bool")
        if isinstance(value, float) and not isfinite(value):
            raise ValueError("partition value float must be finite")
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError("partition value Decimal must be finite")
        if not isinstance(value, (str, int, float, Decimal, date, datetime, type(None))):
            raise TypeError(
                "partition value must be str, int, float, Decimal, "
                f"date, datetime, or None, got {type(value).__name__}"
            )
        self.value = value

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate SQL for a partition bound value.

        Delegates to the dialect's format_partition_value() method.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_partition_value(self)


class PostgresCreatePartitionExpression(BaseExpression):
    """PostgreSQL CREATE TABLE ... PARTITION OF statement expression.

    Creates a new partition as a child of a partitioned table.
    Supports RANGE, LIST, and HASH partitioning with version-specific features.

    Attributes:
        partition_name: Name of the partition to create.
        parent_table: Name of the parent partitioned table.
        partition_type: Partition type: 'RANGE', 'LIST', or 'HASH'.
        partition_values: Partition bounds values (dict with 'from', 'to' for RANGE,
            list of values for LIST, or modulus/remainder for HASH).
        schema: Schema name for the partition.
        tablespace: Tablespace for the partition.
        if_not_exists: Add IF NOT EXISTS clause.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # RANGE partition
        >>> partition = PostgresCreatePartitionExpression(
        ...     dialect=dialect,
        ...     partition_name="orders_2024_q1",
        ...     parent_table="orders",
        ...     partition_type="RANGE",
        ...     partition_values={"from": "2024-01-01", "to": "2024-04-01"},
        ... )
        >>> sql, params = partition.to_sql()
        >>> sql
        "CREATE TABLE orders_2024_q1 PARTITION OF orders RANGE ('2024-01-01', '2024-04-01')"

        >>> # LIST partition
        >>> partition = PostgresCreatePartitionExpression(
        ...     dialect=dialect,
        ...     partition_name="orders_active",
        ...     parent_table="orders",
        ...     partition_type="LIST",
        ...     partition_values={"values": ["active", "pending"]},
        ... )

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        partition_name: str,
        parent_table: str,
        partition_type: str,
        partition_values: Dict[str, Any],
        schema: Optional[str] = None,
        tablespace: Optional[str] = None,
        if_not_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.partition_type = partition_type
        self.partition_values = partition_values
        self.schema = schema
        self.tablespace = tablespace
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate CREATE TABLE PARTITION SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_create_partition_statement(self)


class PostgresDetachPartitionExpression(BaseExpression):
    """PostgreSQL ALTER TABLE ... DETACH PARTITION statement expression.

    Detaches a partition from its parent table without dropping the data.
    The partition becomes an independent regular table.

    Attributes:
        partition_name: Name of the partition to detach.
        parent_table: Name of the parent partitioned table.
        schema: Schema name for the partition.
        concurrently: Perform detach without locks (PG 14+).
        finalize: Finalize concurrent detach (PG 14+).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> detach = PostgresDetachPartitionExpression(
        ...     dialect=dialect,
        ...     partition_name="orders_2023",
        ...     parent_table="orders",
        ...     concurrently=True,
        ... )
        >>> sql, params = detach.to_sql()
        >>> sql
        "ALTER TABLE orders DETACH PARTITION orders_2023 CONCURRENTLY"

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        partition_name: str,
        parent_table: str,
        schema: Optional[str] = None,
        concurrently: bool = False,
        finalize: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.schema = schema
        self.concurrently = concurrently
        self.finalize = finalize
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate DETACH PARTITION SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_detach_partition_statement(self)


class PostgresAttachPartitionExpression(BaseExpression):
    """PostgreSQL ALTER TABLE ... ATTACH PARTITION statement expression.

    Attaches an existing table as a partition of a partitioned table.
    Supports CONCURRENTLY mode on PostgreSQL 14+.

    Attributes:
        partition_name: Name of the table to attach.
        parent_table: Name of the parent partitioned table.
        partition_type: Partition type: 'RANGE', 'LIST', or 'HASH'.
        partition_values: Partition bounds values.
        schema: Schema name for the partition.
        concurrently: Use non-blocking ATTACH mode (PG 14+).

    Raises:
        ValueError: If concurrent_attach is used on PostgreSQL < 14.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> attach = PostgresAttachPartitionExpression(
        ...     dialect=dialect,
        ...     partition_name="orders_2024_q1",
        ...     parent_table="orders",
        ...     partition_type="RANGE",
        ...     partition_values={"from": "2024-01-01", "to": "2024-04-01"},
        ... )
        >>> sql, params = attach.to_sql()
        >>> sql
        "ALTER TABLE orders ATTACH PARTITION orders_2024_q1 FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')"

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        partition_name: str,
        parent_table: str,
        partition_type: str,
        partition_values: Dict[str, Any],
        schema: Optional[str] = None,
        concurrently: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.partition_type = partition_type
        self.partition_values = partition_values
        self.schema = schema
        self.concurrently = concurrently
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate ATTACH PARTITION SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            ValueError: If concurrently is used on PostgreSQL < 14.

        """
        return self.dialect.format_attach_partition_statement(self)


class PostgresPartitionMetadataExpression(BaseExpression):
    """PostgreSQL partition metadata query expression.

    This expression builds the public metadata query used by tests and provider
    code to inspect a partitioned parent table through PostgreSQL catalogs. It
    keeps catalog SQL construction behind the Expression-Dialect protocol
    boundary instead of embedding raw SQL in tests.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        parent_table: str,
        schema: Optional[str] = None,
        *,
        include_partitions: bool = True,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        if not parent_table:
            raise ValueError("parent_table must not be empty")
        self.parent_table = parent_table
        self.schema = schema
        self.include_partitions = include_partitions
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate PostgreSQL partition metadata query SQL."""
        return self.dialect.format_partition_metadata_query(self)