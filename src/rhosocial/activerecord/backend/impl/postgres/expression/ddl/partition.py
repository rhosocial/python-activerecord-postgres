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
- Concurrent ATTACH: unsupported by this implementation
"""

from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from typing import Any, Dict, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import FunctionCall, Literal
from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
from rhosocial.activerecord.backend.expression.statements import PartitionClause

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PartitionValue",
    "PostgresPartitionClause",
    "PostgresCreatePartitionExpression",
    "PostgresDetachPartitionExpression",
    "PostgresAttachPartitionExpression",
    "PostgresPartitionMetadataExpression",
]


class PostgresPartitionClause(PartitionClause):
    """PostgreSQL ``PARTITION BY {RANGE|LIST|HASH} (...)`` clause.

    PostgreSQL's declarative partitioning shares the generic clause shape, so
    this subclass currently adds no fields; it exists as the PostgreSQL-owned
    clause type (for capability gating and future PG-only partition parameters)
    and to distinguish PG-declared candidates during backend selection.

    Concrete partitions are **not** declared inline: PostgreSQL creates them
    through the separate ``CREATE TABLE ... PARTITION OF`` statement
    (``PostgresCreatePartitionExpression``). This class therefore remains
    clause-only and never carries ``PartitionDefinition`` values.
    """


class PartitionValue(BaseExpression):
    """Represents a partition bound value in PostgreSQL DDL.

    Handles the formatting of partition boundary values used in
    FOR VALUES clauses of PARTITION OF and ATTACH PARTITION statements.

    Value handling:
    - ``None`` renders as ``NULL``.
    - RANGE ``MINVALUE`` and ``MAXVALUE`` render as keywords.
    - LIST renders those strings as quoted values.
    - Same-dialect ``FunctionCall``, ``Literal``, and ``RawSQLExpression`` values
      delegate through their own formatters and retain their parameters.
    - Other scalar values use a whitelist; arbitrary objects are rejected.

    Delegates to dialect.format_partition_value() for SQL generation.

    Attributes:
        value: The partition bound value or expression.
        partition_type: Optional RANGE, LIST, or HASH context.

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
        partition_type: Optional[str] = None,
    ):
        super().__init__(dialect)
        if partition_type is None:
            normalized_type = None
        elif not isinstance(partition_type, str):
            raise TypeError("partition_type must be a string")
        else:
            normalized_type = partition_type.upper()
            if normalized_type not in {"RANGE", "LIST", "HASH"}:
                raise ValueError("partition_type must be RANGE, LIST, or HASH")

        if isinstance(value, BaseExpression):
            if normalized_type == "HASH":
                raise TypeError("HASH partition bounds require modulus and remainder")
            pending = [value]
            seen = set()
            while pending:
                current = pending.pop()
                identity = id(current)
                if identity in seen:
                    continue
                seen.add(identity)
                if current.dialect is not dialect:
                    raise ValueError(
                        "partition value expressions must use the same dialect as PartitionValue"
                    )
                if isinstance(current, FunctionCall):
                    if current.alias is not None:
                        raise ValueError("partition value expressions must not have aliases")
                    pending.extend(current.args)
                elif isinstance(current, (Literal, RawSQLExpression)):
                    if getattr(current, "alias", None) is not None:
                        raise ValueError("partition value expressions must not have aliases")
                else:
                    raise TypeError(
                        "partition value expressions must be FunctionCall, Literal, "
                        "or RawSQLExpression"
                    )
        else:
            if isinstance(value, bool):
                raise TypeError("partition value must not be bool")
            if isinstance(value, float) and not isfinite(value):
                raise ValueError("partition value float must be finite")
            if isinstance(value, Decimal) and not value.is_finite():
                raise ValueError("partition value Decimal must be finite")
            if not isinstance(value, (str, int, float, Decimal, date, datetime, type(None))):
                raise TypeError(
                    "partition value must be a supported expression, str, int, float, "
                    f"Decimal, date, datetime, or None, got {type(value).__name__}"
                )
        self.value = value
        self.partition_type = normalized_type

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_partition_value"


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
        parent_schema: Optional schema for the parent table.
        partition_clause: Optional child-level PARTITION BY clause.
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
        parent_schema: Optional[str] = None,
        partition_clause: Optional[PartitionClause] = None,
    ):
        super().__init__(dialect)
        if partition_clause is not None:
            if not isinstance(partition_clause, PartitionClause):
                raise TypeError("partition_clause must be a PartitionClause")
            if partition_clause.dialect is not dialect:
                raise ValueError("partition_clause must use the same dialect")
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.partition_type = partition_type
        self.partition_values = partition_values
        self.schema = schema
        self.parent_schema = parent_schema
        self.partition_clause = partition_clause
        self.tablespace = tablespace
        self.if_not_exists = if_not_exists

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_create_partition_statement"


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
        parent_schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.schema = schema
        self.concurrently = concurrently
        self.finalize = finalize
        self.parent_schema = parent_schema

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_detach_partition_statement"


class PostgresAttachPartitionExpression(BaseExpression):
    """PostgreSQL ALTER TABLE ... ATTACH PARTITION statement expression.

    Attaches an existing table as a partition of a partitioned table.
    Concurrent attachment is rejected for every PostgreSQL version.

    Attributes:
        partition_name: Name of the table to attach.
        parent_table: Name of the parent partitioned table.
        partition_type: Partition type: 'RANGE', 'LIST', or 'HASH'.
        partition_values: Partition bounds values.
        schema: Schema name for the partition.
        concurrently: Requests unsupported concurrent attachment.

    Raises:
        UnsupportedFeatureError: If ``concurrently=True``.

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
        parent_schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.partition_type = partition_type
        self.partition_values = partition_values
        self.schema = schema
        self.concurrently = concurrently
        self.parent_schema = parent_schema

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_attach_partition_statement"


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
    ):
        super().__init__(dialect)
        if not parent_table:
            raise ValueError("parent_table must not be empty")
        self.parent_table = parent_table
        self.schema = schema
        self.include_partitions = include_partitions

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_partition_metadata_query"