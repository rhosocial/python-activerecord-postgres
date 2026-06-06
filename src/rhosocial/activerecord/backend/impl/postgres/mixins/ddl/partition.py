# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/partition.py
"""PostgreSQL partitioning enhancements implementation.

This module provides the PostgresPartitionMixin class for handling
PostgreSQL table partitioning operations including RANGE, LIST, and HASH
partitioning with support for various PostgreSQL versions.
"""

from typing import Any, Tuple, TYPE_CHECKING  # noqa: F401

if TYPE_CHECKING:
    from ...expression.ddl import (
        PartitionValue,
        PostgresCreatePartitionExpression,
        PostgresDetachPartitionExpression,
        PostgresAttachPartitionExpression,
    )


class PostgresPartitionMixin:
    """PostgreSQL partitioning enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_hash_partitioning(self) -> bool:
        """HASH partitioning is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_default_partition(self) -> bool:
        """DEFAULT partition is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_partition_key_update(self) -> bool:
        """Partition key row movement is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_concurrent_detach(self) -> bool:
        """Concurrent DETACH is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_partition_bounds_expression(self) -> bool:
        """Partition bounds expression is native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_partitionwise_join(self) -> bool:
        """Partitionwise join is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_partitionwise_aggregate(self) -> bool:
        """Partitionwise aggregate is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def format_partition_value(self, expr: "PartitionValue") -> Tuple[str, tuple]:
        """Format a partition bound value from expression.

        Public method implementing the PartitionValue expression's
        SQL generation, following the Expression-Dialect-Protocol pattern.

        Value handling:
        - None → 'NULL'
        - String 'MAXVALUE' or 'MINVALUE' (case-insensitive) → as-is
        - String values → quoted with internal single-quote escaping
        - Other types → str() representation

        Args:
            expr: PartitionValue with the bound value.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        value = expr.value
        if value is None:
            return "NULL", ()
        elif isinstance(value, str):
            upper_val = value.upper()
            if upper_val == "MAXVALUE" or upper_val == "MINVALUE":
                return upper_val, ()
            # Add quotes around string values, escaping internal single quotes
            return f"'{value.replace(chr(39), chr(39)+chr(39))}'", ()
        else:
            return str(value), ()

    def format_create_partition_statement(self, expr: "PostgresCreatePartitionExpression") -> Tuple[str, tuple]:
        """Format CREATE TABLE ... PARTITION OF statement from expression.

        Supported partition types are ``RANGE``, ``LIST``, and ``HASH``.

        - ``expr.partition_type`` — partition method (``RANGE`` / ``LIST`` / ``HASH``).
        - ``expr.if_not_exists`` — add ``IF NOT EXISTS``.
        - ``expr.schema`` — optional schema qualifier.
        - ``expr.partition_name`` — new partition name.
        - ``expr.parent_table`` — parent partitioned table.
        - ``expr.partition_values`` — bound values (see below).
        - ``expr.tablespace`` — optional tablespace.

        ``partition_values`` dict format:

        - RANGE: ``{"from": ..., "to": ...}`` or ``{"default": True}``.
        - LIST: ``{"values": [..., ...]}`` or ``{"default": True}``.
        - HASH: ``{"modulus": N, "remainder": M}``.

        Args:
            expr: PostgresCreatePartitionExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        Raises:
            ValueError: If partition_type is invalid or required bound values are missing.

        """
        partition_type = expr.partition_type.upper()
        if partition_type not in ("RANGE", "LIST", "HASH"):
            raise ValueError(f"Invalid partition_type: {partition_type}")

        parts = ["CREATE TABLE"]
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")

        # Partition name with optional schema
        if expr.schema:
            parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.partition_name)}")
        else:
            parts.append(self.format_identifier(expr.partition_name))

        # PARTITION OF parent
        if expr.schema:
            parts.append(
                f"PARTITION OF {self.format_identifier(expr.schema)}.{self.format_identifier(expr.parent_table)}"
            )
        else:
            parts.append(f"PARTITION OF {self.format_identifier(expr.parent_table)}")

        # FOR VALUES clause
        parts.append("FOR VALUES")

        if partition_type == "RANGE":
            if "default" in expr.partition_values and expr.partition_values["default"]:
                parts.append("DEFAULT")
            else:
                from_val = expr.partition_values.get("from")
                to_val = expr.partition_values.get("to")
                if from_val is None or to_val is None:
                    raise ValueError("RANGE partition requires 'from' and 'to' values")
                from ...expression.ddl import PartitionValue
                from_sql, _ = PartitionValue(dialect=self, value=from_val).to_sql()
                to_sql, _ = PartitionValue(dialect=self, value=to_val).to_sql()
                parts.append(f"FROM ({from_sql}) TO ({to_sql})")

        elif partition_type == "LIST":
            if "default" in expr.partition_values and expr.partition_values["default"]:
                parts.append("DEFAULT")
            else:
                values = expr.partition_values.get("values", [])
                if not values:
                    raise ValueError("LIST partition requires 'values' list")
                from ...expression.ddl import PartitionValue
                vals_str = ", ".join(
                    PartitionValue(dialect=self, value=v).to_sql()[0]
                    for v in values
                )
                parts.append(f"IN ({vals_str})")

        elif partition_type == "HASH":
            modulus = expr.partition_values.get("modulus")
            remainder = expr.partition_values.get("remainder")
            if modulus is None or remainder is None:
                raise ValueError("HASH partition requires 'modulus' and 'remainder'")
            if not self.supports_hash_partitioning():
                raise ValueError("HASH partitioning requires PostgreSQL 11+")
            parts.append(f"WITH (MODULUS {modulus}, REMAINDER {remainder})")

        # TABLESPACE
        if expr.tablespace:
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")

        return (" ".join(parts), ())

    def format_detach_partition_statement(self, expr: "PostgresDetachPartitionExpression") -> Tuple[str, tuple]:
        """Format ALTER TABLE ... DETACH PARTITION statement from expression.

        - ``expr.parent_table`` — partitioned table name.
        - ``expr.schema`` — optional schema qualifier.
        - ``expr.partition_name`` — partition to detach.
        - ``expr.concurrently`` — add ``DETACH CONCURRENTLY`` (PG 14+).
        - ``expr.finalize`` — add ``FINALIZE`` (valid only with CONCURRENTLY).

        Args:
            expr: PostgresDetachPartitionExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        parts = ["ALTER TABLE"]

        if expr.schema:
            parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.parent_table)}")
        else:
            parts.append(self.format_identifier(expr.parent_table))

        if expr.concurrently:
            if not self.supports_concurrent_detach():
                raise ValueError("DETACH CONCURRENTLY requires PostgreSQL 14+")
            parts.append("DETACH CONCURRENTLY")
        else:
            parts.append("DETACH PARTITION")

        if expr.schema:
            parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.partition_name)}")
        else:
            parts.append(self.format_identifier(expr.partition_name))

        if expr.finalize:
            if not expr.concurrently:
                raise ValueError("FINALIZE only valid with CONCURRENTLY")
            parts.append("FINALIZE")

        return (" ".join(parts), ())

    def format_attach_partition_statement(self, expr: "PostgresAttachPartitionExpression") -> Tuple[str, tuple]:
        """Format ALTER TABLE ... ATTACH PARTITION statement from expression.

        - ``expr.parent_table`` — partitioned table name.
        - ``expr.schema`` — optional schema qualifier.
        - ``expr.partition_name`` — partition to attach.
        - ``expr.partition_type`` — partition method (``RANGE`` / ``LIST`` / ``HASH``).
        - ``expr.partition_values`` — bound values (see ``format_create_partition_statement``).

        Args:
            expr: PostgresAttachPartitionExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        parts = ["ALTER TABLE"]

        if expr.schema:
            parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.parent_table)}")
        else:
            parts.append(self.format_identifier(expr.parent_table))

        parts.append("ATTACH PARTITION")

        if expr.schema:
            parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.partition_name)}")
        else:
            parts.append(self.format_identifier(expr.partition_name))

        # FOR VALUES clause (same as create partition)
        parts.append("FOR VALUES")

        partition_type = expr.partition_type.upper()
        if partition_type == "RANGE":
            from_val = expr.partition_values.get("from")
            to_val = expr.partition_values.get("to")
            from ...expression.ddl import PartitionValue
            from_sql, _ = PartitionValue(dialect=self, value=from_val).to_sql()
            to_sql, _ = PartitionValue(dialect=self, value=to_val).to_sql()
            parts.append(f"FROM ({from_sql}) TO ({to_sql})")

        elif partition_type == "LIST":
            values = expr.partition_values.get("values", [])
            from ...expression.ddl import PartitionValue
            vals_str = ", ".join(
                PartitionValue(dialect=self, value=v).to_sql()[0]
                for v in values
            )
            parts.append(f"IN ({vals_str})")

        elif partition_type == "HASH":
            modulus = expr.partition_values.get("modulus")
            remainder = expr.partition_values.get("remainder")
            parts.append(f"WITH (MODULUS {modulus}, REMAINDER {remainder})")

        return (" ".join(parts), ())
