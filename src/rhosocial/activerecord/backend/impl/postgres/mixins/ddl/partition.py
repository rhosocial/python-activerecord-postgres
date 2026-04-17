# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/partition.py
"""PostgreSQL partitioning enhancements implementation.

This module provides the PostgresPartitionMixin class for handling
PostgreSQL table partitioning operations including RANGE, LIST, and HASH
partitioning with support for various PostgreSQL versions.
"""

from typing import Any, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import (
        CreatePartitionExpression,
        DetachPartitionExpression,
        AttachPartitionExpression,
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

    def format_create_partition_statement(self, expr: "CreatePartitionExpression") -> Tuple[str, tuple]:
        """Format CREATE TABLE ... PARTITION OF statement from expression."""
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
                parts.append(
                    f"FROM ({self._format_partition_value(from_val)}) TO ({self._format_partition_value(to_val)})"
                )

        elif partition_type == "LIST":
            if "default" in expr.partition_values and expr.partition_values["default"]:
                parts.append("DEFAULT")
            else:
                values = expr.partition_values.get("values", [])
                if not values:
                    raise ValueError("LIST partition requires 'values' list")
                vals_str = ", ".join(self._format_partition_value(v) for v in values)
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

    def _format_partition_value(self, value: Any) -> str:
        """Format a partition bound value.

        Args:
            value: The partition bound value. Can be:
                - None: Returns 'NULL'
                - String 'MAXVALUE' or 'MINVALUE' (case-insensitive): Returns as-is
                - String: Returns the string value quoted
                - Other types: Returns str(value)

        Note:
            For string values that represent dates or other literals,
            pass the raw value without quotes. The method will add quotes.
            Example: Pass '2024-01-01' not "'2024-01-01'"
        """
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            upper_val = value.upper()
            if upper_val == "MAXVALUE" or upper_val == "MINVALUE":
                return upper_val
            # Add quotes around string values
            return f"'{value}'"
        else:
            return str(value)

    def format_detach_partition_statement(self, expr: "DetachPartitionExpression") -> Tuple[str, tuple]:
        """Format ALTER TABLE ... DETACH PARTITION statement from expression."""
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

    def format_attach_partition_statement(self, expr: "AttachPartitionExpression") -> Tuple[str, tuple]:
        """Format ALTER TABLE ... ATTACH PARTITION statement from expression."""
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
            parts.append(f"FROM ({self._format_partition_value(from_val)}) TO ({self._format_partition_value(to_val)})")

        elif partition_type == "LIST":
            values = expr.partition_values.get("values", [])
            vals_str = ", ".join(self._format_partition_value(v) for v in values)
            parts.append(f"IN ({vals_str})")

        elif partition_type == "HASH":
            modulus = expr.partition_values.get("modulus")
            remainder = expr.partition_values.get("remainder")
            parts.append(f"WITH (MODULUS {modulus}, REMAINDER {remainder})")

        return (" ".join(parts), ())
