# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/partition.py
"""PostgreSQL partitioning enhancements implementation.

This module provides the PostgresPartitionMixin class for handling
PostgreSQL table partitioning operations including RANGE, LIST, and HASH
partitioning with support for various PostgreSQL versions.
"""

from typing import Any, List, Tuple, TYPE_CHECKING  # noqa: F401

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

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

    def supports_table_partitioning(self) -> bool:
        """Declarative table partitioning is native feature, PG 10+."""
        return self.version >= (10, 0, 0)

    def supports_partitioned_table_creation(self) -> bool:
        """CREATE TABLE can create partitioned parent tables in PG 10+."""
        return self.supports_table_partitioning()

    def supports_range_table_partitioning(self) -> bool:
        """RANGE table partitioning is native feature, PG 10+."""
        return self.supports_table_partitioning()

    def supports_list_table_partitioning(self) -> bool:
        """LIST table partitioning is native feature, PG 10+."""
        return self.supports_table_partitioning()

    def supports_hash_table_partitioning(self) -> bool:
        """HASH table partitioning is native feature, PG 11+."""
        return self.supports_hash_partitioning()

    def supports_subpartitioning(self) -> bool:
        """Nested partitioned partitions are not exposed by this API yet."""
        return False

    def supports_partition_metadata_introspection(self) -> bool:
        """Partition metadata introspection is available through pg_catalog in PG 10+."""
        return self.supports_table_partitioning()

    def supports_add_partition(self) -> bool:
        """Adding a partition maps to CREATE TABLE ... PARTITION OF in PG 10+."""
        return self.supports_table_partitioning()

    def supports_drop_partition(self) -> bool:
        """Dropping a partition maps to dropping the child partition table."""
        return self.supports_table_partitioning()

    def supports_truncate_partition(self) -> bool:
        """Truncating a partition maps to TRUNCATE TABLE on the child table."""
        return self.supports_table_partitioning()

    def supports_reorganize_partition(self) -> bool:
        """PostgreSQL has no MySQL-style REORGANIZE PARTITION operation."""
        return False

    def supports_attach_partition(self) -> bool:
        """PostgreSQL supports ALTER TABLE ... ATTACH PARTITION."""
        return self.supports_table_partitioning()

    def supports_detach_partition(self) -> bool:
        """PostgreSQL supports ALTER TABLE ... DETACH PARTITION."""
        return self.supports_table_partitioning()

    def format_partition_clause(self, expr) -> Tuple[str, tuple]:
        """Format a PostgreSQL PARTITION BY clause from PartitionClause."""
        if not self.supports_partitioned_table_creation():
            raise UnsupportedFeatureError(
                self.name,
                "declarative table partitioning",
                "Declarative table partitioning requires PostgreSQL 10+.",
            )

        method = expr.method.upper()
        if method == "RANGE":
            if not self.supports_range_table_partitioning():
                raise UnsupportedFeatureError(self.name, "RANGE partitioning")
        elif method == "LIST":
            if not self.supports_list_table_partitioning():
                raise UnsupportedFeatureError(self.name, "LIST partitioning")
        elif method == "HASH":
            if not self.supports_hash_table_partitioning():
                raise UnsupportedFeatureError(
                    self.name,
                    "HASH partitioning",
                    "HASH partitioning requires PostgreSQL 11+.",
                )
        elif method in {"KEY", "RANGE COLUMNS", "LIST COLUMNS", "LINEAR HASH", "LINEAR KEY"}:
            raise UnsupportedFeatureError(
                self.name,
                f"{method} partitioning",
                "This is not a PostgreSQL partitioning method.",
            )
        else:
            raise ValueError(f"Invalid PostgreSQL partition method: {expr.method}")

        key_parts: List[str] = []
        params: List[Any] = []
        for key in expr.keys:
            key_sql, key_params = key.to_sql()
            key_parts.append(key_sql)
            params.extend(key_params)
        return f" PARTITION BY {method} ({', '.join(key_parts)})", tuple(params)

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
        - String 'MAXVALUE', 'MINVALUE', or 'DEFAULT' (case-insensitive) → as-is
        - String/date/datetime/Decimal/numeric values → whitelist-formatted SQL literal
        - Arbitrary objects are rejected by PartitionValue before formatting

        Args:
            expr: PartitionValue with the bound value.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        from datetime import date, datetime
        from decimal import Decimal
        from math import isfinite

        value = expr.value
        if value is None:
            return "NULL", ()
        if isinstance(value, bool):
            raise TypeError("partition value must not be bool")
        if isinstance(value, str):
            upper_val = value.upper()
            if upper_val in {"MAXVALUE", "MINVALUE", "DEFAULT"}:
                return upper_val, ()
            return f"'{value.replace(chr(39), chr(39)+chr(39))}'", ()
        if isinstance(value, int):
            return str(value), ()
        if isinstance(value, float):
            if not isfinite(value):
                raise ValueError("partition value float must be finite")
            return repr(value), ()
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ValueError("partition value Decimal must be finite")
            return str(value), ()
        if isinstance(value, datetime):
            return f"'{value.isoformat(sep=' ')}'", ()
        if isinstance(value, date):
            return f"'{value.isoformat()}'", ()
        raise TypeError(
            "partition value must be str, int, float, Decimal, "
            f"date, datetime, or None, got {type(value).__name__}"
        )

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

        if partition_type == "RANGE":
            if "default" in expr.partition_values and expr.partition_values["default"]:
                parts.append("DEFAULT")
            else:
                parts.append("FOR VALUES")
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
                parts.append("FOR VALUES")
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
            parts.append("FOR VALUES")
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

        if expr.concurrently and not self.supports_concurrent_detach():
            raise ValueError("DETACH CONCURRENTLY requires PostgreSQL 14+")
        parts.append("DETACH PARTITION")

        if expr.schema:
            parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.partition_name)}")
        else:
            parts.append(self.format_identifier(expr.partition_name))

        if expr.concurrently:
            parts.append("CONCURRENTLY")

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
        if partition_type not in ("RANGE", "LIST", "HASH"):
            raise ValueError(f"Invalid partition_type: {partition_type}")

        if partition_type == "RANGE":
            from_val = expr.partition_values.get("from")
            to_val = expr.partition_values.get("to")
            if from_val is None or to_val is None:
                raise ValueError("RANGE partition requires 'from' and 'to' values")
            from ...expression.ddl import PartitionValue
            from_sql, _ = PartitionValue(dialect=self, value=from_val).to_sql()
            to_sql, _ = PartitionValue(dialect=self, value=to_val).to_sql()
            parts.append(f"FROM ({from_sql}) TO ({to_sql})")

        elif partition_type == "LIST":
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

        return (" ".join(parts), ())

    def format_partition_metadata_query(self, expr: "PostgresPartitionMetadataExpression") -> Tuple[str, tuple]:
        """Format pg_catalog query for partition metadata introspection."""
        if not self.supports_partition_metadata_introspection():
            raise UnsupportedFeatureError(
                self.name,
                "partition metadata introspection",
                "Partition metadata introspection requires PostgreSQL 10+.",
            )

        if expr.include_partitions:
            sql = """
                SELECT pg_get_partkeydef(parent.oid) AS partition_key,
                       child.relname AS name,
                       pg_get_expr(child.relpartbound, child.oid) AS bound
                FROM pg_class parent
                JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                LEFT JOIN pg_inherits i ON i.inhparent = parent.oid
                LEFT JOIN pg_class child ON child.oid = i.inhrelid
                WHERE parent.relname = %s
                  AND (%s::text IS NULL OR parent_ns.nspname = %s)
                ORDER BY child.relname
            """
        else:
            sql = """
                SELECT pg_get_partkeydef(parent.oid) AS partition_key,
                       NULL::text AS name,
                       NULL::text AS bound
                FROM pg_class parent
                JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                WHERE parent.relname = %s
                  AND (%s::text IS NULL OR parent_ns.nspname = %s)
            """
        return sql, (expr.parent_table, expr.schema, expr.schema)
