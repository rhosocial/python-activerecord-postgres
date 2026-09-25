# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/partition.py
"""PostgreSQL partitioning enhancements implementation.

This module provides the PostgresPartitionMixin class for handling
PostgreSQL table partitioning operations including RANGE, LIST, and HASH
partitioning with support for various PostgreSQL versions.
"""

from copy import copy
from typing import Any, List, Optional, Tuple, TYPE_CHECKING  # noqa: F401

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.statements import PartitionClause
from rhosocial.activerecord.ddl.partition import (
    AttachPartitionRequest,
    CreatePartitionRequest,
    DetachPartitionRequest,
    DropPartitionRequest,
    PartitionCapabilities,
    PartitionLifecycleContractError,
    PartitionOperation,
    PartitionOperationNotSupportedError,
    PartitionRequest,
    TruncatePartitionRequest,
)

if TYPE_CHECKING:
    from ...expression.ddl import (
        PartitionValue,
        PostgresCreatePartitionExpression,
        PostgresDetachPartitionExpression,
        PostgresAttachPartitionExpression,
        PostgresPartitionMetadataExpression,
    )


class PostgresPartitionMixin:
    """PostgreSQL partitioning enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_table_partitioning(self) -> bool:
        """Check if declarative table partitioning is supported.

        PostgreSQL 10+ supports declarative partitioning natively.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.version >= (10, 0, 0)

    def supports_partitioned_table_creation(self) -> bool:
        """Check if CREATE TABLE can create partitioned parent tables.

        Alias for supports_table_partitioning(), PG 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_range_table_partitioning(self) -> bool:
        """Check if RANGE table partitioning is supported.

        Native feature, PostgreSQL 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_list_table_partitioning(self) -> bool:
        """Check if LIST table partitioning is supported.

        Native feature, PostgreSQL 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_hash_table_partitioning(self) -> bool:
        """Check if HASH table partitioning is supported.

        Native feature, PostgreSQL 11+.

        Returns:
            True if PostgreSQL version >= 11.
        """
        return self.supports_hash_partitioning()

    def supports_subpartitioning(self) -> bool:
        """Check if nested partitioning is supported.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_partition_metadata_introspection(self) -> bool:
        """Check if partition metadata introspection is available.

        Uses pg_catalog views to introspect partition structure, PG 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_add_partition(self) -> bool:
        """Check if adding a partition is supported.

        Maps to CREATE TABLE ... PARTITION OF, PG 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_drop_partition(self) -> bool:
        """Check if dropping a partition is supported.

        Maps to DROP TABLE on the child partition table, PG 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_truncate_partition(self) -> bool:
        """Check if truncating a partition is supported.

        Maps to TRUNCATE TABLE on the child partition table, PG 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_reorganize_partition(self) -> bool:
        """Check if REORGANIZE PARTITION is supported.

        PostgreSQL does not support MySQL-style REORGANIZE PARTITION operation.

        Returns:
            Always False.
        """
        return False

    def supports_attach_partition(self) -> bool:
        """Check if ATTACH PARTITION is supported.

        PostgreSQL supports ALTER TABLE ... ATTACH PARTITION, PG 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def supports_detach_partition(self) -> bool:
        """Check if DETACH PARTITION is supported.

        PostgreSQL supports ALTER TABLE ... DETACH PARTITION, PG 10+.

        Returns:
            True if PostgreSQL version >= 10.
        """
        return self.supports_table_partitioning()

    def get_partition_lifecycle_provider(self) -> "PostgresPartitionLifecycleProvider":
        return PostgresPartitionLifecycleProvider(self)

    def format_partition_clause(self, expr) -> Tuple[str, tuple]:
        """Format a PostgreSQL PARTITION BY clause from PartitionClause."""
        if not self.supports_partitioned_table_creation():
            raise UnsupportedFeatureError(
                self.name,
                "declarative table partitioning",
                "Declarative table partitioning requires PostgreSQL 10+.",
            )

        if expr.dialect is not self:
            raise ValueError("partition clause must use the PostgreSQL dialect")
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
        if method == "LIST" and len(expr.keys) != 1:
            raise ValueError(
                "PostgreSQL LIST partitioning accepts exactly one key expression"
            )

        key_parts: List[str] = []
        params: List[Any] = []
        for key in expr.keys:
            if key.dialect is not self:
                raise ValueError("partition key expressions must use the PostgreSQL dialect")
            bound_key = self._inline_partition_expression(key)
            key_sql, key_params = bound_key.to_sql()
            if key_params:
                raise ValueError("partition key expressions must not contain bind parameters")
            key_parts.append(key_sql)
        return f" PARTITION BY {method} ({', '.join(key_parts)})", tuple(params)

    def supports_hash_partitioning(self) -> bool:
        """Check if HASH partitioning is supported.

        Native feature, PostgreSQL 11+. Enables PARTITION BY HASH clause.

        Returns:
            True if PostgreSQL version >= 11.
        """
        return self.version >= (11, 0, 0)

    def supports_default_partition(self) -> bool:
        """Check if DEFAULT partition is supported.

        Native feature, PostgreSQL 11+. Allows a DEFAULT partition to catch
        rows that do not match any other partition.

        Returns:
            True if PostgreSQL version >= 11.
        """
        return self.version >= (11, 0, 0)

    def supports_partition_key_update(self) -> bool:
        """Check if partition key update row movement is supported.

        Native feature, PostgreSQL 11+. When a partition key is updated,
        rows automatically move to the correct partition.

        Returns:
            True if PostgreSQL version >= 11.
        """
        return self.version >= (11, 0, 0)

    def supports_concurrent_detach(self) -> bool:
        """Check if CONCURRENTLY DETACH is supported.

        Native feature, PostgreSQL 14+. Enables non-blocking partition
        detachment without exclusive locks.

        Returns:
            True if PostgreSQL version >= 14.
        """
        return self.version >= (14, 0, 0)

    def supports_concurrent_attach(self) -> bool:
        """Check if CONCURRENTLY ATTACH is exposed by this implementation.

        Returns:
            Always False.
        """
        return False

    def supports_partition_bounds_expression(self) -> bool:
        """Check if partition bounds expressions are supported.

        Native feature, PostgreSQL 12+. Enables non-constant expressions
        in partition bound specifications.

        Returns:
            True if PostgreSQL version >= 12.
        """
        return self.version >= (12, 0, 0)

    def supports_partitionwise_join(self) -> bool:
        """Check if partitionwise join optimization is supported.

        Native feature, PostgreSQL 11+. Enables join optimization for
        partitioned tables.

        Returns:
            True if PostgreSQL version >= 11.
        """
        return self.version >= (11, 0, 0)

    def supports_partitionwise_aggregate(self) -> bool:
        """Check if partitionwise aggregate optimization is supported.

        Native feature, PostgreSQL 11+. Enables aggregate optimization for
        partitioned tables.

        Returns:
            True if PostgreSQL version >= 11.
        """
        return self.version >= (11, 0, 0)

    def _inline_partition_expression(
        self,
        value: Any,
        memo: Optional[dict] = None,
    ) -> Any:
        if memo is None:
            memo = {}
        identity = id(value)
        if identity in memo:
            return memo[identity]
        if isinstance(value, Literal):
            bound = copy(value)
            bound.dialect = self
            bound.inline_literals = True
            memo[identity] = bound
            return bound
        if isinstance(value, BaseExpression):
            bound = copy(value)
            bound.dialect = self
            memo[identity] = bound
            for name, child in vars(value).items():
                if name == "_dialect":
                    continue
                setattr(bound, name, self._inline_partition_expression(child, memo))
            return bound
        if isinstance(value, list):
            bound_list = [self._inline_partition_expression(child, memo) for child in value]
            memo[identity] = bound_list
            return bound_list
        if isinstance(value, tuple):
            bound_tuple = tuple(self._inline_partition_expression(child, memo) for child in value)
            memo[identity] = bound_tuple
            return bound_tuple
        if isinstance(value, dict):
            bound_dict = {
                key: self._inline_partition_expression(child, memo)
                for key, child in value.items()
            }
            memo[identity] = bound_dict
            return bound_dict
        return value

    def format_partition_value(self, expr: "PartitionValue") -> Tuple[str, tuple]:
        """Format a PostgreSQL partition bound value."""
        if not self.supports_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "partition boundary value",
                "Declarative table partitioning requires PostgreSQL 10+.",
            )

        from datetime import date, datetime
        from decimal import Decimal
        from math import isfinite

        value = expr.value
        if isinstance(value, BaseExpression):
            if not self.supports_partition_bounds_expression():
                raise UnsupportedFeatureError(
                    self.name,
                    "partition bound expressions",
                    "Partition bound expressions require PostgreSQL 12+.",
                )
            bound_value = self._inline_partition_expression(value)
            value_sql, value_params = bound_value.to_sql()
            if value_params:
                raise ValueError("partition bound expressions must not contain bind parameters")
            return value_sql, ()
        if value is None:
            return "NULL", ()
        if isinstance(value, bool):
            raise TypeError("partition value must not be bool")
        if isinstance(value, str):
            upper_value = value.upper()
            if expr.partition_type in {None, "RANGE"} and upper_value in {
                "MINVALUE",
                "MAXVALUE",
            }:
                return upper_value, ()
            if expr.partition_type is None and upper_value == "DEFAULT":
                if not self.supports_default_partition():
                    raise UnsupportedFeatureError(
                        self.name,
                        "DEFAULT partition boundary",
                        "DEFAULT partitions require PostgreSQL 11+.",
                    )
                return "DEFAULT", ()
            escaped = value.replace("'", "''")
            return f"'{escaped}'", ()
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
            "partition value must be a supported expression, str, int, float, "
            f"Decimal, date, datetime, or None, got {type(value).__name__}"
        )

    def _format_list_partition_values(self, values: Any) -> Tuple[List[str], List[Any]]:
        if not isinstance(values, (list, tuple)) or not values:
            raise ValueError("LIST partition requires a non-empty 'values' list")
        if any(isinstance(value, (list, tuple)) for value in values):
            raise ValueError(
                "PostgreSQL LIST partitioning accepts exactly one bound expression per value; "
                "nested rows are not supported"
            )

        from ...expression.ddl import PartitionValue

        value_sql_parts = []
        params: List[Any] = []
        for value in values:
            value_sql, value_params = PartitionValue(
                self,
                value,
                partition_type="LIST",
            ).to_sql()
            value_sql_parts.append(value_sql)
            params.extend(value_params)
        return value_sql_parts, params

    def format_create_partition_statement(
        self,
        expr: "PostgresCreatePartitionExpression",
    ) -> Tuple[str, tuple]:
        """Format CREATE TABLE ... PARTITION OF for PostgreSQL."""
        if not self.supports_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE TABLE ... PARTITION OF",
                "Declarative table partitioning requires PostgreSQL 10+.",
            )
        if not isinstance(expr.partition_type, str):
            raise TypeError("partition_type must be a string")
        partition_type = expr.partition_type.upper()
        if partition_type not in {"RANGE", "LIST", "HASH"}:
            raise ValueError(f"Invalid partition_type: {partition_type}")

        default_marker = expr.partition_values.get("default", False)
        if not isinstance(default_marker, bool):
            raise TypeError("partition_values['default'] must be a bool")
        if default_marker:
            if partition_type == "HASH":
                raise ValueError("HASH partitions cannot use DEFAULT")
            if not self.supports_default_partition():
                raise UnsupportedFeatureError(
                    self.name,
                    "DEFAULT partition",
                    "DEFAULT partitions require PostgreSQL 11+.",
                )

        parts = ["CREATE TABLE"]
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")
        if expr.schema:
            child_name = (
                f"{self.format_identifier(expr.schema)}."
                f"{self.format_identifier(expr.partition_name)}"
            )
        else:
            child_name = self.format_identifier(expr.partition_name)
        parts.append(child_name)

        parent_schema = expr.parent_schema if expr.parent_schema is not None else expr.schema
        if parent_schema:
            parent_name = (
                f"{self.format_identifier(parent_schema)}."
                f"{self.format_identifier(expr.parent_table)}"
            )
        else:
            parent_name = self.format_identifier(expr.parent_table)
        parts.append(f"PARTITION OF {parent_name}")

        params: List[Any] = []
        from ...expression.ddl import PartitionValue

        if default_marker:
            parts.append("DEFAULT")
        elif partition_type == "RANGE":
            from_value = expr.partition_values.get("from")
            to_value = expr.partition_values.get("to")
            if from_value is None or to_value is None:
                raise ValueError("RANGE partition requires 'from' and 'to' values")
            from_values = list(from_value) if isinstance(from_value, (list, tuple)) else [from_value]
            to_values = list(to_value) if isinstance(to_value, (list, tuple)) else [to_value]
            if not from_values or not to_values:
                raise ValueError("RANGE partition boundaries must not be empty")
            if len(from_values) != len(to_values):
                raise ValueError("RANGE partition boundaries must have the same column count")
            from_sql_parts = []
            to_sql_parts = []
            for value in from_values:
                value_sql, value_params = PartitionValue(
                    self,
                    value,
                    partition_type="RANGE",
                ).to_sql()
                from_sql_parts.append(value_sql)
                params.extend(value_params)
            for value in to_values:
                value_sql, value_params = PartitionValue(
                    self,
                    value,
                    partition_type="RANGE",
                ).to_sql()
                to_sql_parts.append(value_sql)
                params.extend(value_params)
            parts.append(
                f"FOR VALUES FROM ({', '.join(from_sql_parts)}) "
                f"TO ({', '.join(to_sql_parts)})"
            )
        elif partition_type == "LIST":
            value_sql_parts, value_params = self._format_list_partition_values(
                expr.partition_values.get("values")
            )
            params.extend(value_params)
            parts.append(f"FOR VALUES IN ({', '.join(value_sql_parts)})")
        else:
            if not self.supports_hash_partitioning():
                raise UnsupportedFeatureError(
                    self.name,
                    "HASH partitioning",
                    "HASH partitioning requires PostgreSQL 11+.",
                )
            modulus = expr.partition_values.get("modulus")
            remainder = expr.partition_values.get("remainder")
            if modulus is None or remainder is None:
                raise ValueError("HASH partition requires 'modulus' and 'remainder'")
            if isinstance(modulus, bool) or not isinstance(modulus, int):
                raise TypeError("HASH modulus must be an int")
            if modulus <= 0:
                raise ValueError("HASH modulus must be a positive integer")
            if isinstance(remainder, bool) or not isinstance(remainder, int):
                raise TypeError("HASH remainder must be an int")
            if not 0 <= remainder < modulus:
                raise ValueError("HASH remainder must satisfy 0 <= remainder < modulus")
            parts.append(f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})")

        if expr.partition_clause is not None:
            if not isinstance(expr.partition_clause, PartitionClause):
                raise TypeError("partition_clause must be a PartitionClause")
            clause_sql, clause_params = expr.partition_clause.to_sql()
            if clause_params:
                raise ValueError("partition clause must not contain bind parameters")
            parts.append(clause_sql.strip())
        if expr.tablespace:
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")

        return " ".join(parts), tuple(params)

    def format_detach_partition_statement(
        self,
        expr: "PostgresDetachPartitionExpression",
    ) -> Tuple[str, tuple]:
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
        if not self.supports_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE ... DETACH PARTITION",
                "Declarative table partitioning requires PostgreSQL 10+.",
            )

        parts = ["ALTER TABLE"]

        parent_schema = expr.parent_schema if expr.parent_schema is not None else expr.schema
        if parent_schema:
            parts.append(
                f"{self.format_identifier(parent_schema)}."
                f"{self.format_identifier(expr.parent_table)}"
            )
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

    def format_attach_partition_statement(
        self,
        expr: "PostgresAttachPartitionExpression",
    ) -> Tuple[str, tuple]:
        """Format ALTER TABLE ... ATTACH PARTITION for PostgreSQL."""
        if not self.supports_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE ... ATTACH PARTITION",
                "Declarative table partitioning requires PostgreSQL 10+.",
            )
        if expr.concurrently:
            raise UnsupportedFeatureError(
                self.name,
                "ATTACH PARTITION CONCURRENTLY",
                "Use non-concurrent ATTACH PARTITION.",
            )
        if not isinstance(expr.partition_type, str):
            raise TypeError("partition_type must be a string")
        partition_type = expr.partition_type.upper()
        if partition_type not in {"RANGE", "LIST", "HASH"}:
            raise ValueError(f"Invalid partition_type: {partition_type}")

        default_marker = expr.partition_values.get("default", False)
        if not isinstance(default_marker, bool):
            raise TypeError("partition_values['default'] must be a bool")
        if default_marker:
            if partition_type == "HASH":
                raise ValueError("HASH partitions cannot use DEFAULT")
            if not self.supports_default_partition():
                raise UnsupportedFeatureError(
                    self.name,
                    "DEFAULT partition",
                    "DEFAULT partitions require PostgreSQL 11+.",
                )

        parts = ["ALTER TABLE"]
        parent_schema = expr.parent_schema if expr.parent_schema is not None else expr.schema
        if parent_schema:
            parent_name = (
                f"{self.format_identifier(parent_schema)}."
                f"{self.format_identifier(expr.parent_table)}"
            )
        else:
            parent_name = self.format_identifier(expr.parent_table)
        if expr.schema:
            partition_name = (
                f"{self.format_identifier(expr.schema)}."
                f"{self.format_identifier(expr.partition_name)}"
            )
        else:
            partition_name = self.format_identifier(expr.partition_name)
        parts.extend((parent_name, "ATTACH PARTITION", partition_name))

        params: List[Any] = []
        from ...expression.ddl import PartitionValue

        if default_marker:
            parts.append("DEFAULT")
        elif partition_type == "RANGE":
            from_value = expr.partition_values.get("from")
            to_value = expr.partition_values.get("to")
            if from_value is None or to_value is None:
                raise ValueError("RANGE partition requires 'from' and 'to' values")
            from_values = list(from_value) if isinstance(from_value, (list, tuple)) else [from_value]
            to_values = list(to_value) if isinstance(to_value, (list, tuple)) else [to_value]
            if not from_values or not to_values:
                raise ValueError("RANGE partition boundaries must not be empty")
            if len(from_values) != len(to_values):
                raise ValueError("RANGE partition boundaries must have the same column count")
            from_sql_parts = []
            to_sql_parts = []
            for value in from_values:
                value_sql, value_params = PartitionValue(
                    self,
                    value,
                    partition_type="RANGE",
                ).to_sql()
                from_sql_parts.append(value_sql)
                params.extend(value_params)
            for value in to_values:
                value_sql, value_params = PartitionValue(
                    self,
                    value,
                    partition_type="RANGE",
                ).to_sql()
                to_sql_parts.append(value_sql)
                params.extend(value_params)
            parts.append(
                f"FOR VALUES FROM ({', '.join(from_sql_parts)}) "
                f"TO ({', '.join(to_sql_parts)})"
            )
        elif partition_type == "LIST":
            value_sql_parts, value_params = self._format_list_partition_values(
                expr.partition_values.get("values")
            )
            params.extend(value_params)
            parts.append(f"FOR VALUES IN ({', '.join(value_sql_parts)})")
        else:
            if not self.supports_hash_partitioning():
                raise UnsupportedFeatureError(
                    self.name,
                    "HASH partitioning",
                    "HASH partitioning requires PostgreSQL 11+.",
                )
            modulus = expr.partition_values.get("modulus")
            remainder = expr.partition_values.get("remainder")
            if modulus is None or remainder is None:
                raise ValueError("HASH partition requires 'modulus' and 'remainder'")
            if isinstance(modulus, bool) or not isinstance(modulus, int):
                raise TypeError("HASH modulus must be an int")
            if modulus <= 0:
                raise ValueError("HASH modulus must be a positive integer")
            if isinstance(remainder, bool) or not isinstance(remainder, int):
                raise TypeError("HASH remainder must be an int")
            if not 0 <= remainder < modulus:
                raise ValueError("HASH remainder must satisfy 0 <= remainder < modulus")
            parts.append(f"FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})")

        return " ".join(parts), tuple(params)


    def format_partition_metadata_query(self, expr: "PostgresPartitionMetadataExpression") -> Tuple[str, tuple]:
        """Format pg_catalog query for partition metadata introspection.

        Builds a parameterised query against pg_catalog to inspect a partitioned
        table's partition key, child partitions, and their bounds.

        - ``expr.parent_table`` — name of the parent partitioned table.
        - ``expr.schema`` — optional schema to scope the query (None = all schemas).
        - ``expr.include_partitions`` — if True, lists child partitions with bounds;
          if False, returns only parent metadata without partition details.

        The query uses parameter binding (%s placeholders) with
        (parent_table, schema, schema) as parameters. When schema is None,
        the WHERE clause omits the namespace filter.

        Args:
            expr: PostgresPartitionMetadataExpression with parent table details.

        Returns:
            Tuple of (SQL string with %s placeholders, params tuple).

        Raises:
            UnsupportedFeatureError: If PostgreSQL version < 10.
        """
        if not self.supports_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "partition metadata introspection",
                "Partition metadata introspection requires PostgreSQL 10+.",
            )

        params: List[Any] = [expr.parent_table]
        schema_filter = ""
        if expr.schema is not None:
            schema_filter = f" AND parent_ns.nspname = {self.p()}"
            params.append(expr.schema)

        if expr.include_partitions:
            sql = f"""
                SELECT pg_get_partkeydef(parent.oid) AS partition_key,
                       child.relname AS name,
                       pg_get_expr(child.relpartbound, child.oid) AS bound
                FROM pg_class parent
                JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                LEFT JOIN pg_partitioned_table partitioned_parent
                  ON partitioned_parent.partrelid = parent.oid
                LEFT JOIN pg_inherits i ON i.inhparent = parent.oid
                LEFT JOIN pg_class child ON child.oid = i.inhrelid
                WHERE parent.relname = {self.p()}{schema_filter}
                  AND (
                      parent.relkind = 'p'
                      OR partitioned_parent.partrelid IS NOT NULL
                  )
                ORDER BY child.relname
            """
        else:
            sql = f"""
                SELECT pg_get_partkeydef(parent.oid) AS partition_key,
                       NULL::text AS name,
                       NULL::text AS bound
                FROM pg_class parent
                JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                LEFT JOIN pg_partitioned_table partitioned_parent
                  ON partitioned_parent.partrelid = parent.oid
                WHERE parent.relname = {self.p()}{schema_filter}
                  AND (
                      parent.relkind = 'p'
                      OR partitioned_parent.partrelid IS NOT NULL
                  )
            """
        return sql, tuple(params)


class PostgresPartitionLifecycleProvider:
    """Construct PostgreSQL partition lifecycle expressions."""

    _SUPPORT_METHODS = {
        PartitionOperation.CREATE: "supports_add_partition",
        PartitionOperation.DROP: "supports_drop_partition",
        PartitionOperation.TRUNCATE: "supports_truncate_partition",
        PartitionOperation.ATTACH: "supports_attach_partition",
        PartitionOperation.DETACH: "supports_detach_partition",
    }

    def __init__(self, dialect: Any):
        self.dialect = dialect

    def capabilities(self) -> PartitionCapabilities:
        operations = frozenset(
            operation
            for operation, method_name in self._SUPPORT_METHODS.items()
            if getattr(self.dialect, method_name)()
        )
        strategies = tuple(
            strategy
            for strategy, supported in (
                ("RANGE", self.dialect.supports_range_table_partitioning()),
                ("LIST", self.dialect.supports_list_table_partitioning()),
                ("HASH", self.dialect.supports_hash_table_partitioning()),
            )
            if supported
        )
        return PartitionCapabilities(operations, strategies)

    def supports(self, operation: PartitionOperation) -> bool:
        return operation in self._SUPPORT_METHODS and self.capabilities().supports(operation)

    def _require(self, request: PartitionRequest) -> None:
        if not self.supports(request.operation):
            raise PartitionOperationNotSupportedError(self.dialect.name, request.operation)

    def _require_type(self, request: PartitionRequest, request_type: type) -> None:
        if not isinstance(request, request_type):
            raise PartitionLifecycleContractError(
                f"PostgreSQL partition provider received {type(request).__name__} for "
                f"{request.operation.value}"
            )

    def build(self, request: PartitionRequest) -> BaseExpression:
        self._require(request)
        from rhosocial.activerecord.backend.expression.statements.ddl_table import DropTableExpression
        from rhosocial.activerecord.backend.expression.statements.ddl_truncate import TruncateExpression
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
            PostgresAttachPartitionExpression,
            PostgresCreatePartitionExpression,
            PostgresDetachPartitionExpression,
        )

        if request.operation is PartitionOperation.CREATE:
            self._require_type(request, CreatePartitionRequest)
            parent_schema = (
                request.parent_schema
                if request.parent_schema is not None
                else request.table.schema_name
            )
            partition_schema = (
                request.partition_schema
                if request.partition_schema is not None
                else request.table.schema_name
            )
            return PostgresCreatePartitionExpression(
                self.dialect,
                partition_name=request.partition_name,
                parent_table=request.table.name,
                partition_type=request.partition_type,
                partition_values=dict(request.partition_values),
                schema=partition_schema,
                parent_schema=parent_schema,
                partition_clause=request.partition_clause,
                tablespace=request.tablespace,
                if_not_exists=request.if_not_exists,
            )
        if request.operation is PartitionOperation.DROP:
            self._require_type(request, DropPartitionRequest)
            from rhosocial.activerecord.backend.expression.core import TableExpression
            partition_schema = (
                request.partition_schema
                if request.partition_schema is not None
                else request.table.schema_name
            )
            return DropTableExpression(
                self.dialect,
                TableExpression(
                    self.dialect,
                    request.partition_name,
                    schema_name=partition_schema,
                ),
            )
        if request.operation is PartitionOperation.TRUNCATE:
            self._require_type(request, TruncatePartitionRequest)
            partition_schema = (
                request.partition_schema
                if request.partition_schema is not None
                else request.table.schema_name
            )
            return TruncateExpression(
                self.dialect,
                request.partition_name,
                schema=partition_schema,
            )
        if request.operation is PartitionOperation.ATTACH:
            self._require_type(request, AttachPartitionRequest)
            parent_schema = (
                request.parent_schema
                if request.parent_schema is not None
                else request.table.schema_name
            )
            partition_schema = (
                request.partition_schema
                if request.partition_schema is not None
                else request.table.schema_name
            )
            return PostgresAttachPartitionExpression(
                self.dialect,
                partition_name=request.partition_name,
                parent_table=request.table.name,
                partition_type=request.partition_type,
                partition_values=dict(request.partition_values),
                schema=partition_schema,
                parent_schema=parent_schema,
                concurrently=request.concurrently,
            )
        if request.operation is PartitionOperation.DETACH:
            self._require_type(request, DetachPartitionRequest)
            parent_schema = (
                request.parent_schema
                if request.parent_schema is not None
                else request.table.schema_name
            )
            partition_schema = (
                request.partition_schema
                if request.partition_schema is not None
                else request.table.schema_name
            )
            return PostgresDetachPartitionExpression(
                self.dialect,
                partition_name=request.partition_name,
                parent_table=request.table.name,
                schema=partition_schema,
                parent_schema=parent_schema,
                concurrently=request.concurrently,
                finalize=request.finalize,
            )
        raise PartitionOperationNotSupportedError(self.dialect.name, request.operation)
