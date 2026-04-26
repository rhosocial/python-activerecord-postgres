# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/index.py
"""PostgreSQL index enhancements mixin implementation.

This module provides the PostgresIndexMixin class which implements
PostgreSQL-specific index features and operations.
"""

from typing import Any, Dict, Optional, Tuple, List, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import ToSQLProtocol

if TYPE_CHECKING:
    from ...expression.ddl import PostgresReindexExpression
    from rhosocial.activerecord.backend.expression.statements.ddl_index import (
        CreateIndexExpression,
    )


class PostgresIndexMixin:
    """PostgreSQL index enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_safe_hash_index(self) -> bool:
        """Hash index WAL logging is native feature, PG 10+."""
        return self.version >= (10, 0, 0)

    def supports_parallel_create_index(self) -> bool:
        """Parallel index build is native feature, PG 11+."""
        return self.version >= (11, 0, 0)

    def supports_gist_include(self) -> bool:
        """GiST INCLUDE is native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_reindex_concurrently(self) -> bool:
        """REINDEX CONCURRENTLY is native feature, PG 12+."""
        return self.version >= (12, 0, 0)

    def supports_btree_deduplication(self) -> bool:
        """B-tree deduplication is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    def supports_brin_multivalue(self) -> bool:
        """BRIN multivalue is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_brin_bloom(self) -> bool:
        """BRIN bloom filter is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_spgist_include(self) -> bool:
        """SP-GiST INCLUDE is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    # =========================================================================
    # Extended Statistics Support (PostgreSQL 10+)
    # =========================================================================

    def supports_create_statistics(self) -> bool:
        """CREATE STATISTICS for extended statistics is supported since PostgreSQL 10.

        Allows creating extended statistics objects for better
        query planning on column correlations.

        Returns:
            True if CREATE STATISTICS is supported
        """
        return self.version >= (10, 0, 0)

    def supports_statistics_mcv(self) -> bool:
        """MCV (Most Common Values) statistics are supported since PostgreSQL 12.

        Extended statistics can include MCV lists for better
        estimation of combined column values.

        Returns:
            True if MCV statistics are supported
        """
        return self.version >= (12, 0, 0)

    def supports_statistics_dependencies(self) -> bool:
        """Statistics dependencies are supported since PostgreSQL 10.

        Functional dependencies between columns can be tracked
        for better row estimation.

        Returns:
            True if statistics dependencies are supported
        """
        return self.version >= (10, 0, 0)

    def supports_statistics_ndistinct(self) -> bool:
        """NDistinct statistics are supported since PostgreSQL 10.

        Tracks number of distinct values for column groups.

        Returns:
            True if ndistinct statistics are supported
        """
        return self.version >= (10, 0, 0)

    # =========================================================================
    # PostgreSQL 16+ Index Features
    # =========================================================================

    def supports_concurrent_unique_nulls_not_distinct(self) -> bool:
        """CONCURRENT unique index with NULLS NOT DISTINCT since PostgreSQL 16.

        Returns:
            True if supported
        """
        return self.version >= (16, 0, 0)

    # =========================================================================
    # PostgreSQL 18+ Index Features
    # =========================================================================

    def supports_streaming_btree_index_build(self) -> bool:
        """Streaming B-tree index build is supported since PostgreSQL 18.

        Allows concurrent index builds with less memory usage.

        Returns:
            True if supported
        """
        return self.version >= (18, 0, 0)

    def format_create_index_statement(self, expr: "CreateIndexExpression") -> Tuple[str, tuple]:
        """Format CREATE INDEX statement with PostgreSQL-specific options.

        Extends the generic format_create_index_statement to support:
        - Operator classes via dialect_options["opclasses"]
        - CONCURRENTLY clause
        - INCLUDE clause with version checks
        """
        all_params = []
        parts = ["CREATE"]

        if expr.unique:
            parts.append("UNIQUE")
        parts.append("INDEX")

        # CONCURRENTLY requires PG 11+
        if expr.concurrent:
            if not self.supports_parallel_create_index():
                raise ValueError("CREATE INDEX CONCURRENTLY requires PostgreSQL 11+")
            parts.append("CONCURRENTLY")

        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")
        parts.append(self.format_identifier(expr.index_name))
        parts.append("ON")
        parts.append(self.format_identifier(expr.table_name))

        if expr.index_type:
            parts.append(f"USING {expr.index_type}")

        # Columns with optional operator classes
        opclasses = expr.dialect_options.get("opclasses", {})
        col_parts = []
        for col in expr.columns:
            if isinstance(col, ToSQLProtocol):
                col_sql, col_params = col.to_sql()
                col_parts.append(col_sql)
                all_params.extend(col_params)
            else:
                col_name = self.format_identifier(str(col))
                # Append operator class if specified for this column
                opclass = opclasses.get(str(col))
                if opclass:
                    col_parts.append(f"{col_name} {opclass}")
                else:
                    col_parts.append(col_name)
        parts.append(f"({', '.join(col_parts)})")

        # INCLUDE clause (PG 12+ for GiST, PG 14+ for SP-GiST)
        if expr.include:
            index_type = (expr.index_type or "").lower()
            if index_type == "gist" and not self.supports_gist_include():
                raise ValueError("INCLUDE for GiST indexes requires PostgreSQL 12+")
            if index_type == "spgist" and not self.supports_spgist_include():
                raise ValueError("INCLUDE for SP-GiST indexes requires PostgreSQL 14+")
            include_cols = ", ".join(self.format_identifier(c) for c in expr.include)
            parts.append(f"INCLUDE ({include_cols})")

        # WITH options via dialect_options
        with_options = expr.dialect_options.get("with")
        if with_options:
            opts = ", ".join(f"{k} = {v}" for k, v in with_options.items())
            parts.append(f"WITH ({opts})")

        if expr.where:
            where_sql, where_params = expr.where.to_sql()
            parts.append(f"WHERE {where_sql}")
            all_params.extend(where_params)

        if expr.tablespace:
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")

        return " ".join(parts), tuple(all_params)

    def format_reindex_statement(self, expr: "PostgresReindexExpression") -> Tuple[str, tuple]:
        """Format REINDEX statement with PostgreSQL-specific options."""
        target_type = expr.target_type.upper()
        if target_type not in ("INDEX", "TABLE", "SCHEMA", "DATABASE", "SYSTEM"):
            raise ValueError(f"Invalid target_type: {target_type}")

        parts = ["REINDEX"]

        # CONCURRENTLY requires PG 12+
        if expr.concurrently:
            if not self.supports_reindex_concurrently():
                raise ValueError("REINDEX CONCURRENTLY requires PostgreSQL 12+")
            parts.append("CONCURRENTLY")

        parts.append(target_type)

        # Add schema qualifier for INDEX and TABLE
        if target_type in ("INDEX", "TABLE") and expr.schema:
            parts.append(f"{self.format_identifier(expr.schema)}.")

        # Add name (format as identifier)
        parts.append(self.format_identifier(expr.name))

        # TABLESPACE option (PG 14+)
        if expr.tablespace:
            parts.append("TABLESPACE")
            parts.append(self.format_identifier(expr.tablespace))

        if expr.verbose:
            parts.append("VERBOSE")

        return (" ".join(parts), ())

    def format_create_index_pg_statement(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
        schema: Optional[str] = None,
        unique: bool = False,
        index_type: str = "btree",
        concurrently: bool = False,
        if_not_exists: bool = False,
        include_columns: Optional[List[str]] = None,
        with_options: Optional[Dict[str, Any]] = None,
        tablespace: Optional[str] = None,
        where_clause: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement with PostgreSQL-specific options.

        This method provides a convenient way to create indexes with
        PostgreSQL-specific options without using expression objects.
        """
        parts = ["CREATE"]

        if unique:
            parts.append("UNIQUE")

        parts.append("INDEX")

        # CONCURRENTLY requires PG 11+
        if concurrently:
            if not self.supports_parallel_create_index():
                raise ValueError("CREATE INDEX CONCURRENTLY requires PostgreSQL 11+")
            parts.append("CONCURRENTLY")

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        # Index name with optional schema
        if schema:
            parts.append(f"{self.format_identifier(schema)}.{self.format_identifier(index_name)}")
        else:
            parts.append(self.format_identifier(index_name))

        # ON table
        parts.append("ON")
        if schema:
            parts.append(f"{self.format_identifier(schema)}.{self.format_identifier(table_name)}")
        else:
            parts.append(self.format_identifier(table_name))

        # Index type
        index_type = index_type.lower()
        valid_types = ("btree", "hash", "gist", "gin", "spgist", "brin")
        if index_type not in valid_types:
            raise ValueError(f"Invalid index_type: {index_type}. Must be one of {valid_types}")
        parts.append(f"USING {index_type}")

        # Columns
        parts.append("(" + ", ".join(self.format_identifier(c) for c in columns) + ")")

        # INCLUDE clause (PG 12+ for GiST, PG 14+ for SP-GiST)
        if include_columns:
            if index_type == "gist" and not self.supports_gist_include():
                raise ValueError("INCLUDE for GiST indexes requires PostgreSQL 12+")
            if index_type == "spgist" and not self.supports_spgist_include():
                raise ValueError("INCLUDE for SP-GiST indexes requires PostgreSQL 14+")
            parts.append("INCLUDE (" + ", ".join(self.format_identifier(c) for c in include_columns) + ")")

        # WITH options
        if with_options:
            opts = ", ".join(f"{k} = {v}" for k, v in with_options.items())
            parts.append(f"WITH ({opts})")

        # TABLESPACE
        if tablespace:
            parts.append(f"TABLESPACE {self.format_identifier(tablespace)}")

        # WHERE clause for partial index
        if where_clause:
            parts.append(f"WHERE {where_clause}")

        return (" ".join(parts), ())
