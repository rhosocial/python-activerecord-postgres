# src/rhosocial/activerecord/backend/impl/postgres/introspection/introspector.py
"""
PostgreSQL concrete introspectors.

Implements SyncAbstractIntrospector and AsyncAbstractIntrospector for PostgreSQL
databases using pg_catalog system tables for metadata queries.

The introspectors are exposed via ``backend.introspector``.

Architecture:
  - SQL generation: Delegated to PostgresIntrospectionCapabilityMixin.format_*_query()
    methods in the Dialect layer via Expression.to_sql()
  - Query execution: Handled by IntrospectorExecutor
  - Result parsing: _parse_* methods in this module (pure Python, no I/O)

Key behaviours:
  - Queries pg_catalog system tables (pg_class, pg_attribute, pg_index,
    pg_constraint, pg_trigger, pg_proc, etc.)
  - _parse_* methods are pure Python — shared by sync and async introspectors
  - Default schema is 'public'

Design principle: Sync and Async are separate and cannot coexist.
- SyncPostgreSQLIntrospector: for synchronous backends
- AsyncPostgreSQLIntrospector: for asynchronous backends
"""

import re
from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.introspection.base import (
    IntrospectorMixin,
    SyncAbstractIntrospector,
    AsyncAbstractIntrospector,
)
from rhosocial.activerecord.backend.introspection.executor import (
    SyncIntrospectorExecutor,
    AsyncIntrospectorExecutor,
)
from rhosocial.activerecord.backend.introspection.types import (
    ColumnInfo,
    ColumnNullable,
    DatabaseInfo,
    ForeignKeyInfo,
    IndexColumnInfo,
    IndexInfo,
    IndexType,
    IntrospectionScope,
    ReferentialAction,
    TableInfo,
    TableType,
    TriggerInfo,
    ViewInfo,
)


class PostgreSQLIntrospectorMixin(IntrospectorMixin):
    """Mixin providing shared PostgreSQL-specific introspection logic.

    Both SyncPostgreSQLIntrospector and AsyncPostgreSQLIntrospector inherit
    from this mixin to share:
    - Default schema handling
    - _parse_* implementations
    - Trigger decoding helpers

    SQL generation is delegated to the Dialect layer via Expression.to_sql()
    which calls PostgresIntrospectionCapabilityMixin.format_*_query() methods.
    """

    def _get_default_schema(self) -> str:
        """Return 'public' as PostgreSQL default schema."""
        return "public"

    def _get_database_name(self) -> str:
        """Return the PostgreSQL database name from backend config."""
        if hasattr(self._backend, "config") and hasattr(self._backend.config, "database"):
            return self._backend.config.database or "postgres"
        return "postgres"

    # ------------------------------------------------------------------ #
    # Parse methods — pure Python, no I/O
    # ------------------------------------------------------------------ #

    def _parse_database_info(self, rows: List[Dict[str, Any]]) -> DatabaseInfo:
        db_row = rows[0] if rows else {}
        db_name = self._get_database_name()
        # Dialect returns 'version' column from format_database_info_query()
        version_str = db_row.get("version", "PostgreSQL 11.0")
        match = re.search(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?", version_str)
        version_tuple = (
            (
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)) if match.group(3) else 0,
            )
            if match
            else (11, 0, 0)
        )

        return DatabaseInfo(
            name=db_name,
            version=version_str,
            version_tuple=version_tuple,
            vendor="PostgreSQL",
            encoding=db_row.get("encoding"),
            collation=db_row.get("collation"),  # Dialect returns 'collation' (not 'datcollate')
            size_bytes=db_row.get("size"),
        )

    def _parse_tables(self, rows: List[Dict[str, Any]], schema: Optional[str]) -> List[TableInfo]:
        target_schema = schema if schema is not None else self._get_default_schema()
        table_type_map = {
            "BASE TABLE": TableType.BASE_TABLE,
            "VIEW": TableType.VIEW,
            "MATERIALIZED VIEW": TableType.BASE_TABLE,
            "FOREIGN TABLE": TableType.EXTERNAL,
            "PARTITIONED TABLE": TableType.BASE_TABLE,
        }
        return [
            TableInfo(
                name=row["table_name"],
                schema=target_schema,
                table_type=table_type_map.get(row.get("table_type", "BASE TABLE"), TableType.BASE_TABLE),
                comment=row.get("comment"),
                row_count=row.get("row_count"),
                size_bytes=row.get("size_bytes"),
            )
            for row in rows
        ]

    def _parse_columns(self, rows: List[Dict[str, Any]], table_name: str, schema: str) -> List[ColumnInfo]:
        columns = []
        for row in rows:
            data_type = row.get("data_type", "")
            # Extract base type (remove array brackets, size specifiers)
            base_type = data_type.split("[")[0].split("(")[0].strip()
            columns.append(
                ColumnInfo(
                    name=row["column_name"],
                    table_name=table_name,
                    schema=schema,
                    ordinal_position=row["ordinal_position"],
                    data_type=base_type,
                    data_type_full=data_type,
                    nullable=ColumnNullable.NULLABLE if row.get("is_nullable") else ColumnNullable.NOT_NULL,
                    default_value=row.get("default_value"),
                    is_primary_key=row.get("is_primary_key", False),
                    comment=row.get("comment"),
                )
            )
        return columns

    def _parse_indexes(self, rows: List[Dict[str, Any]], table_name: str, schema: str) -> List[IndexInfo]:
        index_type_map = {
            "btree": IndexType.BTREE,
            "hash": IndexType.HASH,
            "gin": IndexType.GIN,
            "gist": IndexType.GIST,
            "spgist": IndexType.GIST,
            "brin": IndexType.BTREE,
        }
        index_map: Dict[str, IndexInfo] = {}
        for row in rows:
            idx_name = row["index_name"]
            if idx_name not in index_map:
                idx_type_str = (row.get("index_type") or "btree").lower()
                index_map[idx_name] = IndexInfo(
                    name=idx_name,
                    table_name=table_name,
                    schema=schema,
                    is_unique=bool(row.get("is_unique")),
                    is_primary=bool(row.get("is_primary")),
                    index_type=index_type_map.get(idx_type_str, IndexType.UNKNOWN),
                    columns=[],
                    filter_condition=row.get("filter_condition"),
                )
            index_map[idx_name].columns.append(
                IndexColumnInfo(
                    name=row["column_name"],
                    ordinal_position=(row.get("column_position") or 0) + 1,
                )
            )
        return list(index_map.values())

    def _parse_foreign_keys(self, rows: List[Dict[str, Any]], table_name: str, schema: str) -> List[ForeignKeyInfo]:
        # PostgreSQL uses single-char codes for referential actions
        action_map = {
            "a": ReferentialAction.NO_ACTION,
            "r": ReferentialAction.RESTRICT,
            "c": ReferentialAction.CASCADE,
            "n": ReferentialAction.SET_NULL,
            "d": ReferentialAction.SET_DEFAULT,
        }
        fk_map: Dict[str, ForeignKeyInfo] = {}
        for row in rows:
            fk_name = row["constraint_name"]
            if fk_name not in fk_map:
                fk_map[fk_name] = ForeignKeyInfo(
                    name=fk_name,
                    table_name=table_name,
                    schema=schema,
                    referenced_table=row.get("referenced_table", ""),
                    referenced_schema=row.get("referenced_schema"),
                    on_update=action_map.get(row.get("on_update", "a"), ReferentialAction.NO_ACTION),
                    on_delete=action_map.get(row.get("on_delete", "a"), ReferentialAction.NO_ACTION),
                    columns=[],
                    referenced_columns=[],
                )
            fk_map[fk_name].columns.append(row.get("column_name", ""))
            fk_map[fk_name].referenced_columns.append(row.get("referenced_column", ""))
        return list(fk_map.values())

    def _parse_views(self, rows: List[Dict[str, Any]], schema: str) -> List[ViewInfo]:
        return [
            ViewInfo(
                name=row.get("view_name", ""),
                schema=schema,
                definition=row.get("definition"),
                comment=row.get("comment"),
            )
            for row in rows
        ]

    def _parse_view_info(self, rows: List[Dict[str, Any]], view_name: str, schema: str) -> Optional[ViewInfo]:
        if not rows:
            return None
        row = rows[0]
        return ViewInfo(
            name=row.get("view_name", view_name),
            schema=schema,
            definition=row.get("definition"),
            comment=row.get("comment"),
        )

    def _parse_triggers(self, rows: List[Dict[str, Any]], schema: str) -> List[TriggerInfo]:
        triggers = []
        for row in rows:
            # Dialect returns 'timing' column already decoded
            timing = row.get("timing", "AFTER")
            # Dialect returns 'trigger_type' (tgtype) for event decoding
            tgtype = int(row.get("trigger_type", row.get("tgtype", 0)))
            events = self._decode_trigger_events(tgtype)
            per_row = bool(tgtype & 1)  # bit 0 = row-level
            level = "ROW" if per_row else "STATEMENT"
            enabled = row.get("enabled", "O")
            status = "ENABLED" if enabled != "D" else "DISABLED"
            function_name = row.get("function_name")

            triggers.append(
                TriggerInfo(
                    name=row["trigger_name"],
                    table_name=row["table_name"],
                    schema=schema,
                    timing=timing,
                    events=events,
                    level=level,
                    definition=function_name,
                    status=status,
                    extra={"function_name": function_name} if function_name else {},
                )
            )
        return triggers

    @staticmethod
    def _decode_trigger_timing(tgtype: int) -> str:
        """Decode timing from tgtype bitmask.

        Bit 6 (64) = INSTEAD OF
        Bit 1 (2)  = BEFORE
        Otherwise  = AFTER
        """
        if tgtype & 64:
            return "INSTEAD OF"
        if tgtype & 2:
            return "BEFORE"
        return "AFTER"

    @staticmethod
    def _decode_trigger_events(tgtype: int) -> List[str]:
        """Decode events from tgtype bitmask.

        Bit 2 (4)  = INSERT
        Bit 3 (8)  = DELETE
        Bit 4 (16) = UPDATE
        Bit 5 (32) = TRUNCATE
        """
        events = []
        if tgtype & 4:
            events.append("INSERT")
        if tgtype & 8:
            events.append("DELETE")
        if tgtype & 16:
            events.append("UPDATE")
        if tgtype & 32:
            events.append("TRUNCATE")
        return events if events else ["UPDATE"]


class SyncPostgreSQLIntrospector(PostgreSQLIntrospectorMixin, SyncAbstractIntrospector):
    """Synchronous introspector for PostgreSQL backends.

    Uses pg_catalog system tables rather than information_schema for
    accurate, high-performance metadata queries.

    SQL generation is delegated to PostgresIntrospectionCapabilityMixin
    in the Dialect layer via Expression.to_sql().

    Usage::

        tables = backend.introspector.list_tables()
        info   = backend.introspector.get_table_info("users")
        cols   = backend.introspector.list_columns("users")
    """

    def __init__(self, backend: Any, executor: SyncIntrospectorExecutor) -> None:
        super().__init__(backend, executor)


class AsyncPostgreSQLIntrospector(PostgreSQLIntrospectorMixin, AsyncAbstractIntrospector):
    """Asynchronous introspector for PostgreSQL backends.

    Uses pg_catalog system tables rather than information_schema for
    accurate, high-performance metadata queries.

    SQL generation is delegated to PostgresIntrospectionCapabilityMixin
    in the Dialect layer via Expression.to_sql().

    Usage::

        tables = await backend.introspector.list_tables()
        info   = await backend.introspector.get_table_info("users")
        cols   = await backend.introspector.list_columns("users")
    """

    def __init__(self, backend: Any, executor: AsyncIntrospectorExecutor) -> None:
        super().__init__(backend, executor)
