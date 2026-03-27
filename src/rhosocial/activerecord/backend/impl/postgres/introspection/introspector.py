# src/rhosocial/activerecord/backend/impl/postgres/introspection/introspector.py
"""
PostgreSQL concrete introspectors.

Implements SyncAbstractIntrospector and AsyncAbstractIntrospector for PostgreSQL
databases using pg_catalog system tables for metadata queries.

The introspectors are exposed via ``backend.introspector``.

Key behaviours:
  - Queries pg_catalog system tables (pg_class, pg_attribute, pg_index,
    pg_constraint, pg_trigger, pg_proc, etc.)
  - _parse_* methods are pure Python — shared by sync and async introspectors
  - Default schema is 'public'
  - tgtype bitmap is decoded properly for trigger events

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
    - SQL generation overrides
    - _parse_* implementations
    - Trigger decoding helpers
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
    # SQL generation overrides — direct pg_catalog queries
    # ------------------------------------------------------------------ #

    def _build_database_info_sql(self):
        """Return SQL that fetches charset/collation for the current database."""
        db_name = self._get_database_name()
        sql = (
            "SELECT datname, pg_encoding_to_char(encoding) AS encoding, "
            "datcollate, pg_database_size(datname) AS size "
            "FROM pg_database WHERE datname = %s"
        )
        return sql, (db_name,)

    def _build_table_list_sql(
        self,
        schema: Optional[str],
        include_system: bool,
        include_views: bool = True,
        table_type: Optional[str] = None,
    ):
        target_schema = schema if schema is not None else self._get_default_schema()

        # Decide which relkinds to include
        if include_views:
            relkinds = "('r', 'v', 'm', 'f', 'p')"
        else:
            relkinds = "('r', 'p')"

        sql = (
            "SELECT c.relname AS table_name, "
            "CASE c.relkind "
            "  WHEN 'r' THEN 'BASE TABLE' "
            "  WHEN 'v' THEN 'VIEW' "
            "  WHEN 'm' THEN 'MATERIALIZED VIEW' "
            "  WHEN 'f' THEN 'FOREIGN TABLE' "
            "  WHEN 'p' THEN 'PARTITIONED TABLE' "
            "END AS table_type, "
            "obj_description(c.oid) AS comment, "
            "c.reltuples::bigint AS row_count, "
            "pg_total_relation_size(c.oid) AS size_bytes "
            "FROM pg_class c "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            f"WHERE n.nspname = %s AND c.relkind IN {relkinds}"
        )
        params: list = [target_schema]

        if not include_system:
            sql += " AND c.relname NOT LIKE 'pg_%%'"

        sql += " ORDER BY c.relname"
        return sql, tuple(params)

    def _build_column_info_sql(self, table_name: str, schema: Optional[str]):
        target_schema = schema if schema is not None else self._get_default_schema()
        sql = (
            "SELECT a.attname AS column_name, "
            "a.attnum AS ordinal_position, "
            "pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type, "
            "NOT a.attnotnull AS is_nullable, "
            "pg_get_expr(d.adbin, d.adrelid) AS default_value, "
            "(EXISTS ("
            "  SELECT 1 FROM pg_index i "
            "  WHERE i.indrelid = a.attrelid AND i.indisprimary "
            "  AND a.attnum = ANY(i.indkey)"
            ")) AS is_primary_key, "
            "col_description(a.attrelid, a.attnum) AS comment "
            "FROM pg_attribute a "
            "LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum "
            "JOIN pg_class c ON a.attrelid = c.oid "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = %s AND c.relname = %s "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum"
        )
        return sql, (target_schema, table_name)

    def _build_index_info_sql(self, table_name: str, schema: Optional[str]):
        target_schema = schema if schema is not None else self._get_default_schema()
        sql = (
            "SELECT i.relname AS index_name, "
            "a.attname AS column_name, "
            "array_position(ix.indkey, a.attnum) AS column_position, "
            "ix.indisunique AS is_unique, "
            "ix.indisprimary AS is_primary, "
            "am.amname AS index_type, "
            "ix.indpred AS filter_condition "
            "FROM pg_index ix "
            "JOIN pg_class t ON ix.indrelid = t.oid "
            "JOIN pg_class i ON ix.indexrelid = i.oid "
            "JOIN pg_namespace n ON t.relnamespace = n.oid "
            "JOIN pg_am am ON i.relam = am.oid "
            "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) "
            "WHERE n.nspname = %s AND t.relname = %s "
            "ORDER BY i.relname, column_position"
        )
        return sql, (target_schema, table_name)

    def _build_foreign_key_sql(self, table_name: str, schema: Optional[str]):
        target_schema = schema if schema is not None else self._get_default_schema()
        sql = (
            "SELECT con.conname AS constraint_name, "
            "a.attname AS column_name, "
            "ref_class.relname AS referenced_table, "
            "ref_ns.nspname AS referenced_schema, "
            "ref_a.attname AS referenced_column, "
            "con.confdeltype::text AS on_delete, "
            "con.confupdtype::text AS on_update, "
            "array_position(con.conkey, a.attnum) AS column_position "
            "FROM pg_constraint con "
            "JOIN pg_class class ON con.conrelid = class.oid "
            "JOIN pg_namespace ns ON class.relnamespace = ns.oid "
            "JOIN pg_attribute a ON a.attrelid = class.oid AND a.attnum = ANY(con.conkey) "
            "JOIN pg_class ref_class ON con.confrelid = ref_class.oid "
            "JOIN pg_namespace ref_ns ON ref_class.relnamespace = ref_ns.oid "
            "JOIN pg_attribute ref_a ON ref_a.attrelid = ref_class.oid "
            "  AND ref_a.attnum = con.confkey[array_position(con.conkey, a.attnum)] "
            "WHERE con.contype = 'f' AND ns.nspname = %s AND class.relname = %s "
            "ORDER BY con.conname, column_position"
        )
        return sql, (target_schema, table_name)

    def _build_view_list_sql(self, schema: Optional[str], include_system: bool):
        target_schema = schema if schema is not None else self._get_default_schema()
        sql = (
            "SELECT c.relname AS view_name, "
            "pg_get_viewdef(c.oid, true) AS definition, "
            "obj_description(c.oid) AS comment "
            "FROM pg_class c "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = %s AND c.relkind = 'v'"
        )
        if not include_system:
            sql += " AND c.relname NOT LIKE 'pg_%%'"
        sql += " ORDER BY c.relname"
        return sql, (target_schema,)

    def _build_view_info_sql(self, view_name: str, schema: Optional[str]):
        target_schema = schema if schema is not None else self._get_default_schema()
        sql = (
            "SELECT c.relname AS view_name, "
            "pg_get_viewdef(c.oid, true) AS definition, "
            "obj_description(c.oid) AS comment "
            "FROM pg_class c "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = %s AND c.relkind = 'v' AND c.relname = %s"
        )
        return sql, (target_schema, view_name)

    def _build_trigger_list_sql(self, table_name: Optional[str], schema: Optional[str]):
        target_schema = schema if schema is not None else self._get_default_schema()
        # tgtype is a bitmask:
        #   bit 0 (1)  = ROW level (vs STATEMENT)
        #   bit 1 (2)  = BEFORE (vs AFTER)
        #   bit 2 (4)  = INSERT
        #   bit 3 (8)  = DELETE
        #   bit 4 (16) = UPDATE
        #   bit 5 (32) = TRUNCATE
        #   bit 6 (64) = INSTEAD OF
        sql = (
            "SELECT t.tgname AS trigger_name, "
            "c.relname AS table_name, "
            "t.tgtype::integer AS tgtype, "
            "p.proname AS function_name, "
            "t.tgenabled AS enabled "
            "FROM pg_trigger t "
            "JOIN pg_class c ON t.tgrelid = c.oid "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "JOIN pg_proc p ON t.tgfoid = p.oid "
            "WHERE n.nspname = %s AND NOT t.tgisinternal"
        )
        params: list = [target_schema]
        if table_name:
            sql += " AND c.relname = %s"
            params.append(table_name)
        sql += " ORDER BY t.tgname"
        return sql, tuple(params)

    # ------------------------------------------------------------------ #
    # Parse methods — pure Python, no I/O
    # ------------------------------------------------------------------ #

    def _parse_database_info(self, rows: List[Dict[str, Any]]) -> DatabaseInfo:
        db_row = rows[0] if rows else {}
        db_name = self._get_database_name()
        version_str = db_row.get("_version_str", "PostgreSQL 11.0")
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
            collation=db_row.get("datcollate"),
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
            tgtype = int(row.get("tgtype", 0))
            timing = self._decode_trigger_timing(tgtype)
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

    Usage::

        tables = backend.introspector.list_tables()
        info   = backend.introspector.get_table_info("users")
        cols   = backend.introspector.list_columns("users")
    """

    def __init__(self, backend: Any, executor: SyncIntrospectorExecutor) -> None:
        super().__init__(backend, executor)

    # ------------------------------------------------------------------ #
    # get_database_info override — needs two queries
    # ------------------------------------------------------------------ #

    def get_database_info(self) -> DatabaseInfo:
        """Return basic information about the connected PostgreSQL database.

        Overrides the base class to perform two queries:
        1. SELECT version() — for version string
        2. pg_database — for charset/collation
        """
        key = self._make_cache_key(IntrospectionScope.DATABASE)
        cached = self._get_cached(key)
        if cached is not None:
            return cached

        # Query version
        version_rows = self._executor.execute("SELECT version()", ())
        version_str = version_rows[0]["version"] if version_rows else "PostgreSQL 11.0"

        # Query pg_database
        pg_sql, pg_params = self._build_database_info_sql()
        db_rows = self._executor.execute(pg_sql, pg_params)

        # Inject version_str into the row so _parse_database_info can use it
        db_row = dict(db_rows[0]) if db_rows else {}
        db_row["_version_str"] = version_str

        result = self._parse_database_info([db_row])
        self._set_cached(key, result)
        return result


class AsyncPostgreSQLIntrospector(PostgreSQLIntrospectorMixin, AsyncAbstractIntrospector):
    """Asynchronous introspector for PostgreSQL backends.

    Uses pg_catalog system tables rather than information_schema for
    accurate, high-performance metadata queries.

    Usage::

        tables = await backend.introspector.list_tables()
        info   = await backend.introspector.get_table_info("users")
        cols   = await backend.introspector.list_columns("users")
    """

    def __init__(self, backend: Any, executor: AsyncIntrospectorExecutor) -> None:
        super().__init__(backend, executor)

    # ------------------------------------------------------------------ #
    # get_database_info override — needs two queries
    # ------------------------------------------------------------------ #

    async def get_database_info(self) -> DatabaseInfo:
        """Return basic information about the connected PostgreSQL database.

        Overrides the base class to perform two queries:
        1. SELECT version() — for version string
        2. pg_database — for charset/collation
        """
        key = self._make_cache_key(IntrospectionScope.DATABASE)
        cached = self._get_cached(key)
        if cached is not None:
            return cached

        # Query version
        version_rows = await self._executor.execute("SELECT version()", ())
        version_str = version_rows[0]["version"] if version_rows else "PostgreSQL 11.0"

        # Query pg_database
        pg_sql, pg_params = self._build_database_info_sql()
        db_rows = await self._executor.execute(pg_sql, pg_params)

        # Inject version_str into the row so _parse_database_info can use it
        db_row = dict(db_rows[0]) if db_rows else {}
        db_row["_version_str"] = version_str

        result = self._parse_database_info([db_row])
        self._set_cached(key, result)
        return result
