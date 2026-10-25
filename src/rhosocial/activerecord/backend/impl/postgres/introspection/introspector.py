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
  - Result parsing: parse_* methods in this module (pure Python, no I/O)

Key behaviours:
  - Queries pg_catalog system tables (pg_class, pg_attribute, pg_index,
    pg_constraint, pg_trigger, pg_proc, etc.)
  - parse_* methods are pure Python — shared by sync and async introspectors
  - Default schema is 'public'

Design principle: Sync and Async are separate and cannot coexist.
- SyncPostgreSQLIntrospector: for synchronous backends
- AsyncPostgreSQLIntrospector: for asynchronous backends
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from rhosocial.activerecord.backend.introspection.base import (
    IntrospectorMixin,
    SyncAbstractIntrospector,
    AsyncAbstractIntrospector,
)
from rhosocial.activerecord.backend.introspection.cache import IntrospectionCache
from rhosocial.activerecord.backend.introspection.executor import (
    SyncIntrospectorExecutor,
    AsyncIntrospectorExecutor,
)
from rhosocial.activerecord.backend.introspection.status import ExtensionInfo
from rhosocial.activerecord.backend.introspection.types import (
    IntrospectionColumnInfo,
    ColumnNullable,
    IntrospectionDatabaseInfo,
    IntrospectionForeignKeyInfo,
    IndexColumnInfo,
    IndexInfo,
    IndexType,
    IntrospectionScope,
    ReferentialAction,
    IntrospectionTableInfo,
    TableType,
    IntrospectionTriggerInfo,
    IntrospectionViewInfo,
)
from rhosocial.activerecord.backend.expression.types import DataType
from .status_introspector import (
    SyncPostgreSQLStatusIntrospector,
    AsyncPostgreSQLStatusIntrospector,
)
from .types import SequenceInfo


class PostgreSQLIntrospectorMixin(IntrospectorMixin):
    """Mixin providing shared PostgreSQL-specific introspection logic.

    Both SyncPostgreSQLIntrospector and AsyncPostgreSQLIntrospector inherit
    from this mixin to share:
    - Default schema handling
    - parse_* implementations
    - Trigger decoding helpers

    SQL generation is delegated to the Dialect layer via Expression.to_sql()
    which calls PostgresIntrospectionCapabilityMixin.format_*_query() methods.
    """

    @property
    def default_schema(self) -> str:
        """The actual schema from backend config or search_path.

        Priority:
        1. config.default_schema
        2. First entry of config.search_path (e.g. 'broker, public' → 'broker')
        3. Fallback to 'public'
        """
        config = getattr(self._backend, "config", None) if hasattr(self, "_backend") else None
        if config is not None:
            if config.default_schema:
                return str(config.default_schema)
            if config.search_path:
                sp = str(config.search_path)
                first = sp.split(",")[0].strip().strip('"').strip("'")
                if first:
                    return first
        return "public"

    def _get_database_name(self) -> str:
        """Return the PostgreSQL database name from backend config."""
        if hasattr(self._backend, "config") and hasattr(self._backend.config, "database"):
            return self._backend.config.database or "postgres"
        return "postgres"

    # ------------------------------------------------------------------ #
    # Parse methods — pure Python, no I/O
    # ------------------------------------------------------------------ #

    def parse_database_info(self, rows: List[Dict[str, Any]]) -> IntrospectionDatabaseInfo:
        db_row = rows[0] if rows else {}
        db_name = self._get_database_name()
        # Dialect returns 'version' column from format_database_info_query_statement()
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

        return IntrospectionDatabaseInfo(
            name=db_name,
            version=version_str,
            version_tuple=version_tuple,
            vendor="PostgreSQL",
            encoding=db_row.get("encoding"),
            collation=db_row.get("collation"),  # Dialect returns 'collation' (not 'datcollate')
            size_bytes=db_row.get("size"),
        )

    def parse_tables(self, rows: List[Dict[str, Any]], schema: Optional[str]) -> List[IntrospectionTableInfo]:
        target_schema = schema if schema is not None else self.default_schema
        table_type_map = {
            "BASE TABLE": TableType.BASE_TABLE,
            "VIEW": TableType.VIEW,
            "MATERIALIZED VIEW": TableType.BASE_TABLE,
            "FOREIGN TABLE": TableType.EXTERNAL,
            "PARTITIONED TABLE": TableType.BASE_TABLE,
        }
        return [
            IntrospectionTableInfo(
                name=row["table_name"],
                schema=target_schema,
                table_type=table_type_map.get(row.get("table_type", "BASE TABLE"), TableType.BASE_TABLE),
                comment=row.get("comment"),
                # PostgreSQL 14+ returns -1 for unanalyzed tables, convert to None for consistency
                row_count=row.get("row_count") if row.get("row_count") != -1 else None,
                size_bytes=row.get("size_bytes"),
            )
            for row in rows
        ]

    def parse_columns(self, rows: List[Dict[str, Any]], table_name: str, schema: str) -> List[IntrospectionColumnInfo]:
        columns = []
        for row in rows:
            data_type = row.get("data_type", "")
            # Extract base type (remove array brackets, size specifiers)
            base_type = data_type.split("[")[0].split("(")[0].strip()
            parsed = DataType.parse_data_type_str(self._backend.dialect, data_type)
            columns.append(
                IntrospectionColumnInfo(
                    name=row["column_name"],
                    table_name=table_name,
                    schema=schema,
                    ordinal_position=row["ordinal_position"],
                    data_type=base_type,
                    data_type_full=data_type,
                    parsed_data_type=parsed,
                    nullable=ColumnNullable.NULLABLE if row.get("is_nullable") else ColumnNullable.NOT_NULL,
                    default_value=row.get("default_value"),
                    is_primary_key=row.get("is_primary_key", False),
                    comment=row.get("comment"),
                )
            )
        return columns

    def parse_indexes(self, rows: List[Dict[str, Any]], table_name: str, schema: str) -> List[IndexInfo]:
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

    def parse_foreign_keys(
        self, rows: List[Dict[str, Any]], table_name: str, schema: str
    ) -> List[IntrospectionForeignKeyInfo]:
        # PostgreSQL uses single-char codes for referential actions
        action_map = {
            "a": ReferentialAction.NO_ACTION,
            "r": ReferentialAction.RESTRICT,
            "c": ReferentialAction.CASCADE,
            "n": ReferentialAction.SET_NULL,
            "d": ReferentialAction.SET_DEFAULT,
        }
        fk_map: Dict[str, IntrospectionForeignKeyInfo] = {}
        for row in rows:
            fk_name = row["constraint_name"]
            if fk_name not in fk_map:
                fk_map[fk_name] = IntrospectionForeignKeyInfo(
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

    def parse_views(self, rows: List[Dict[str, Any]], schema: str) -> List[IntrospectionViewInfo]:
        return [
            IntrospectionViewInfo(
                name=row.get("view_name", ""),
                schema=schema,
                definition=row.get("definition"),
                comment=row.get("comment"),
            )
            for row in rows
        ]

    def parse_view_info(
        self, rows: List[Dict[str, Any]], view_name: str, schema: str
    ) -> Optional[IntrospectionViewInfo]:
        if not rows:
            return None
        row = rows[0]
        return IntrospectionViewInfo(
            name=row.get("view_name", view_name),
            schema=schema,
            definition=row.get("definition"),
            comment=row.get("comment"),
        )

    @staticmethod
    def parse_materialized_view(
        row: Dict[str, Any], default_schema: str, default_name: str = ""
    ) -> IntrospectionViewInfo:
        """Build a IntrospectionViewInfo for a materialized view.

        Materialized views are never updatable/insertable; ``extra`` carries the
        PostgreSQL specific state: ``is_materialized``, ``is_populated`` and
        ``has_unique_index`` (the prerequisite for a CONCURRENTLY refresh).
        """
        return IntrospectionViewInfo(
            name=row.get("view_name") or default_name,
            schema=row.get("schema_name") or default_schema,
            definition=row.get("definition"),
            is_updatable=False,
            is_insertable=False,
            comment=row.get("comment"),
            extra={
                "is_materialized": True,
                "is_populated": bool(row.get("is_populated")),
                "has_unique_index": bool(row.get("has_unique_index")),
            },
        )

    @staticmethod
    def parse_materialized_views(
        rows: List[Dict[str, Any]], schema: str
    ) -> List[IntrospectionViewInfo]:
        return [
            PostgreSQLIntrospectorMixin.parse_materialized_view(row, schema) for row in rows
        ]

    @staticmethod
    def parse_materialized_view_info(
        rows: List[Dict[str, Any]], view_name: str, schema: str
    ) -> Optional[IntrospectionViewInfo]:
        if not rows:
            return None
        return PostgreSQLIntrospectorMixin.parse_materialized_view(
            rows[0], schema, view_name
        )

    def _build_materialized_view_list_sql(
        self, schema: Optional[str], include_system: bool
    ) -> Tuple[str, tuple]:
        from ..expression.introspection import PostgresMaterializedViewListQueryStatement

        target_schema = schema if schema is not None else self.default_schema
        expr = PostgresMaterializedViewListQueryStatement(
            self.dialect, schema=target_schema, include_system=include_system
        )
        return expr.to_sql()

    def _build_materialized_view_info_sql(
        self, view_name: str, schema: Optional[str]
    ) -> Tuple[str, tuple]:
        from ..expression.introspection import PostgresMaterializedViewInfoQueryStatement

        target_schema = schema if schema is not None else self.default_schema
        expr = PostgresMaterializedViewInfoQueryStatement(
            self.dialect, view_name, schema=target_schema
        )
        return expr.to_sql()

    def _materialized_view_cache_key(
        self, schema: Optional[str], view_name: Optional[str] = None, include_system: bool = False
    ) -> str:
        """Build the cache key for materialized view introspection results.

        Materialized views share ``IntrospectionScope.VIEW`` with regular views,
        so the key is namespaced to avoid cross-contamination.
        """
        return self._cache.key(
            IntrospectionScope.VIEW,
            view_name or "*",
            schema=schema,
            extra=f"matviews:{include_system}",
        )

    # ------------------------------------------------------------------ #
    # Sequence introspection
    # ------------------------------------------------------------------ #

    def _server_has_pg_sequences(self) -> bool:
        """Whether the connected server has the ``pg_sequences`` catalog view.

        The view arrived in PostgreSQL 10. A server older than that answers
        the widened question with ``relation does not exist``, so the caller
        asks the narrower one and reports the two columns ``pg_sequences``
        would have carried as absent. The version is read from the cache
        ``introspect_and_adapt()`` keeps, then from the dialect, which is how
        the rest of the backend answers the same question.

        A dialect that was never adapted raises rather than reporting an
        unknown version; that case answers the narrower question, because a
        server this backend never learned to speak to is not one to assume
        carries the newest catalog.
        """
        backend = getattr(self, "_backend", None)
        version = getattr(backend, "_server_version_cache", None) if backend is not None else None
        if not version:
            dialect = getattr(backend, "dialect", None) if backend is not None else None
            try:
                version = getattr(dialect, "version", None) if dialect is not None else None
            except Exception:
                version = None
        if not version:
            return False
        return tuple(version) >= (10, 0, 0)

    def _resolve_include_pg_sequences(self, include_pg_sequences: Optional[bool]) -> bool:
        """``None`` asks the server; an explicit flag is taken as given."""
        if include_pg_sequences is None:
            return self._server_has_pg_sequences()
        return include_pg_sequences

    @staticmethod
    def parse_sequence(
        row: Dict[str, Any], default_schema: str, default_name: str = ""
    ) -> SequenceInfo:
        """Build a SequenceInfo from one ``information_schema.sequences`` row.

        ``cycle_option`` is the server's ``YES``/``NO`` text, not a boolean,
        and ``last_value`` is ``None`` for a sequence that never handed out a
        value -- neither of which the caller should have to normalise.
        """
        cycle = row.get("cycle")
        return SequenceInfo(
            name=row.get("sequence_name") or default_name,
            schema=row.get("schema_name") or default_schema,
            data_type=row.get("data_type") or "",
            start_value=row.get("start_value"),
            minimum_value=row.get("minimum_value"),
            maximum_value=row.get("maximum_value"),
            increment=row.get("increment"),
            cycle=str(cycle).strip().upper() == "YES" if cycle is not None else False,
            cache_size=row.get("cache_size"),
            last_value=row.get("last_value"),
        )

    @staticmethod
    def parse_sequences(rows: List[Dict[str, Any]], schema: str) -> List[SequenceInfo]:
        return [
            PostgreSQLIntrospectorMixin.parse_sequence(row, schema) for row in rows
        ]

    @staticmethod
    def parse_sequence_info(
        rows: List[Dict[str, Any]], sequence_name: str, schema: str
    ) -> Optional[SequenceInfo]:
        if not rows:
            return None
        return PostgreSQLIntrospectorMixin.parse_sequence(rows[0], schema, sequence_name)

    def _build_sequence_list_sql(
        self, schema: Optional[str], include_pg_sequences: bool
    ) -> Tuple[str, tuple]:
        """Render the sequence list query for one schema."""
        from ..expression.introspection import PostgresSequenceListQueryStatement

        return PostgresSequenceListQueryStatement(
            self.dialect, schema=schema, include_pg_sequences=include_pg_sequences
        ).to_sql()

    def _build_sequence_info_sql(
        self, sequence_name: str, schema: Optional[str], include_pg_sequences: bool
    ) -> Tuple[str, tuple]:
        """Render the single-sequence query."""
        from ..expression.introspection import PostgresSequenceInfoQueryStatement

        return PostgresSequenceInfoQueryStatement(
            self.dialect,
            sequence_name,
            schema=schema,
            include_pg_sequences=include_pg_sequences,
        ).to_sql()

    def _sequence_cache_key(
        self,
        schema: Optional[str],
        sequence_name: Optional[str] = None,
        include_pg_sequences: bool = True,
    ) -> str:
        """Build the cache key for sequence introspection results.

        A 9.x server answers a different question than a 10+ one -- the two
        columns ``pg_sequences`` carries are simply absent -- so the flag
        is part of the key rather than assumed constant for a connection.
        """
        return self._cache.key(
            IntrospectionScope.SEQUENCE,
            sequence_name or "*",
            schema=schema,
            extra=str(include_pg_sequences),
        )

    # ------------------------------------------------------------------ #
    # Extension introspection
    # ------------------------------------------------------------------ #

    @staticmethod
    def parse_extensions(rows: List[Dict[str, Any]]) -> List[ExtensionInfo]:
        """Build the ExtensionInfo list from a ``pg_extension`` listing."""
        return [
            ExtensionInfo(
                name=row.get("name") or "",
                version=row.get("version"),
                schema=row.get("schema"),
                description=row.get("description"),
            )
            for row in rows
        ]

    def _build_extension_list_sql(self) -> Tuple[str, tuple]:
        """Render the installed-extension listing."""
        from ..expression.status import PostgresExtensionListQuery

        return PostgresExtensionListQuery(self.dialect).to_sql()

    def _extension_cache_key(self) -> str:
        """Build the cache key for the installed-extension listing.

        Extensions have no scope of their own, and the one closest in
        meaning is the schema: an extension is a named object published
        into a schema. The ``extra`` keeps the family apart from any other
        schema-listing that comes later.
        """
        return self._cache.key(
            IntrospectionScope.SCHEMA, "*", extra="extensions"
        )

    def parse_triggers(self, rows: List[Dict[str, Any]], schema: str) -> List[IntrospectionTriggerInfo]:
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
                IntrospectionTriggerInfo(
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

    def __init__(
        self,
        backend: Any,
        executor: SyncIntrospectorExecutor,
        cache: Optional[IntrospectionCache] = None,
    ) -> None:
        super().__init__(backend, executor, cache)
        self._status_instance: Optional[SyncPostgreSQLStatusIntrospector] = None

    @property
    def status(self) -> SyncPostgreSQLStatusIntrospector:
        """PostgreSQL status introspector (lazily created)."""
        if self._status_instance is None:
            self._status_instance = SyncPostgreSQLStatusIntrospector(self._backend)
        return self._status_instance

    def list_materialized_views(
        self,
        schema: Optional[str] = None,
        include_system: bool = False,
    ) -> List[IntrospectionViewInfo]:
        """List materialized views in the given schema.

        Each entry carries ``extra['is_populated']`` (false for a view created
        WITH NO DATA that was never refreshed) and ``extra['has_unique_index']``
        (the prerequisite for REFRESH ... CONCURRENTLY).

        Args:
            schema: Schema to inspect. Defaults to the connection default schema.
            include_system: Whether to include system objects.

        Returns:
            List of IntrospectionViewInfo describing each materialized view.
        """
        target = schema if schema is not None else self.default_schema
        key = self._materialized_view_cache_key(schema, include_system=include_system)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self._build_materialized_view_list_sql(schema, include_system)
        result = self.parse_materialized_views(self._executor.execute(sql, params), target)
        self._cache.set(key, result)
        return result

    def get_materialized_view_info(
        self, view_name: str, schema: Optional[str] = None
    ) -> Optional[IntrospectionViewInfo]:
        """Return information for a single materialized view, or None if absent.

        Args:
            view_name: Materialized view name.
            schema: Schema to inspect. Defaults to the connection default schema.

        Returns:
            IntrospectionViewInfo, or None when the materialized view does not exist.
        """
        target = schema if schema is not None else self.default_schema
        key = self._materialized_view_cache_key(schema, view_name=view_name)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self._build_materialized_view_info_sql(view_name, schema)
        result = self.parse_materialized_view_info(
            self._executor.execute(sql, params), view_name, target
        )
        if result is not None:
            self._cache.set(key, result)
        return result

    def materialized_view_exists(self, view_name: str, schema: Optional[str] = None) -> bool:
        """Return True when the named materialized view exists."""
        return self.get_materialized_view_info(view_name, schema) is not None

    # ------------------------------------------------------------------ #
    # Sequence introspection
    # ------------------------------------------------------------------ #

    def list_sequences(
        self,
        schema: Optional[str] = None,
        include_pg_sequences: Optional[bool] = None,
    ) -> List[SequenceInfo]:
        """List the sequences in one schema.

        The object list comes from ``information_schema.sequences``, the SQL
        standard view; ``pg_sequences`` is joined beside it for the runtime
        state the standard view omits (``cache_size`` and ``last_value``)
        whenever the server has it.

        Args:
            schema: Schema to inspect. Defaults to the connection default schema.
            include_pg_sequences: Ask for the PostgreSQL 10+ columns as well.
                ``None`` (the default) detects it from the server version.

        Returns:
            List of SequenceInfo, ordered by sequence name.
        """
        with_pg = self._resolve_include_pg_sequences(include_pg_sequences)
        key = self._sequence_cache_key(schema, include_pg_sequences=with_pg)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        target = schema if schema is not None else self.default_schema
        sql, params = self._build_sequence_list_sql(schema, with_pg)
        result = self.parse_sequences(self._executor.execute(sql, params), target)
        self._cache.set(key, result)
        return result

    def get_sequence_info(
        self,
        sequence_name: str,
        schema: Optional[str] = None,
        include_pg_sequences: Optional[bool] = None,
    ) -> Optional[SequenceInfo]:
        """Return information for one sequence, or None when absent.

        Args:
            sequence_name: Name of the sequence to read.
            schema: Schema to inspect. Defaults to the connection default schema.
            include_pg_sequences: Ask for the PostgreSQL 10+ columns as well.

        Returns:
            SequenceInfo, or None when the sequence does not exist.
        """
        with_pg = self._resolve_include_pg_sequences(include_pg_sequences)
        key = self._sequence_cache_key(schema, sequence_name=sequence_name, include_pg_sequences=with_pg)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        target = schema if schema is not None else self.default_schema
        sql, params = self._build_sequence_info_sql(sequence_name, schema, with_pg)
        result = self.parse_sequence_info(
            self._executor.execute(sql, params), sequence_name, target
        )
        if result is not None:
            self._cache.set(key, result)
        return result

    def sequence_exists(
        self, sequence_name: str, schema: Optional[str] = None
    ) -> bool:
        """Return True when the named sequence exists."""
        return self.get_sequence_info(sequence_name, schema) is not None

    # ------------------------------------------------------------------ #
    # Extension introspection
    # ------------------------------------------------------------------ #

    def list_extensions(self) -> List[ExtensionInfo]:
        """List the extensions installed in this database, by name.

        The listing is the catalog's own: ``pg_extension`` joined to
        ``pg_namespace`` for the schema each extension published its objects
        into. Extensions live in the schema introspector rather than the
        status introspector because they are schema objects, not position
        in a status report; ``status.get_overview()`` keeps its own call for
        the overview it builds.

        Returns:
            List of ExtensionInfo, ordered by extension name.
        """
        key = self._extension_cache_key()
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self._build_extension_list_sql()
        result = self.parse_extensions(self._executor.execute(sql, params))
        self._cache.set(key, result)
        return result

    def get_extension_info(self, name: str) -> Optional[ExtensionInfo]:
        """Return information for one installed extension, or None.

        The catalog is asked for the whole family once and filtered here,
        so a caller asking about a single extension pays one query rather
        than one per call.
        """
        return next(
            (ext for ext in self.list_extensions() if ext.name == name), None
        )

    def extension_exists(self, name: str) -> bool:
        """Return True when the named extension is installed."""
        return self.get_extension_info(name) is not None


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

    def __init__(
        self,
        backend: Any,
        executor: AsyncIntrospectorExecutor,
        cache: Optional[IntrospectionCache] = None,
    ) -> None:
        super().__init__(backend, executor, cache)
        self._status_instance: Optional[AsyncPostgreSQLStatusIntrospector] = None

    @property
    def status(self) -> AsyncPostgreSQLStatusIntrospector:
        """PostgreSQL status introspector (lazily created)."""
        if self._status_instance is None:
            self._status_instance = AsyncPostgreSQLStatusIntrospector(self._backend)
        return self._status_instance

    async def list_materialized_views(
        self,
        schema: Optional[str] = None,
        include_system: bool = False,
    ) -> List[IntrospectionViewInfo]:
        """List materialized views in the given schema (async counterpart).

        Args:
            schema: Schema to inspect. Defaults to the connection default schema.
            include_system: Whether to include system objects.

        Returns:
            List of IntrospectionViewInfo describing each materialized view.
        """
        target = schema if schema is not None else self.default_schema
        key = self._materialized_view_cache_key(schema, include_system=include_system)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self._build_materialized_view_list_sql(schema, include_system)
        rows = await self._executor.execute(sql, params)
        result = self.parse_materialized_views(rows, target)
        self._cache.set(key, result)
        return result

    async def get_materialized_view_info(
        self, view_name: str, schema: Optional[str] = None
    ) -> Optional[IntrospectionViewInfo]:
        """Return information for a single materialized view, or None (async).

        Args:
            view_name: Materialized view name.
            schema: Schema to inspect. Defaults to the connection default schema.

        Returns:
            IntrospectionViewInfo, or None when the materialized view does not exist.
        """
        target = schema if schema is not None else self.default_schema
        key = self._materialized_view_cache_key(schema, view_name=view_name)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self._build_materialized_view_info_sql(view_name, schema)
        rows = await self._executor.execute(sql, params)
        result = self.parse_materialized_view_info(rows, view_name, target)
        if result is not None:
            self._cache.set(key, result)
        return result

    async def materialized_view_exists(
        self, view_name: str, schema: Optional[str] = None
    ) -> bool:
        """Return True when the named materialized view exists (async)."""
        return await self.get_materialized_view_info(view_name, schema) is not None

    # ------------------------------------------------------------------ #
    # Sequence introspection
    # ------------------------------------------------------------------ #

    async def list_sequences(
        self,
        schema: Optional[str] = None,
        include_pg_sequences: Optional[bool] = None,
    ) -> List[SequenceInfo]:
        """List the sequences in one schema (async counterpart).

        Args:
            schema: Schema to inspect. Defaults to the connection default schema.
            include_pg_sequences: Ask for the PostgreSQL 10+ columns as well.

        Returns:
            List of SequenceInfo, ordered by sequence name.
        """
        with_pg = self._resolve_include_pg_sequences(include_pg_sequences)
        key = self._sequence_cache_key(schema, include_pg_sequences=with_pg)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        target = schema if schema is not None else self.default_schema
        sql, params = self._build_sequence_list_sql(schema, with_pg)
        rows = await self._executor.execute(sql, params)
        result = self.parse_sequences(rows, target)
        self._cache.set(key, result)
        return result

    async def get_sequence_info(
        self,
        sequence_name: str,
        schema: Optional[str] = None,
        include_pg_sequences: Optional[bool] = None,
    ) -> Optional[SequenceInfo]:
        """Return information for one sequence, or None when absent (async).

        Args:
            sequence_name: Name of the sequence to read.
            schema: Schema to inspect. Defaults to the connection default schema.
            include_pg_sequences: Ask for the PostgreSQL 10+ columns as well.

        Returns:
            SequenceInfo, or None when the sequence does not exist.
        """
        with_pg = self._resolve_include_pg_sequences(include_pg_sequences)
        key = self._sequence_cache_key(schema, sequence_name=sequence_name, include_pg_sequences=with_pg)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        target = schema if schema is not None else self.default_schema
        sql, params = self._build_sequence_info_sql(sequence_name, schema, with_pg)
        rows = await self._executor.execute(sql, params)
        result = self.parse_sequence_info(rows, sequence_name, target)
        if result is not None:
            self._cache.set(key, result)
        return result

    async def sequence_exists(
        self, sequence_name: str, schema: Optional[str] = None
    ) -> bool:
        """Return True when the named sequence exists (async)."""
        return await self.get_sequence_info(sequence_name, schema) is not None

    # ------------------------------------------------------------------ #
    # Extension introspection
    # ------------------------------------------------------------------ #

    async def list_extensions(self) -> List[ExtensionInfo]:
        """List the extensions installed in this database, by name (async).

        Returns:
            List of ExtensionInfo, ordered by extension name.
        """
        key = self._extension_cache_key()
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self._build_extension_list_sql()
        rows = await self._executor.execute(sql, params)
        result = self.parse_extensions(rows)
        self._cache.set(key, result)
        return result

    async def get_extension_info(self, name: str) -> Optional[ExtensionInfo]:
        """Return information for one installed extension, or None (async)."""
        return next(
            (ext for ext in await self.list_extensions() if ext.name == name), None
        )

    async def extension_exists(self, name: str) -> bool:
        """Return True when the named extension is installed (async)."""
        return await self.get_extension_info(name) is not None
