# src/rhosocial/activerecord/backend/impl/postgres/introspection/introspector.py
"""
PostgreSQL concrete introspectors.

Implements AbstractIntrospector and AsyncAbstractIntrospector for PostgreSQL
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
- PostgreSQLIntrospector: for synchronous backends
- AsyncPostgreSQLIntrospector: for asynchronous backends
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import json
import re

from rhosocial.activerecord.backend.introspection.base import (
    IntrospectorMixin,
    AbstractIntrospector,
    AsyncAbstractIntrospector,
)
from rhosocial.activerecord.backend.introspection.cache import IntrospectionCache
from rhosocial.activerecord.backend.introspection.defaults import VALUE_SAMPLE_LIMIT
from rhosocial.activerecord.backend.introspection.executor import (
    IntrospectorExecutor,
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
    ProfileSource,
)
from rhosocial.activerecord.backend.expression.types import DataType
from .status_introspector import (
    PostgreSQLStatusIntrospector,
    AsyncPostgreSQLStatusIntrospector,
)
from .types import (
    SequenceInfo,
    PostgresPartitionStrategy,
    PostgresPartitionBound,
    PostgresPartitionInfo,
    PostgresRoutineKind,
    PostgresRoutineInfo,
    PostgresTypeKind,
    PostgresTypeInfo,
    PostgresOperatorInfo,
    PostgresOperatorClassInfo,
    PostgresOperatorFamilyInfo,
)

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.introspection import (
        PostgresMaterializedViewInfoQueryStatement,
        PostgresMaterializedViewListQueryStatement,
        PostgresSequenceInfoQueryStatement,
        PostgresSequenceListQueryStatement,
        PostgresPartitionListQueryStatement,
        PostgresRoutineListQueryStatement,
        PostgresTypeListQueryStatement,
        PostgresOperatorListQueryStatement,
        PostgresOperatorClassListQueryStatement,
        PostgresOperatorFamilyListQueryStatement,
    )
    from ..expression.status import PostgresExtensionListQuery


class PostgreSQLIntrospectorMixin(IntrospectorMixin):
    """Mixin providing shared PostgreSQL-specific introspection logic.

    Both PostgreSQLIntrospector and AsyncPostgreSQLIntrospector inherit
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
        """Turn the one database row into a record.

        The version comes from :attr:`server_version`, not from the strict
        ``dialect.version``: a parser has no connection to ask, and refusing to
        describe a database until ``introspect_and_adapt()`` has run would make
        the answer depend on a step the caller did not need. Unknown is
        reported as the literal ``"unknown"`` with ``(0, 0, 0)``, which is
        distinguishable from a server that really is version 0.0.0.

        ``size_bytes`` is the size column the statement selects. ``table_count``
        is the count the statement selects; a missing or unreadable cell is
        reported as ``None`` -- unknown -- rather than allowed to raise past the
        rest of the answer, which is already known.
        """
        db_row = rows[0] if rows else {}
        version = self.server_version
        try:
            table_count = int(db_row.get("table_count"))
        except (TypeError, ValueError):
            table_count = None
        return IntrospectionDatabaseInfo(
            name=self._get_database_name(),
            version=".".join(str(part) for part in version) if version else "unknown",
            version_tuple=tuple(version) if version else (0, 0, 0),
            vendor="PostgreSQL",
            encoding=db_row.get("encoding"),
            collation=db_row.get("collation"),  # Dialect returns 'collation' (not 'datcollate')
            size_bytes=db_row.get("size"),
            table_count=table_count,
        )

    def parse_tables(self, rows: List[Dict[str, Any]], schema: Optional[str]) -> List[IntrospectionTableInfo]:
        """Turn a table listing into records.

        Every kind the catalog names gets its own :class:`TableType`: the
        catalog distinguished them, and collapsing a materialized view or a
        partitioned table back into ``BASE_TABLE`` would lose that at the only
        layer that saw it.

        The storage slots are filled from the trio the listing projects:
        ``size_bytes`` is the total footprint, ``data_size_bytes`` the heap and
        ``index_size_bytes`` the index side of it -- the two PostgreSQL keeps
        apart and a total cannot separate. A missing or unreadable cell is
        reported as ``None`` rather than allowed to raise past the rest of the
        answer, which is already known.

        **Which row estimate answers ``row_count``.** The listing carries two:
        ``reltuples``, the planner's estimate, and ``live_row_count``, the
        statistics view's ``n_live_tup``. The planner's is preferred, because it
        is the number the server itself plans with and it is refreshed by
        ``ANALYZE``/``VACUUM`` for exactly that purpose -- a value it holds is
        the server's best current estimate of the relation's size. The
        statistics view's is the fallback, taken only when the planner has
        none: PostgreSQL 14+ reports ``-1`` for a relation that has never been
        analyzed, which is the absence of an estimate rather than an empty
        table, and in that case a live-row estimate says more than nothing does.
        Neither is ever presented as exact, and neither is invented: a relation
        with neither estimate reports ``None``.
        """
        target_schema = schema if schema is not None else self.default_schema
        table_type_map = {
            "BASE TABLE": TableType.BASE_TABLE,
            "VIEW": TableType.VIEW,
            "MATERIALIZED VIEW": TableType.MATERIALIZED_VIEW,
            "FOREIGN TABLE": TableType.FOREIGN_TABLE,
            "PARTITIONED TABLE": TableType.PARTITIONED_TABLE,
        }
        return [
            IntrospectionTableInfo(
                name=row["table_name"],
                schema=target_schema,
                table_type=table_type_map.get(row.get("table_type", "BASE TABLE"), TableType.BASE_TABLE),
                comment=row.get("comment"),
                row_count=self.parse_row_count(row),
                size_bytes=row.get("size_bytes"),
                data_size_bytes=row.get("data_size_bytes"),
                index_size_bytes=row.get("index_size_bytes"),
            )
            for row in rows
        ]

    @staticmethod
    def parse_row_count(row: Dict[str, Any]) -> Optional[int]:
        """The row estimate a catalog row answers with, or ``None``.

        Two estimates arrive and one is chosen. ``reltuples`` is the planner's
        and wins when it holds a value; ``n_live_tup`` (``live_row_count``) is
        the statistics view's and answers only when the planner has none.

        PostgreSQL 14+ reports ``-1`` for a never-analyzed relation, which is
        "no estimate" rather than "zero rows", so that value is normalized to
        ``None`` here and the fallback is consulted; ``0`` from either source is
        a real estimate and passes through unchanged. Anything unreadable is
        ``None``: unknown is an answer, and guessing one would be worse.
        """
        reltuples = row.get("row_count")
        if reltuples is not None and reltuples != -1:
            return int(reltuples)
        live_rows = row.get("live_row_count")
        if live_rows is not None and live_rows != -1:
            return int(live_rows)
        return None

    @staticmethod
    def _is_serial_default(default_value: Any) -> bool:
        """Whether a column default is the ``nextval(...)`` a SERIAL carries.

        PostgreSQL spells a SERIAL column's default as
        ``nextval('table_column_seq'::regclass)``. That is the only signal on
        such a column -- it has no ``attidentity`` -- so recognising it is
        what lets a caller tell a database-generated primary key apart from
        one the application supplies.
        """
        if not isinstance(default_value, str):
            return False
        return default_value.strip().lower().startswith("nextval(")

    def parse_columns(self, rows: List[Dict[str, Any]], table_name: str, schema: str) -> List[IntrospectionColumnInfo]:
        columns = []
        for row in rows:
            data_type = row.get("data_type", "")
            # Extract base type (remove array brackets, size specifiers)
            base_type = data_type.split("[")[0].split("(")[0].strip()
            parsed = DataType.parse_data_type_str(self._backend.dialect, data_type)
            default_value = row.get("default_value")
            identity_kind = row.get("identity_kind") or ""
            generated_kind = row.get("generated_kind") or ""
            # Identity columns have attidentity and no pg_attrdef row; SERIAL
            # columns have the nextval default and no identity. Both mean the
            # database generates the value, and both are what a model
            # generator needs to set __pk_auto_generated__ correctly.
            is_auto_increment = bool(identity_kind) or self._is_serial_default(default_value)
            is_generated = bool(generated_kind)
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
                    default_value=default_value,
                    is_primary_key=row.get("is_primary_key", False),
                    is_auto_increment=is_auto_increment,
                    is_generated=is_generated,
                    generated_expression=default_value if is_generated else None,
                    comment=row.get("comment"),
                    # The four shape attributes information_schema.columns
                    # derives from atttypmod. They are absent for a relation
                    # that view does not list, which is why they are read
                    # with .get() rather than indexed.
                    character_maximum_length=row.get("character_maximum_length"),
                    numeric_precision=row.get("numeric_precision"),
                    numeric_scale=row.get("numeric_scale"),
                    datetime_precision=row.get("datetime_precision"),
                    extra=(
                        {"identity_kind": identity_kind}
                        if identity_kind
                        else {}
                    ),
                )
            )
        return columns

    # ------------------------------------------------------------------ #
    # Data profiles -- what the rows hold, as pg_stats estimates it
    # ------------------------------------------------------------------ #

    @staticmethod
    def _stats_list(value: Any) -> List[Any]:
        """A ``pg_stats`` array as a list, whichever shape the driver gave.

        The formatters project these as ``to_jsonb``, so psycopg may hand back
        a decoded list or the raw JSON text depending on how its json adapter
        is registered. Both are the same fact, and the parser should not care
        which arrived; anything else is "no list".
        """
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return list(value)
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "replace")
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except (ValueError, TypeError):
                return []
            return decoded if isinstance(decoded, list) else []
        return []

    @staticmethod
    def _lower_of(current: Any, candidate: Any) -> Any:
        """The smaller of two statistics values, when they can be compared.

        Both sides come out of ``to_jsonb``, so the shapes are JSON's: numbers,
        strings, booleans and null. Only like kinds are reconciled -- two
        numbers, or two strings, the latter covering text, and dates and uuids
        in their ISO spellings -- because a JSON string and a JSON number have
        no meaningful order between them.
        """
        if current is None:
            return candidate
        both_numbers = isinstance(current, (int, float)) and isinstance(
            candidate, (int, float)
        )
        both_strings = isinstance(current, str) and isinstance(candidate, str)
        if not (both_numbers or both_strings):
            return current
        return candidate if candidate < current else current

    def _merged_bounds(
        self, min_value: Any, max_value: Any, most_common: List[Any]
    ) -> "tuple":
        """The true extremes, histogram endpoints merged with the kept values.

        ``histogram_bounds`` deliberately excludes the values ANALYZE kept as
        most-common, so its first and last elements are the extremes of what
        is left. When the extreme value happens to be a frequent one -- a
        status column whose minimum is also its most common value -- the
        endpoint alone understates the range, and merging the retained values
        back in is what makes ``min``/``max`` mean what the slot promises.
        """
        for value in most_common:
            if value is None:
                continue
            min_value = self._lower_of(min_value, value)
            if max_value is None:
                max_value = value
            else:
                both_numbers = isinstance(max_value, (int, float)) and isinstance(
                    value, (int, float)
                )
                both_strings = isinstance(max_value, str) and isinstance(value, str)
                if (both_numbers or both_strings) and value > max_value:
                    max_value = value
        return min_value, max_value

    def parse_column_data_profiles(
        self,
        rows: List[Dict[str, Any]],
        schema: str,
        table_name: str,
    ) -> List[Any]:
        """Project ``pg_stats`` rows into profiles, converting what it encodes.

        Three conversions live here because each needs the whole row:

        * ``n_distinct`` is negative when it means a *fraction* of the rows, so
          the count is ``-n_distinct * reltuples``; a positive value is already
          a count and a zero is "not computed, but not zero rows either".
        * ``null_frac`` is ``-1`` on a column ANALYZE has not measured since
          the statistics were reset, which is "unknown" rather than a negative
          fraction.
        * ``reltuples`` is ``-1`` for a relation never scanned by the planner,
          which is likewise unknown rather than minus one row.

        ``source`` is ``ESTIMATE`` whenever the vintage is known and
        ``UNKNOWN`` when it is not: the figures are estimates either way, but
        the rule for this family is that an estimate must carry the time it
        was collected. Two things can leave that time unrecorded, and neither
        is an error: a ``pg_stat_reset()`` erases it while the statistics stay,
        and the statistics collector records ``last_analyze`` asynchronously,
        so a profile read immediately after ``ANALYZE`` usually finds no
        timestamp yet. Saying ``UNKNOWN`` keeps a figure whose age is unknown
        from claiming an age it does not have.

        ``min_value``/``max_value`` are the histogram's endpoints merged with
        the retained most-common values, because ANALYZE's histogram excludes
        what it kept as common. Both arrive as JSON scalars: a number is a
        number, while a date or a UUID is its text form. ``pg_stats`` is
        declared over ``anyarray`` and cannot be subscripted or aggregated, so
        a typed endpoint is not obtainable -- see the dialect's
        ``_histogram_projection``.
        """
        from .types import PostgresColumnDataProfile

        profiles: List[Any] = []
        for row in rows or []:
            column_name = row.get("column_name")
            if not column_name:
                continue

            row_count = row.get("row_count")
            if row_count is not None and row_count < 0:
                row_count = None
            elif row_count is not None:
                row_count = int(row_count)

            n_distinct = row.get("n_distinct")
            distinct_count: Optional[int] = None
            if n_distinct is not None:
                if n_distinct >= 0:
                    distinct_count = int(n_distinct)
                elif row_count:
                    # Negative means "this fraction of the rows are distinct".
                    distinct_count = int(round(-float(n_distinct) * row_count))

            null_fraction = row.get("null_fraction")
            if null_fraction is not None:
                null_fraction = float(null_fraction)
                if null_fraction < 0:
                    null_fraction = None

            most_common = self._stats_list(row.get("most_common_vals"))
            frequencies = [
                float(value)
                for value in self._stats_list(row.get("most_common_freqs"))
                if isinstance(value, (int, float))
            ]
            bounds = self._stats_list(row.get("histogram_bounds"))
            min_value, max_value = self._merged_bounds(
                bounds[0] if bounds else None,
                bounds[-1] if bounds else None,
                most_common,
            )

            collected_at = row.get("last_analyze") or row.get("last_autoanalyze")
            profiles.append(
                PostgresColumnDataProfile(
                    schema=row.get("schema_name") or schema,
                    table_name=table_name,
                    column_name=str(column_name),
                    null_fraction=null_fraction,
                    distinct_count=distinct_count,
                    min_value=min_value,
                    max_value=max_value,
                    row_count=row_count,
                    source=(
                        ProfileSource.ESTIMATE
                        if collected_at is not None
                        else ProfileSource.UNKNOWN
                    ),
                    collected_at=str(collected_at) if collected_at is not None else None,
                    correlation=(
                        float(row["correlation"])
                        if row.get("correlation") is not None
                        else None
                    ),
                    most_common_vals=most_common,
                    most_common_freqs=frequencies,
                )
            )
        return profiles

    def parse_column_value_samples(self, rows: List[Dict[str, Any]]) -> List[Any]:
        """Pair the two ``pg_stats`` arrays into value/frequency rows.

        Position is the pairing: ANALYZE writes the values and their shares in
        lockstep, so the i-th frequency belongs to the i-th value. A missing
        frequency is left ``None`` rather than guessed, and a value of ``NULL``
        is kept because NULL is a legitimate member of a column's domain.
        """
        from rhosocial.activerecord.backend.introspection.types import (
            ColumnValueFrequency,
        )

        found: List[Any] = []
        for row in rows or []:
            values = self._stats_list(row.get("most_common_vals"))
            frequencies = self._stats_list(row.get("most_common_freqs"))
            for position, value in enumerate(values):
                frequency = (
                    float(frequencies[position])
                    if position < len(frequencies)
                    and isinstance(frequencies[position], (int, float))
                    else None
                )
                found.append(
                    ColumnValueFrequency(value=value, frequency=frequency)
                )
        return found

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

    def materialized_view_list_statement(
        self, *, schema: Optional[str], include_system: bool
    ) -> "PostgresMaterializedViewListQueryStatement":
        """Return the statement that lists materialized views in one schema."""
        from ..expression.introspection import PostgresMaterializedViewListQueryStatement

        target_schema = schema if schema is not None else self.default_schema
        return PostgresMaterializedViewListQueryStatement(
            self.dialect, schema=target_schema, include_system=include_system
        )

    def materialized_view_info_statement(
        self, *, view_name: str, schema: Optional[str]
    ) -> "PostgresMaterializedViewInfoQueryStatement":
        """Return the statement that asks for one materialized view."""
        from ..expression.introspection import PostgresMaterializedViewInfoQueryStatement

        target_schema = schema if schema is not None else self.default_schema
        return PostgresMaterializedViewInfoQueryStatement(
            self.dialect, view_name, schema=target_schema
        )

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
        would have carried as absent.

        The version is :attr:`server_version`, which reads the cache
        ``introspect_and_adapt()`` keeps and falls back to the dialect without
        raising. An unknown version answers the narrower question: a server
        this backend never learned to speak to is not one to assume carries
        the newest catalog.
        """
        version = self.server_version
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
        if isinstance(cycle, bool):
            # ``pg_sequences.cycle`` is a boolean; information_schema's
            # ``cycle_option`` is the ``YES``/``NO`` text. Both spellings mean
            # the same fact, so the parser accepts either.
            cycle_value = cycle
        elif cycle is None:
            cycle_value = False
        else:
            cycle_value = str(cycle).strip().upper() == "YES"
        return SequenceInfo(
            name=row.get("sequence_name") or default_name,
            schema=row.get("schema_name") or default_schema,
            data_type=row.get("data_type") or "",
            start_value=row.get("start_value"),
            minimum_value=row.get("minimum_value"),
            maximum_value=row.get("maximum_value"),
            increment=row.get("increment"),
            cycle=cycle_value,
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

    def sequence_list_statement(
        self, *, schema: Optional[str], include_pg_sequences: bool
    ) -> "PostgresSequenceListQueryStatement":
        """Return the statement that lists the sequences in one schema."""
        from ..expression.introspection import PostgresSequenceListQueryStatement

        return PostgresSequenceListQueryStatement(
            self.dialect, schema=schema, include_pg_sequences=include_pg_sequences
        )

    def sequence_info_statement(
        self, *, sequence_name: str, schema: Optional[str], include_pg_sequences: bool
    ) -> "PostgresSequenceInfoQueryStatement":
        """Return the statement that asks for one sequence."""
        from ..expression.introspection import PostgresSequenceInfoQueryStatement

        return PostgresSequenceInfoQueryStatement(
            self.dialect,
            sequence_name,
            schema=schema,
            include_pg_sequences=include_pg_sequences,
        )

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
    # Schema-object families: partitions, routines, types, operators
    # ------------------------------------------------------------------ #

    #: ``FOR VALUES FROM (...) TO (...)`` on a RANGE partition.
    _RANGE_BOUND = re.compile(r"FOR VALUES FROM\s*\((.+)\)\s*TO\s*\((.+)\)", re.I | re.S)
    #: ``FOR VALUES IN (...)`` on a LIST partition.
    _LIST_BOUND = re.compile(r"FOR VALUES IN\s*\((.+)\)", re.I | re.S)
    #: ``FOR VALUES WITH (MODULUS n, REMAINDER r)`` on a HASH partition.
    _HASH_BOUND = re.compile(
        r"FOR VALUES WITH\s*\(\s*MODULUS\s+(\d+)\s*,\s*REMAINDER\s+(\d+)\s*\)",
        re.I,
    )
    #: ``RANGE (created_at)`` / ``LIST (status)`` / ``HASH (user_id)``.
    _PARTITION_KEY = re.compile(r"(RANGE|LIST|HASH)\s*\((.*)\)", re.I | re.S)

    @staticmethod
    def _split_top_level(text: str) -> List[str]:
        """Split a comma-separated SQL fragment outside parentheses/quotes."""
        parts: List[str] = []
        current: List[str] = []
        depth = 0
        quote: Optional[str] = None
        for char in text:
            if quote:
                current.append(char)
                if char == quote:
                    quote = None
            elif char in ("'", '"'):
                quote = char
                current.append(char)
            elif char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth = max(depth - 1, 0)
                current.append(char)
            elif char == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(char)
        if current:
            parts.append("".join(current).strip())
        return [part for part in parts if part]

    @staticmethod
    def _unquote_bound_value(value: str) -> str:
        """Strip the SQL quotes a bound literal carries, if it is quoted."""
        value = value.strip()
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            return value[1:-1].replace("''", "'")
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            return value[1:-1].replace('""', '"')
        return value

    @classmethod
    def _parse_partition_key(
        cls, key: Optional[str]
    ) -> Tuple[PostgresPartitionStrategy, List[str], Optional[str]]:
        """Split ``pg_get_partkeydef``'s text into strategy and key.

        ``RANGE (created_at)`` is a column list; ``RANGE (date_trunc(...))``
        is an expression. A key that is not one of those forms is still a
        partition, so the strategy falls back to ``UNKNOWN`` rather than
        raising.
        """
        if not key:
            return PostgresPartitionStrategy.UNKNOWN, [], None
        match = cls._PARTITION_KEY.search(str(key))
        if match is None:
            return PostgresPartitionStrategy.UNKNOWN, [], str(key)
        strategy = {
            "RANGE": PostgresPartitionStrategy.RANGE,
            "LIST": PostgresPartitionStrategy.LIST,
            "HASH": PostgresPartitionStrategy.HASH,
        }.get(match.group(1).upper(), PostgresPartitionStrategy.UNKNOWN)
        inner = match.group(2).strip()
        parts = cls._split_top_level(inner)
        simple = all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", part) for part in parts)
        if simple:
            return strategy, parts, None
        return strategy, [], inner or None

    @classmethod
    def _parse_partition_bound(cls, bound: Optional[str]) -> Optional[PostgresPartitionBound]:
        """Decode the one bound expression ``pg_get_expr(relpartbound)`` gives.

        The text is the engine's own spelling; the types are words, not rows,
        because a RANGE endpoint is a SQL literal in the catalog and cannot be
        safely converted without the column's type. ``text`` always carries
        the original so nothing is lost when a shape is not recognised.
        """
        if not bound:
            return None
        text = str(bound).strip()
        if text.upper() == "DEFAULT":
            return PostgresPartitionBound(text=text)
        match = cls._RANGE_BOUND.search(text)
        if match:
            return PostgresPartitionBound(
                lower=cls._unquote_bound_value(match.group(1)),
                upper=cls._unquote_bound_value(match.group(2)),
                text=text,
            )
        match = cls._LIST_BOUND.search(text)
        if match:
            return PostgresPartitionBound(
                values=[
                    cls._unquote_bound_value(value)
                    for value in cls._split_top_level(match.group(1))
                ],
                text=text,
            )
        match = cls._HASH_BOUND.search(text)
        if match:
            return PostgresPartitionBound(
                modulus=int(match.group(1)),
                remainder=int(match.group(2)),
                text=text,
            )
        return PostgresPartitionBound(text=text)

    def parse_partitions(
        self,
        rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
    ) -> List[PostgresPartitionInfo]:
        """Turn the parent/child rows into one record per child partition."""
        partitions: List[PostgresPartitionInfo] = []
        strategy = PostgresPartitionStrategy.UNKNOWN
        key_columns: List[str] = []
        key_expression: Optional[str] = None
        for row in rows or []:
            name = row.get("name")
            if not name:
                continue
            if not partitions:
                strategy, key_columns, key_expression = self._parse_partition_key(
                    row.get("partition_key")
                )
            row_count = row.get("row_count")
            if row_count is not None and row_count < 0:
                row_count = None
            elif row_count is not None:
                row_count = int(row_count)
            partitions.append(
                PostgresPartitionInfo(
                    table=row.get("parent_table") or table_name,
                    name=str(name),
                    schema=row.get("schema_name") or schema,
                    strategy=strategy,
                    key_columns=key_columns,
                    key_expression=key_expression,
                    parent=row.get("parent_table") or table_name,
                    bound=self._parse_partition_bound(row.get("bound")),
                    row_count=row_count,
                    size_bytes=row.get("size_bytes"),
                    comment=row.get("comment"),
                )
            )
        return partitions

    @staticmethod
    def parse_routine(row: Dict[str, Any], default_schema: str) -> PostgresRoutineInfo:
        """Build one routine record from a ``pg_proc`` row."""
        kind = {
            "f": PostgresRoutineKind.FUNCTION,
            "p": PostgresRoutineKind.PROCEDURE,
            "a": PostgresRoutineKind.AGGREGATE,
            "w": PostgresRoutineKind.WINDOW,
        }.get(row.get("kind"), PostgresRoutineKind.UNKNOWN)
        volatility = {
            "i": "immutable",
            "s": "stable",
            "v": "volatile",
        }.get(row.get("volatility"), row.get("volatility"))
        return PostgresRoutineInfo(
            name=row.get("name") or "",
            schema=row.get("schema_name") or default_schema,
            kind=kind,
            identity_arguments=row.get("identity_arguments"),
            result_type=row.get("result_type"),
            language=row.get("language"),
            volatility=volatility,
            security_definer=bool(row.get("security_definer")),
            comment=row.get("comment"),
        )

    def parse_routines(
        self, rows: List[Dict[str, Any]], schema: str
    ) -> List[PostgresRoutineInfo]:
        return [self.parse_routine(row, schema) for row in rows or []]

    @staticmethod
    def parse_types(rows: List[Dict[str, Any]], schema: str) -> List[PostgresTypeInfo]:
        """Group the type/label/field rows into one record per type.

        The statement joins ``pg_enum`` and ``pg_attribute`` to one type query;
        each type therefore answers with as many rows as it has labels *or*
        fields. Grouping by schema and name is what turns those rows back into
        the record a caller reads.
        """
        builders: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in rows or []:
            key = (row.get("schema_name") or schema, row.get("name") or "")
            builder = builders.get(key)
            if builder is None:
                builder = {"row": row, "labels": [], "fields": []}
                builders[key] = builder
            if row.get("label") is not None:
                builder["labels"].append((row.get("label_order") or 0, row["label"]))
            if row.get("field_name") is not None:
                builder["fields"].append((row.get("field_order") or 0, row["field_name"]))

        results: List[PostgresTypeInfo] = []
        for (type_schema, type_name), builder in builders.items():
            row = builder["row"]
            kind = {
                "e": PostgresTypeKind.ENUM,
                "d": PostgresTypeKind.DOMAIN,
                "c": PostgresTypeKind.COMPOSITE,
                "r": PostgresTypeKind.RANGE,
                "m": PostgresTypeKind.MULTIRANGE,
            }.get(row.get("type_code"), PostgresTypeKind.UNKNOWN)
            extra: Dict[str, Any] = {}
            if row.get("relation_kind") is not None:
                extra["relation_kind"] = row["relation_kind"]
            if row.get("relation_name") is not None:
                extra["relation_name"] = row["relation_name"]
            results.append(
                PostgresTypeInfo(
                    name=type_name,
                    schema=type_schema,
                    kind=kind,
                    labels=[label for _, label in sorted(builder["labels"])],
                    fields=[field for _, field in sorted(builder["fields"])],
                    base_type=row.get("base_type"),
                    domain_not_null=(
                        bool(row.get("domain_not_null"))
                        if row.get("domain_not_null") is not None
                        else None
                    ),
                    domain_default=row.get("domain_default"),
                    comment=row.get("comment"),
                    extra=extra,
                )
            )
        return results

    @staticmethod
    def parse_operator(row: Dict[str, Any], default_schema: str) -> PostgresOperatorInfo:
        """Build one operator record from a ``pg_operator`` row."""
        return PostgresOperatorInfo(
            name=row.get("name") or "",
            schema=row.get("schema_name") or default_schema,
            operator_kind={
                "b": "binary",
                "l": "prefix",
                "r": "postfix",
            }.get(row.get("operator_kind"), row.get("operator_kind")),
            left_type=row.get("left_type"),
            right_type=row.get("right_type"),
            result_type=row.get("result_type"),
            function_name=row.get("function_name"),
            owner=row.get("owner"),
            comment=row.get("comment"),
        )

    def parse_operators(
        self, rows: List[Dict[str, Any]], schema: str
    ) -> List[PostgresOperatorInfo]:
        return [self.parse_operator(row, schema) for row in rows or []]

    @staticmethod
    def parse_operator_class(
        row: Dict[str, Any], default_schema: str
    ) -> PostgresOperatorClassInfo:
        """Build one operator-class record from a ``pg_opclass`` row."""
        return PostgresOperatorClassInfo(
            name=row.get("name") or "",
            schema=row.get("schema_name") or default_schema,
            method=row.get("method"),
            input_type=row.get("input_type"),
            key_type=row.get("key_type"),
            is_default=bool(row.get("is_default")),
            family=row.get("family"),
            owner=row.get("owner"),
            comment=row.get("comment"),
        )

    def parse_operator_classes(
        self, rows: List[Dict[str, Any]], schema: str
    ) -> List[PostgresOperatorClassInfo]:
        return [self.parse_operator_class(row, schema) for row in rows or []]

    @staticmethod
    def parse_operator_family(
        row: Dict[str, Any], default_schema: str
    ) -> PostgresOperatorFamilyInfo:
        """Build one operator-family record from a ``pg_opfamily`` row."""
        return PostgresOperatorFamilyInfo(
            name=row.get("name") or "",
            schema=row.get("schema_name") or default_schema,
            method=row.get("method"),
            owner=row.get("owner"),
            comment=row.get("comment"),
        )

    def parse_operator_families(
        self, rows: List[Dict[str, Any]], schema: str
    ) -> List[PostgresOperatorFamilyInfo]:
        return [self.parse_operator_family(row, schema) for row in rows or []]

    @staticmethod
    def _schema_family_cache_key(
        cache,
        family: str,
        schema: Optional[str],
        name: Optional[str] = None,
        extra: Optional[str] = None,
    ) -> str:
        """One cache-key shape for the families that share ``SCHEMA`` scope."""
        parts = family if not extra else f"{family}:{extra}"
        return cache.key(
            IntrospectionScope.SCHEMA,
            name or "*",
            schema=schema,
            extra=parts,
        )

    def partition_list_statement(
        self,
        *,
        table_name: str,
        schema: Optional[str],
        include_partitions: bool = True,
        include_statistics: bool = True,
    ) -> "PostgresPartitionListQueryStatement":
        """Return the statement that lists one table's child partitions."""
        from ..expression.introspection import PostgresPartitionListQueryStatement

        return PostgresPartitionListQueryStatement(
            self.dialect,
            table_name,
            schema=schema,
            include_partitions=include_partitions,
            include_statistics=include_statistics,
        )

    def routine_list_statement(
        self, *, schema: Optional[str], routine_name: Optional[str] = None
    ) -> "PostgresRoutineListQueryStatement":
        """Return the statement that lists routines in one schema."""
        from ..expression.introspection import PostgresRoutineListQueryStatement

        return PostgresRoutineListQueryStatement(
            self.dialect, schema=schema, routine_name=routine_name
        )

    def type_list_statement(
        self, *, schema: Optional[str], type_name: Optional[str] = None
    ) -> "PostgresTypeListQueryStatement":
        """Return the statement that lists user types in one schema."""
        from ..expression.introspection import PostgresTypeListQueryStatement

        return PostgresTypeListQueryStatement(
            self.dialect, schema=schema, type_name=type_name
        )

    def operator_list_statement(
        self, *, schema: Optional[str], operator_name: Optional[str] = None
    ) -> "PostgresOperatorListQueryStatement":
        """Return the statement that lists operators in one schema."""
        from ..expression.introspection import PostgresOperatorListQueryStatement

        return PostgresOperatorListQueryStatement(
            self.dialect, schema=schema, operator_name=operator_name
        )

    def operator_class_list_statement(
        self, *, schema: Optional[str], operator_class_name: Optional[str] = None
    ) -> "PostgresOperatorClassListQueryStatement":
        """Return the statement that lists operator classes in one schema."""
        from ..expression.introspection import PostgresOperatorClassListQueryStatement

        return PostgresOperatorClassListQueryStatement(
            self.dialect, schema=schema, operator_class_name=operator_class_name
        )

    def operator_family_list_statement(
        self, *, schema: Optional[str], operator_family_name: Optional[str] = None
    ) -> "PostgresOperatorFamilyListQueryStatement":
        """Return the statement that lists operator families in one schema."""
        from ..expression.introspection import PostgresOperatorFamilyListQueryStatement

        return PostgresOperatorFamilyListQueryStatement(
            self.dialect, schema=schema, operator_family_name=operator_family_name
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

    def extension_list_statement(self) -> "PostgresExtensionListQuery":
        """Return the statement that lists the installed extensions."""
        from ..expression.status import PostgresExtensionListQuery

        return PostgresExtensionListQuery(self.dialect)

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


class PostgreSQLIntrospector(PostgreSQLIntrospectorMixin, AbstractIntrospector):
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
        executor: IntrospectorExecutor,
        cache: Optional[IntrospectionCache] = None,
    ) -> None:
        super().__init__(backend, executor, cache)
        self._status_instance: Optional[PostgreSQLStatusIntrospector] = None

    @property
    def status(self) -> PostgreSQLStatusIntrospector:
        """PostgreSQL status introspector (lazily created)."""
        if self._status_instance is None:
            self._status_instance = PostgreSQLStatusIntrospector(self._backend)
        return self._status_instance

    def sample_column_values(
        self,
        table_name: str,
        column_name: str,
        limit: int = VALUE_SAMPLE_LIMIT,
        schema: Optional[str] = None,
    ):
        """List a column's most frequent values, at most ``limit`` of them.

        The catalog hands back its whole retained list -- ANALYZE keeps at most
        ``statistics_target`` entries -- so the bound is applied here rather
        than in SQL, where unnesting two arrays to pair them would cost more
        than the slice. Returning fewer than ``limit`` is correct: the engine
        kept what it kept.
        """
        return super().sample_column_values(
            table_name, column_name, limit=limit, schema=schema
        )[:limit]

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
        sql, params = self.materialized_view_list_statement(
            schema=schema, include_system=include_system
        ).to_sql()
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
        sql, params = self.materialized_view_info_statement(
            view_name=view_name, schema=schema
        ).to_sql()
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
        sql, params = self.sequence_list_statement(
            schema=schema, include_pg_sequences=with_pg
        ).to_sql()
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
        sql, params = self.sequence_info_statement(
            sequence_name=sequence_name, schema=schema, include_pg_sequences=with_pg
        ).to_sql()
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
    # Partitions, routines, types, operators
    # ------------------------------------------------------------------ #

    def list_partitions(
        self,
        table_name: str,
        schema: Optional[str] = None,
        include_partitions: bool = True,
        include_statistics: bool = True,
    ) -> List[PostgresPartitionInfo]:
        """List one partitioned table's child partitions.

        Args:
            table_name: The partitioned parent table.
            schema: Schema to inspect. Defaults to the connection default schema.
            include_partitions: Whether to list the children (default True).
            include_statistics: Whether to fill each child's row/size (default True).

        Returns:
            One record per child partition, ordered by name. An empty list is
            a real answer: the table may be partitioned with no children yet.
        """
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "partitions", schema, name=table_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.partition_list_statement(
            table_name=table_name,
            schema=schema,
            include_partitions=include_partitions,
            include_statistics=include_statistics,
        ).to_sql()
        result = self.parse_partitions(self._executor.execute(sql, params), table_name, target)
        self._cache.set(key, result)
        return result

    def list_routines(
        self,
        schema: Optional[str] = None,
        routine_name: Optional[str] = None,
    ) -> List[PostgresRoutineInfo]:
        """List functions, procedures, aggregates and window functions.

        Args:
            schema: Schema to inspect. Defaults to the connection default schema.
            routine_name: Optional name to narrow the listing (overloads are kept).

        Returns:
            List of PostgresRoutineInfo, ordered by schema, name and OID.
        """
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "routines", schema, name=routine_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.routine_list_statement(
            schema=schema, routine_name=routine_name
        ).to_sql()
        result = self.parse_routines(self._executor.execute(sql, params), target)
        self._cache.set(key, result)
        return result

    def get_routine_info(
        self, routine_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresRoutineInfo]:
        """Return the first routine named ``routine_name``, or None."""
        return next(
            (
                routine
                for routine in self.list_routines(
                    schema=schema, routine_name=routine_name
                )
            ),
            None,
        )

    def list_types(
        self,
        schema: Optional[str] = None,
        type_name: Optional[str] = None,
    ) -> List[PostgresTypeInfo]:
        """List the schema's enum, domain, composite, range and multirange types.

        Args:
            schema: Schema to inspect. Defaults to the connection default schema.
            type_name: Optional name to narrow the listing.

        Returns:
            List of PostgresTypeInfo. A table's row type is included as a
            composite, with ``extra['relation_kind']`` naming the relation it
            belongs to.
        """
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "types", schema, name=type_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.type_list_statement(
            schema=schema, type_name=type_name
        ).to_sql()
        result = self.parse_types(self._executor.execute(sql, params), target)
        self._cache.set(key, result)
        return result

    def get_type_info(
        self, type_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresTypeInfo]:
        """Return the type named ``type_name``, or None."""
        return next(
            (
                type_info
                for type_info in self.list_types(schema=schema, type_name=type_name)
            ),
            None,
        )

    def list_operators(
        self,
        schema: Optional[str] = None,
        operator_name: Optional[str] = None,
    ) -> List[PostgresOperatorInfo]:
        """List the operators in one schema."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "operators", schema, name=operator_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.operator_list_statement(
            schema=schema, operator_name=operator_name
        ).to_sql()
        result = self.parse_operators(self._executor.execute(sql, params), target)
        self._cache.set(key, result)
        return result

    def get_operator_info(
        self, operator_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresOperatorInfo]:
        """Return the operator named ``operator_name``, or None."""
        return next(
            (
                operator
                for operator in self.list_operators(
                    schema=schema, operator_name=operator_name
                )
            ),
            None,
        )

    def list_operator_classes(
        self,
        schema: Optional[str] = None,
        operator_class_name: Optional[str] = None,
    ) -> List[PostgresOperatorClassInfo]:
        """List the operator classes in one schema."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "operator-classes", schema, name=operator_class_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.operator_class_list_statement(
            schema=schema, operator_class_name=operator_class_name
        ).to_sql()
        result = self.parse_operator_classes(self._executor.execute(sql, params), target)
        self._cache.set(key, result)
        return result

    def get_operator_class_info(
        self, operator_class_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresOperatorClassInfo]:
        """Return the operator class named ``operator_class_name``, or None."""
        return next(
            (
                operator_class
                for operator_class in self.list_operator_classes(
                    schema=schema, operator_class_name=operator_class_name
                )
            ),
            None,
        )

    def list_operator_families(
        self,
        schema: Optional[str] = None,
        operator_family_name: Optional[str] = None,
    ) -> List[PostgresOperatorFamilyInfo]:
        """List the operator families in one schema."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "operator-families", schema, name=operator_family_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.operator_family_list_statement(
            schema=schema, operator_family_name=operator_family_name
        ).to_sql()
        result = self.parse_operator_families(self._executor.execute(sql, params), target)
        self._cache.set(key, result)
        return result

    def get_operator_family_info(
        self, operator_family_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresOperatorFamilyInfo]:
        """Return the operator family named ``operator_family_name``, or None."""
        return next(
            (
                operator_family
                for operator_family in self.list_operator_families(
                    schema=schema, operator_family_name=operator_family_name
                )
            ),
            None,
        )

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
        sql, params = self.extension_list_statement().to_sql()
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
        sql, params = self.materialized_view_list_statement(
            schema=schema, include_system=include_system
        ).to_sql()
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
        sql, params = self.materialized_view_info_statement(
            view_name=view_name, schema=schema
        ).to_sql()
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
        sql, params = self.sequence_list_statement(
            schema=schema, include_pg_sequences=with_pg
        ).to_sql()
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
        sql, params = self.sequence_info_statement(
            sequence_name=sequence_name, schema=schema, include_pg_sequences=with_pg
        ).to_sql()
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
    # Partitions, routines, types, operators
    # ------------------------------------------------------------------ #

    async def list_partitions(
        self,
        table_name: str,
        schema: Optional[str] = None,
        include_partitions: bool = True,
        include_statistics: bool = True,
    ) -> List[PostgresPartitionInfo]:
        """List one partitioned table's child partitions (async counterpart)."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "partitions", schema, name=table_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.partition_list_statement(
            table_name=table_name,
            schema=schema,
            include_partitions=include_partitions,
            include_statistics=include_statistics,
        ).to_sql()
        rows = await self._executor.execute(sql, params)
        result = self.parse_partitions(rows, table_name, target)
        self._cache.set(key, result)
        return result

    async def list_routines(
        self,
        schema: Optional[str] = None,
        routine_name: Optional[str] = None,
    ) -> List[PostgresRoutineInfo]:
        """List PostgreSQL routines (async counterpart)."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "routines", schema, name=routine_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.routine_list_statement(
            schema=schema, routine_name=routine_name
        ).to_sql()
        rows = await self._executor.execute(sql, params)
        result = self.parse_routines(rows, target)
        self._cache.set(key, result)
        return result

    async def get_routine_info(
        self, routine_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresRoutineInfo]:
        """Return the first routine named ``routine_name``, or None (async)."""
        return next(
            iter(
                await self.list_routines(
                    schema=schema, routine_name=routine_name
                )
            ),
            None,
        )

    async def list_types(
        self,
        schema: Optional[str] = None,
        type_name: Optional[str] = None,
    ) -> List[PostgresTypeInfo]:
        """List PostgreSQL user types (async counterpart)."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "types", schema, name=type_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.type_list_statement(
            schema=schema, type_name=type_name
        ).to_sql()
        rows = await self._executor.execute(sql, params)
        result = self.parse_types(rows, target)
        self._cache.set(key, result)
        return result

    async def get_type_info(
        self, type_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresTypeInfo]:
        """Return the type named ``type_name``, or None (async)."""
        return next(
            iter(
                await self.list_types(schema=schema, type_name=type_name)
            ),
            None,
        )

    async def list_operators(
        self,
        schema: Optional[str] = None,
        operator_name: Optional[str] = None,
    ) -> List[PostgresOperatorInfo]:
        """List PostgreSQL operators (async counterpart)."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "operators", schema, name=operator_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.operator_list_statement(
            schema=schema, operator_name=operator_name
        ).to_sql()
        rows = await self._executor.execute(sql, params)
        result = self.parse_operators(rows, target)
        self._cache.set(key, result)
        return result

    async def get_operator_info(
        self, operator_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresOperatorInfo]:
        """Return the operator named ``operator_name``, or None (async)."""
        return next(
            iter(
                await self.list_operators(
                    schema=schema, operator_name=operator_name
                )
            ),
            None,
        )

    async def list_operator_classes(
        self,
        schema: Optional[str] = None,
        operator_class_name: Optional[str] = None,
    ) -> List[PostgresOperatorClassInfo]:
        """List PostgreSQL operator classes (async counterpart)."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "operator-classes", schema, name=operator_class_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.operator_class_list_statement(
            schema=schema, operator_class_name=operator_class_name
        ).to_sql()
        rows = await self._executor.execute(sql, params)
        result = self.parse_operator_classes(rows, target)
        self._cache.set(key, result)
        return result

    async def get_operator_class_info(
        self, operator_class_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresOperatorClassInfo]:
        """Return the named operator class, or None (async)."""
        return next(
            iter(
                await self.list_operator_classes(
                    schema=schema, operator_class_name=operator_class_name
                )
            ),
            None,
        )

    async def list_operator_families(
        self,
        schema: Optional[str] = None,
        operator_family_name: Optional[str] = None,
    ) -> List[PostgresOperatorFamilyInfo]:
        """List PostgreSQL operator families (async counterpart)."""
        target = schema if schema is not None else self.default_schema
        key = self._schema_family_cache_key(
            self._cache, "operator-families", schema, name=operator_family_name
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        sql, params = self.operator_family_list_statement(
            schema=schema, operator_family_name=operator_family_name
        ).to_sql()
        rows = await self._executor.execute(sql, params)
        result = self.parse_operator_families(rows, target)
        self._cache.set(key, result)
        return result

    async def get_operator_family_info(
        self, operator_family_name: str, schema: Optional[str] = None
    ) -> Optional[PostgresOperatorFamilyInfo]:
        """Return the named operator family, or None (async)."""
        return next(
            iter(
                await self.list_operator_families(
                    schema=schema, operator_family_name=operator_family_name
                )
            ),
            None,
        )

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
        sql, params = self.extension_list_statement().to_sql()
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

    async def sample_column_values(
        self,
        table_name: str,
        column_name: str,
        limit: int = VALUE_SAMPLE_LIMIT,
        schema: Optional[str] = None,
    ):
        """List a column's most frequent values, at most ``limit``; async twin.

        Same slice as the synchronous one, for the same reason: the retained
        list is fixed-size and the bound belongs after it arrives.
        """
        samples = await super().sample_column_values(
            table_name, column_name, limit=limit, schema=schema
        )
        return samples[:limit]
