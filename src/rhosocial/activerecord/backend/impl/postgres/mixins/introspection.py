# src/rhosocial/activerecord/backend/impl/postgres/mixins/introspection.py
"""
PostgreSQL introspection capability mixin for dialect layer.

This mixin declares introspection capabilities for PostgreSQL dialect.
It follows the two-layer introspection architecture:
- Dialect layer (this file): Declares what the database CAN do via supports_* methods
  and generates SQL via format_*_query methods
- Backend layer (introspection.py): Implements HOW to query via _query_* methods

PostgreSQL supports comprehensive introspection through pg_catalog system tables.

Key pg_catalog tables used:
- pg_database: Database information
- pg_class/pg_namespace: Table information
- pg_attribute: Column metadata
- pg_index: Index information
- pg_constraint: Foreign key constraints
- pg_trigger: Trigger information
- pg_proc: Function/procedure information

Reference: .claude/plan/db-introspection-guide.md
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.mixins import IntrospectionMixin
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.introspection import (
        ColumnDataProfileQueryStatement,
        ColumnValueSampleQueryStatement,
        DatabaseInfoQueryStatement,
        TableListQueryStatement,
        TableInfoQueryStatement,
        ColumnInfoQueryStatement,
        IndexInfoQueryStatement,
        ForeignKeyQueryStatement,
        JoinCoverageQueryStatement,
        ViewListQueryStatement,
        ViewInfoQueryStatement,
        TriggerListQueryStatement,
        TriggerInfoQueryStatement,
        TableUsageStatsQueryStatement,
        UnusedIndexesQueryStatement,
        RuntimeMetricsQueryStatement,
        IndexUsageStatsQueryStatement,
        ObjectDependencyQueryStatement,
        DDLStatementQueryStatement,
    )
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
        PostgresDatabaseSizeFunction,
        PostgresDatabaseSizeQueryStatement,
        PostgresIndexesSizeFunction,
        PostgresRelationSizeFunction,
        PostgresTableStorageQueryStatement,
    )
    from ..expression.status import (
        PostgresSettingsQuery,
        PostgresActivityCountQuery,
        PostgresDatabaseListQuery,
        PostgresSchemaStatsQuery,
        PostgresCurrentDatabaseQuery,
        PostgresRoleListQuery,
        PostgresCurrentUserQuery,
        PostgresCurrentSchemaQuery,
        PostgresClientAddressQuery,
        PostgresSslSessionQuery,
        PostgresWalPositionQuery,
        PostgresWalFileCountQuery,
        PostgresCheckpointCountQuery,
        PostgresCheckpointLsnQuery,
        PostgresRecoveryStateQuery,
        PostgresReplicationSlotCountQuery,
        PostgresActiveReplicationCountQuery,
        PostgresReplicationLagQuery,
        PostgresArchiveStatsQuery,
        PostgresExtensionListQuery,
    )
    from ..expression.enum_ import (
        PostgresEnumTypeExistsQuery,
        PostgresEnumTypeValuesQuery,
    )


class PostgresIntrospectionCapabilityMixin(IntrospectionMixin):
    """
    PostgreSQL introspection capability declaration for dialect layer.

    Declares that PostgreSQL supports all introspection features through
    its comprehensive pg_catalog system tables.

    This mixin is used by PostgresDialect to declare introspection capabilities.
    The actual implementation is in PostgreSQLIntrospectionMixin (backend layer).

    Dialects declare capabilities (supports_* methods) and generate SQL
    (format_*_query methods), but do not execute queries.
    """

    # ========== Capability Detection ==========

    def supports_introspection(self) -> bool:
        """PostgreSQL supports introspection."""
        return True

    def supports_database_info(self) -> bool:
        """PostgreSQL supports database info via pg_database."""
        return True

    def supports_table_introspection(self) -> bool:
        """PostgreSQL supports table introspection via pg_class."""
        return True

    def supports_column_introspection(self) -> bool:
        """PostgreSQL supports column introspection via pg_attribute."""
        return True

    def supports_index_introspection(self) -> bool:
        """PostgreSQL supports index introspection via pg_index."""
        return True

    def supports_foreign_key_introspection(self) -> bool:
        """PostgreSQL supports foreign key introspection via pg_constraint."""
        return True

    def supports_view_introspection(self) -> bool:
        """PostgreSQL supports view introspection via pg_views."""
        return True

    def supports_trigger_introspection(self) -> bool:
        """PostgreSQL supports trigger introspection via pg_trigger."""
        return True

    def supports_partition_introspection(self) -> bool:
        """PostgreSQL answers child partitions from ``pg_inherits``.

        Declarative partitioning arrived in PostgreSQL 10, which is also when
        the catalog began carrying ``pg_get_partkeydef`` and
        ``relpartbound`` for it. Before that a partitioned table cannot exist,
        so the family is absent rather than empty.
        """
        return self.supports_table_partitioning()

    def supports_partition_info(self) -> bool:
        """Core's name for the partition family, delegating to PostgreSQL's.

        Core declares the probe; this backend answers it. Kept as a delegation
        rather than a second ``True`` so both spellings are one fact.
        """
        return self.supports_partition_introspection()

    def supports_routine_introspection(self) -> bool:
        """PostgreSQL supports routine introspection via ``pg_proc``."""
        return True

    def supports_type_introspection(self) -> bool:
        """PostgreSQL supports type introspection via ``pg_type``/``pg_enum``."""
        return True

    def supports_operator_introspection(self) -> bool:
        """PostgreSQL supports operator introspection via ``pg_operator``."""
        return True

    def supports_operator_class_introspection(self) -> bool:
        """PostgreSQL supports operator-class introspection via ``pg_opclass``."""
        return True

    def supports_operator_family_introspection(self) -> bool:
        """PostgreSQL supports operator-family introspection via ``pg_opfamily``."""
        return True

    def supports_table_storage_introspection(self) -> bool:
        """PostgreSQL reports each table's storage split.

        PostgreSQL's storage is row-plus-index shaped, so the narrow slots of
        ``IntrospectionTableInfo`` can all be answered: ``size_bytes`` from
        ``pg_total_relation_size``, ``data_size_bytes`` from
        ``pg_relation_size`` and ``index_size_bytes`` from ``pg_indexes_size``.
        Declaring the family is what lets the table listing project the trio and
        what a CLI checks before offering it; core defaults the probe to False
        because an engine whose storage answers no part of that has nothing to
        say, and an unanswered family must stay unoffered rather than answered
        with an empty row.
        """
        return True

    def supports_extensions(self) -> bool:
        """The core name for the extension family PostgreSQL answers.

        Declared here rather than on the extension mixin because this is the
        introspection capability mixin: the cross-engine probes live together
        here, and `PostgresExtensionSupport` -- the protocol the extension
        mixin is paired with -- must not declare a method the core
        `IntrospectionSupport` already names, or one capability reads as two
        answers again.

        It delegates rather than re-declaring `True` so the answer is one
        fact: `supports_extension_introspection` on the extension mixin is
        where PostgreSQL's extensions are actually read from `pg_extension`,
        and a caller asking through either spelling gets the same reply.
        """
        return self.supports_extension_introspection()

    def supports_table_stats(self) -> bool:
        """PostgreSQL answers per-table usage counters from pg_stat_user_tables.

        The counters are the statistics collector's, reset on crash or by
        ``pg_stat_reset()`` -- so "since the last reset" is the only tense the
        numbers have, and a zero is "no access in the window". The view also
        carries facts with no common name (dead tuples, last-access times);
        those are real, but they are not projected here, and where they go is
        the backend reader's decision, not the common shape's.
        """
        return True


    def supports_runtime_stats(self) -> bool:
        """PostgreSQL answers runtime measurements from ``pg_stat_activity``.

        The counters cover everything since the statistics were last reset --
        crash or ``pg_stat_reset()`` -- so each number says "in the window the
        server remembers" and a zero is "nothing in that window", never
        "nothing ever". ``pg_stat_statements`` is deliberately *not* read
        here: it is an extension, and answering from a catalogue that is only
        sometimes installed would make the family depend on a module rather
        than on the engine.
        """
        return True

    def supports_index_stats(self) -> bool:
        """PostgreSQL answers per-index counters from ``pg_stat_user_indexes``.

        A stronger answer than the unused-indexes listing: ``idx_scan`` and
        the tuple counters, so "how much" is available and not only "ever".
        """
        return True

    def supports_object_dependencies(self) -> bool:
        """PostgreSQL answers the dependency graph from ``pg_depend``.

        The catalogue's own edges, filtered to the ones a reader acts on
        rather than the internal dependencies the extension machinery also
        records.
        """
        return True

    def supports_ddl_extraction(self) -> bool:
        """False: no DDL text is produced for an ordinary PostgreSQL table.

        ``pg_get_expr`` and ``pg_get_viewdef`` recover expressions, but a
        faithful ``CREATE TABLE`` needs storage parameters, tablespace and
        per-column clauses the catalog keeps only in parts. This backend does
        not reassemble them yet, so advertising the family would promise text
        the answer cannot produce. The probe stays False until a real
        assembly exists; a caller then gets a refusal rather than a successful
        run with nothing in it.
        """
        return False

    def supports_ddl_extraction_native(self) -> bool:
        """False: PostgreSQL has no single native DDL retrieval call.

        An assembly is offered instead (see :meth:`supports_ddl_extraction`)
        and says so through ``DDLStatement.native``. The two probes stay
        separate so a caller can tell "can retrieve" from "can retrieve
        natively" without reading a docstring.
        """
        return False

    def supports_unused_indexes_detection(self) -> bool:
        """PostgreSQL answers from ``pg_stat_user_indexes.idx_scan``.

        A zero counter means "no index scan since the statistics were last
        reset", which is PostgreSQL's tense for this family and a weaker
        claim than MySQL's ``sys.schema_unused_indexes``: an index untouched
        since a crash, or since a ``pg_stat_reset()``, reports zero. The
        answer is "worth reviewing", never "safe to drop".
        """
        return True


    def supports_column_data_profile(self) -> bool:
        """PostgreSQL answers the profile from ``pg_stats`` without reading rows.

        The cheapest profile of any engine here: ``null_frac``, ``n_distinct``
        and ``correlation`` are one row per column, so profiling a table costs
        one catalog query however large the table is. The price is that every
        figure is an *estimate* of the last ``ANALYZE``, which is why the
        parser labels the family ``ESTIMATE`` when the analysis time is
        recorded and ``UNKNOWN`` when it is not -- see its docstring. A table
        that was never analysed has no ``pg_stats`` row and therefore no
        profile: an honest empty answer rather than fabricated zeros.
        """
        return True


    def supports_column_value_sample(self) -> bool:
        """PostgreSQL answers it from the kept ``most_common_vals`` list.

        Also free, and also bounded: ANALYZE keeps at most ``statistics_target``
        values (100 by default), so a low-cardinality column's listing is
        usually complete while a high-cardinality one is a sample of the head.
        Fewer values than requested is the normal answer, not a truncated one.
        """
        return True


    def supports_join_coverage(self) -> bool:
        """PostgreSQL measures overlap by counting distinct values on both sides.

        The engine can read its own rows, so the probe answers ``True``: what
        is expensive is the *statement*, not the capability, and pricing the
        question into a ``False`` would claim PostgreSQL cannot answer at all.
        There is no catalogue shortcut here either -- ``pg_stats`` keeps a
        bounded, sampled most-common list per column, so an overlap computed
        from it would be a guess dressed as a measurement, while counting the
        values is exact.

        That exactness is paid for: the measurement is a double scan plus an
        intersection, which is why it belongs behind an explicit call and never
        in any automatic pass. The statement's docstring states the rest.
        """
        return True


    def get_supported_introspection_scopes(self) -> List[IntrospectionScope]:
        """Get list of supported introspection scopes."""
        return [
            IntrospectionScope.DATABASE,
            IntrospectionScope.TABLE,
            IntrospectionScope.COLUMN,
            IntrospectionScope.INDEX,
            IntrospectionScope.FOREIGN_KEY,
            IntrospectionScope.VIEW,
            IntrospectionScope.TRIGGER,
        ]

    # ========== Helper Methods ==========

    @property
    def default_schema(self) -> str:
        """The default schema name.

        PostgreSQL's default schema is 'public', but may be overridden by
        the connection config's `default_schema` or the first entry of
        `search_path`. Returns 'public' if neither is set.
        """
        config = getattr(self, "_config", None) or getattr(self, "_backend", None)
        cfg = config.config if hasattr(config, "config") else config
        if cfg is not None:
            default_schema = getattr(cfg, "default_schema", None)
            if default_schema:
                return str(default_schema)
            search_path = getattr(cfg, "search_path", None)
            if search_path:
                # search_path 形如 "broker", public → 取第一个有效 schema
                first = str(search_path).split(",")[0].strip().strip('"').strip("'")
                if first:
                    return first
        return "public"

    # ========== Query Formatting Methods ==========
    #
    # Every query below is an expression tree: a projection of Column /
    # FunctionCall / CaseExpression nodes, a relation or a chain of joins, a
    # conjunction of predicates, and an optional ordering. No formatter here
    # writes a clause, a keyword or a separator -- the core renderers do -- and
    # the relation is a Table object, so the pg_catalog names appear once, as
    # the unquoted identifiers they are.
    #
    # Bind values keep the split the concatenated queries had: a caller's
    # schema or object name is data and travels as ``%s``, while a value that
    # is a fact about the catalog (a relkind code, a table-type label, the
    # ``pg_`` prefix filter) is literal-inline and binds nothing.

    #: The label each ``pg_class.relkind`` code reports as ``table_type``.
    #: A catalog fact rather than caller data, so it is written inline.
    _RELKIND_LABELS = (
        ("r", "BASE TABLE"),
        ("v", "VIEW"),
        ("m", "MATERIALIZED VIEW"),
        ("f", "FOREIGN TABLE"),
        ("p", "PARTITIONED TABLE"),
    )

    #: The relkind codes a table listing may be narrowed to, by the type the
    #: caller asked for. One entry per label the catalog reports: a partitioned
    #: table is now its own answer, so it is no longer folded into a base
    #: table. The keys are the labels ``_RELKIND_LABELS`` writes, because the
    #: filter is asked for in the same words the listing reports.
    _RELKIND_FILTERS = {
        "BASE TABLE": ("r",),
        "VIEW": ("v",),
        "MATERIALIZED VIEW": ("m",),
        "FOREIGN TABLE": ("f",),
        "PARTITIONED TABLE": ("p",),
    }

    #: Catalog relations whose names are spelled ``pg_``; a listing that does
    #: not ask for system objects excludes them. The percent is doubled because
    #: psycopg reads a bare ``%`` as the start of a placeholder even when the
    #: statement binds nothing.
    _SYSTEM_PREFIX = "pg_%%"

    #: The schemas PostgreSQL owns: the catalog itself, the standard views, and
    #: the out-of-line storage. A relation in one of them belongs to the server,
    #: not to the database being described.
    _SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")

    #: Temporary namespaces are per session and carry a suffix (``pg_temp_3``),
    #: so they are matched by prefix. Doubled like ``_SYSTEM_PREFIX``: psycopg
    #: reads a bare ``%`` as a placeholder start even when nothing is bound.
    _TEMP_SCHEMA_PREFIX = "pg_temp%%"

    def _catalog_column(self, name: str, table: "Optional[str]" = None, alias: "Optional[str]" = None):
        """A column reference into pg_catalog, quoted as it is written there.

        The catalog's identifiers are lowercase and never quoted, so a quoted
        *reference* would be a different (and missing) column; the *alias* is
        this query's own output name and is quoted like every other projection
        alias the core renderers write.
        """
        from rhosocial.activerecord.backend.expression import Column

        return Column(
            self,
            name,
            table=table,
            alias=alias,
            name_need_quote=False,
            table_need_quote=False,
        )

    def _catalog_relation(
        self,
        name: str,
        alias: "Optional[str]" = None,
        schema: "Optional[str]" = None,
        schema_need_quote: bool = False,
    ):
        """A catalog relation as a FROM source, with its alias.

        ``schema`` qualifies the name for the relations that live outside
        ``pg_catalog`` -- ``information_schema.sequences`` is the one this
        module reads. Neither part is quoted by default: the catalog spells
        both lower-case and never quoted, so quoting would name something
        else.
        """
        from rhosocial.activerecord.backend.expression.objects import Table
        from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

        return NamedRelationRef(
            self,
            Table(
                self,
                name,
                schema_name=schema,
                schema_need_quote=schema_need_quote,
                name_need_quote=False,
            ),
            alias=alias,
            alias_need_quote=False,
        )

    def _catalog_literal(self, value):
        """A catalog constant: written into the statement, never bound."""
        from rhosocial.activerecord.backend.expression import Literal

        return Literal(self, value, inline=True)

    def _catalog_value(self, value):
        """Caller data: bound, so a name can never become part of the text."""
        from rhosocial.activerecord.backend.expression import Literal

        return Literal(self, value)

    def _catalog_select(
        self,
        select: "list",
        source,
        conditions: "Optional[list]" = None,
        order_by: "Optional[list]" = None,
        group_by: "Optional[list]" = None,
    ) -> Tuple[str, tuple]:
        """Assemble a catalog SELECT from its parts, once.

        Args:
            select: The projection, in order.
            source: The FROM source: a relation or a join chain.
            conditions: Predicates to AND together, or ``None``.
            order_by: Sort expressions, or ``None`` for no ORDER BY.
            group_by: Grouping expressions, or ``None`` for no GROUP BY.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            LogicalPredicate,
            OrderByClause,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.query_parts import GroupByHavingClause

        kwargs: "dict" = {"select": select, "from_": source}
        if conditions:
            kwargs["where"] = WhereClause(self, LogicalPredicate(self, "AND", *conditions))
        if group_by:
            kwargs["group_by_having"] = GroupByHavingClause(self, group_by=group_by)
        if order_by:
            kwargs["order_by"] = OrderByClause(self, order_by)
        return QueryStatement(self, **kwargs).to_sql()

    def _relkind_case(self, alias: str):
        """The CASE that names a relation's kind, shared by the table listings."""
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression

        return CaseExpression(
            self,
            value=self._catalog_column("relkind", "c"),
            cases=[
                (self._catalog_literal(code), self._catalog_literal(label))
                for code, label in self._RELKIND_LABELS
            ],
            alias=alias,
        )

    def _relation_size_call(self, function_name: str, alias: str):
        """One of the three size calls, each measured on the relation's own OID.

        ``pg_total_relation_size`` reports the total footprint;
        ``pg_relation_size`` and ``pg_indexes_size`` report the heap and the
        index side of it -- the split the total cannot separate, which is why
        all three are asked for rather than derived from one. The argument is
        the same column in all three: the ``oid`` of the row being read.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall

        return FunctionCall(self, function_name, self._catalog_column("oid", "c"), alias=alias)

    def _relation_projection(self) -> "list":
        """The columns a table listing and a single-table read both report.

        Three of them are the storage trio: ``size_bytes`` (the total, from
        ``pg_total_relation_size``) with ``data_size_bytes`` and
        ``index_size_bytes`` beside it. They are here because
        :meth:`supports_table_storage_introspection` answers True for this
        engine -- PostgreSQL's storage is row-plus-index shaped, so the narrow
        slots of ``IntrospectionTableInfo`` carry the split instead of staying
        empty.

        The row count travels as two signals, not one. ``reltuples`` is the
        planner's estimate of the relation, maintained by ANALYZE; ``n_live_tup``
        is the statistics view's estimate of its live rows, maintained by the
        stats collector. The two go stale by different routes and at different
        times -- a freshly loaded table has neither, a table ANALYZEd long ago
        may have a reliable ``reltuples`` and a decayed ``n_live_tup``, and a
        table that has only ever been written through DML may have the reverse
        -- so the projection carries both and the parser chooses. Both are cast
        to ``bigint``, the type each is already stored as: the row shape states
        it rather than leaving the driver to decide.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall
        from rhosocial.activerecord.backend.expression.types import BigIntType

        return [
            self._catalog_column("relname", "c", alias="table_name"),
            self._relkind_case("table_type"),
            FunctionCall(
                self, "obj_description", self._catalog_column("oid", "c"), alias="comment"
            ),
            self._catalog_column("reltuples", "c").cast(BigIntType(self)).as_("row_count"),
            self._catalog_column("n_live_tup", "s").cast(BigIntType(self)).as_("live_row_count"),
            self._relation_size_call("pg_total_relation_size", "size_bytes"),
            self._relation_size_call("pg_relation_size", "data_size_bytes"),
            self._relation_size_call("pg_indexes_size", "index_size_bytes"),
        ]

    def _pg_class_join(self):
        """``pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid``."""
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, JoinClause

        return JoinClause(
            self,
            left_table=self._catalog_relation("pg_class", "c"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )

    def _relation_source(self):
        """``pg_class c JOIN pg_namespace n ... LEFT JOIN pg_stat_user_tables s ...``.

        The statistics join is what makes ``n_live_tup`` readable beside the
        planner's ``reltuples``. It is a LEFT JOIN because that view lists only
        the relations the stats collector has a row for: a relation it does not
        list must still be reported, with the live-row column absent rather than
        the whole row missing.
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        return self._pg_class_join().join(
            self._catalog_relation("pg_stat_user_tables", "s"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relid", "s"),
                self._catalog_column("oid", "c"),
            ),
        )

    def _table_count_projection(self):
        """``table_count``: how many tables this database declares.

        The count is a scalar subquery in the database-info projection, so the
        answer stays one round trip.

        What it asks for is the catalog's own definition of a table, not the
        listing's: ordinary and partitioned tables (``relkind`` ``'r'`` and
        ``'p'``) are the kinds that are tables, and asking for those two codes
        excludes views, materialized views and foreign tables by construction
        rather than by subtracting them afterwards. The namespace test is what
        makes the answer the *database's*: the catalog, the standard views and
        the toast storage are PostgreSQL's, and each session's temporary
        namespace is the session's, whichever schema the connection defaults
        to. Every other schema in the database counts, so this is the
        whole-database scope the SQLite backend answers, not one schema's.
        """
        from rhosocial.activerecord.backend.expression import (
            LikeOperator,
            LogicalPredicate,
            QueryStatement,
            RowCountWildcard,
            Subquery,
        )
        from rhosocial.activerecord.backend.expression.functions import CountFunction
        from rhosocial.activerecord.backend.expression.predicates import (
            InPredicate,
            LikePredicate,
        )
        from rhosocial.activerecord.backend.expression.query_parts import WhereClause

        return Subquery(
            self,
            QueryStatement(
                self,
                select=[CountFunction(self, RowCountWildcard(self))],
                from_=self._pg_class_join(),
                where=WhereClause(
                    self,
                    LogicalPredicate(
                        self,
                        "AND",
                        InPredicate(
                            self,
                            self._catalog_column("relkind", "c"),
                            self._catalog_literal(("r", "p")),
                        ),
                        LogicalPredicate(
                            self,
                            "NOT",
                            InPredicate(
                                self,
                                self._catalog_column("nspname", "n"),
                                self._catalog_literal(self._SYSTEM_SCHEMAS),
                            ),
                        ),
                        LikePredicate(
                            self,
                            LikeOperator.NOT_LIKE,
                            self._catalog_column("nspname", "n"),
                            self._catalog_literal(self._TEMP_SCHEMA_PREFIX),
                        ),
                    ),
                ),
            ),
            alias="table_count",
        )

    def format_database_info_query_statement(self, expr: "DatabaseInfoQueryStatement") -> Tuple[str, tuple]:
        """Format database information query.

        Uses pg_database and version() to get database metadata: the row the
        server answers with describes the database this session is connected
        to, so the WHERE clause names ``current_database()`` on both sides.

        ``table_count`` is "how many tables this database declares, system ones
        excluded": ordinary and partitioned tables in any schema the database
        owns, with the server's own schemas (``pg_catalog``,
        ``information_schema``, ``pg_toast`` and the ``pg_temp*`` family) left
        out. Views, materialized views and foreign tables are not tables. The
        count travels as a scalar subquery in this projection, which keeps the
        answer to a single round trip.

        Args:
            expr: Database info expression with parameters.

        Returns:
            Tuple of (SQL string, empty parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, FunctionCall

        current_database = FunctionCall(self, "current_database")
        return self._catalog_select(
            [
                FunctionCall(self, "version", alias="version"),
                FunctionCall(self, "current_database", alias="name"),
                FunctionCall(
                    self, "pg_encoding_to_char", self._catalog_column("encoding"), alias="encoding"
                ),
                self._catalog_column("datcollate", alias="collation"),
                FunctionCall(self, "pg_database_size", current_database, alias="size"),
                self._table_count_projection(),
            ],
            self._catalog_relation("pg_database"),
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("datname"), current_database
                )
            ],
        )

    def format_table_list_query_statement(self, expr: "TableListQueryStatement") -> Tuple[str, tuple]:
        """Format table list query using pg_catalog.

        Uses pg_class and pg_namespace to query table information.
        The relkind field distinguishes table types:
        - 'r': Ordinary table (BASE TABLE)
        - 'v': View
        - 'm': Materialized view
        - 'f': Foreign table
        - 'p': Partitioned table

        The relkind-IN filter and the ``table_type`` narrowing are separate
        conditions, exactly as the concatenated form spelled them: the first
        says which kinds a listing may contain at all, the second narrows to the
        one the caller asked for, and the two stay visible here rather than
        folded into one expression the caller would have to reconstruct.

        Args:
            expr: Table list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            InPredicate,
            LikeOperator,
            LikePredicate,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_views = params.get("include_views", True)
        include_system = params.get("include_system", False)
        table_type = params.get("table_type")

        relkind_filter = ("r", "v", "m", "f", "p") if include_views else ("r", "f", "p")
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            InPredicate(
                self,
                self._catalog_column("relkind", "c"),
                self._catalog_literal(relkind_filter),
            ),
        ]

        if not include_system:
            conditions.append(
                LikePredicate(
                    self,
                    LikeOperator.NOT_LIKE,
                    self._catalog_column("relname", "c"),
                    self._catalog_literal(self._SYSTEM_PREFIX),
                )
            )

        relkind_narrowing = self._RELKIND_FILTERS.get(table_type)
        if relkind_narrowing is not None:
            # One code narrows by equality, several by membership -- the same
            # split the concatenated branches wrote (``IN ('r', 'p')`` for a
            # base table, ``= 'v'`` for a view).
            if len(relkind_narrowing) == 1:
                conditions.append(
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("relkind", "c"),
                        self._catalog_literal(relkind_narrowing[0]),
                    )
                )
            else:
                conditions.append(
                    InPredicate(
                        self,
                        self._catalog_column("relkind", "c"),
                        self._catalog_literal(relkind_narrowing),
                    )
                )

        return self._catalog_select(
            self._relation_projection(),
            self._relation_source(),
            conditions,
            [self._catalog_column("relname", "c")],
        )

    def format_table_info_query_statement(self, expr: "TableInfoQueryStatement") -> Tuple[str, tuple]:
        """Format single table information query.

        Query pg_class for table details.

        Args:
            expr: Table info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self.default_schema

        return self._catalog_select(
            self._relation_projection(),
            self._relation_source(),
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(table_name),
                ),
            ],
        )

    def format_column_info_query_statement(self, expr: "ColumnInfoQueryStatement") -> Tuple[str, tuple]:
        """Format column information query using pg_attribute.

        Uses pg_attribute, pg_attrdef, and pg_class to query column metadata.
        The attnum > 0 condition excludes system columns (ctid, oid, etc.).
        The attisdropped = false condition excludes dropped columns.

        ``is_primary_key`` is an EXISTS subquery, and it is a node: the nested
        QueryStatement is what the predicate holds, so the primary-key test is
        inspectable rather than a parenthesised string. ``a.attnum = ANY(...)``
        is a quantified comparison against another column, which is exactly
        :class:`~...expression.advanced_functions.AnyPredicate`'s shape.

        Args:
            expr: Column info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
            LogicalPredicate,
            QueryStatement,
            Subquery,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import (
            AnyPredicate,
            ExistsPredicate,
        )

        params = expr.get_params()
        table_name = params.get("table_name", "")
        include_hidden = params.get("include_hidden", False)
        schema = params.get("schema") or self.default_schema

        # pg_index row for the primary-key test, over the column's own table.
        primary_key_index = QueryStatement(
            self,
            select=[self._catalog_literal(1)],
            from_=self._catalog_relation("pg_index", "i"),
            where=WhereClause(
                self,
                LogicalPredicate(
                    self,
                    "AND",
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("indrelid", "i"),
                        self._catalog_column("attrelid", "a"),
                    ),
                    self._catalog_column("indisprimary", "i"),
                    AnyPredicate(
                        self,
                        self._catalog_column("attnum", "a"),
                        "=",
                        Subquery(self, self._catalog_column("indkey", "i")),
                    ),
                ),
            ),
        )

        projection = [
            self._catalog_column("attname", "a", alias="column_name"),
            self._catalog_column("attnum", "a", alias="ordinal_position"),
            FunctionCall(
                self,
                "pg_catalog.format_type",
                self._catalog_column("atttypid", "a"),
                self._catalog_column("atttypmod", "a"),
                alias="data_type",
            ),
            # ``NOT a.attnotnull``: a truth test on one column, aliased into the
            # projection. The NOT is the logical node, so the negation is a
            # node rather than an operator glued to the front of a string.
            LogicalPredicate(self, "NOT", self._catalog_column("attnotnull", "a")).as_(
                "is_nullable"
            ),
            FunctionCall(
                self,
                "pg_get_expr",
                self._catalog_column("adbin", "d"),
                self._catalog_column("adrelid", "d"),
                alias="default_value",
            ),
            ExistsPredicate(self, primary_key_index).as_("is_primary_key"),
            FunctionCall(
                self,
                "col_description",
                self._catalog_column("attrelid", "a"),
                self._catalog_column("attnum", "a"),
                alias="comment",
            ),
            # The four shape attributes are not columns of pg_attribute: they
            # are what information_schema.columns derives from atttypmod and
            # the type. Reading them from that view keeps the derivation the
            # server's rather than a second one written here, and the LEFT JOIN
            # keeps a relation the view does not list (a materialized view, for
            # one) in the answer with those attributes absent.
            self._catalog_column(
                "character_maximum_length", "ic", alias="character_maximum_length"
            ),
            self._catalog_column("numeric_precision", "ic", alias="numeric_precision"),
            self._catalog_column("numeric_scale", "ic", alias="numeric_scale"),
            self._catalog_column("datetime_precision", "ic", alias="datetime_precision"),
        ]
        # Identity columns arrived in PostgreSQL 10 and generated columns in
        # 12. A 9.x server has neither catalog column, and asking for it would
        # fail the whole query; the columns are therefore projected only when
        # the server in front of us has them. The parser reads an absent
        # column as "no identity/generation" -- which on those releases is the
        # truth, because the feature does not exist there.
        if self.version_at_least((10, 0, 0)):
            projection.append(
                self._catalog_column("attidentity", "a", alias="identity_kind")
            )
        if self.version_at_least((12, 0, 0)):
            projection.append(
                self._catalog_column("attgenerated", "a", alias="generated_kind")
            )

        attribute_join = JoinClause(
            self,
            left_table=self._catalog_relation("pg_attribute", "a"),
            right_table=self._catalog_relation("pg_attrdef", "d"),
            join_type="LEFT",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "a"),
                    self._catalog_column("adrelid", "d"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attnum", "a"),
                    self._catalog_column("adnum", "d"),
                ),
            ),
        )
        source = attribute_join.join(
            self._catalog_relation("pg_class", "c"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("attrelid", "a"),
                self._catalog_column("oid", "c"),
            ),
        ).join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        ).join(
            # The shape attributes travel with the column's own row: schema,
            # relation and column name are the view's key, so all three pairs
            # are compared rather than the name alone.
            self._catalog_relation("columns", "ic", schema="information_schema"),
            join_type="LEFT",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("table_schema", "ic"),
                    self._catalog_column("nspname", "n"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("table_name", "ic"),
                    self._catalog_column("relname", "c"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("column_name", "ic"),
                    self._catalog_column("attname", "a"),
                ),
            ),
        )

        # The base conditions exclude dropped columns and system columns; the
        # attisdropped repeat below is what this query has always rendered for
        # a caller who did not ask for hidden columns, and it is kept as a
        # second condition so the bytes (and the parameters) stay as they were.
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relname", "c"),
                self._catalog_value(table_name),
            ),
            ComparisonPredicate(
                self,
                "gt",
                self._catalog_column("attnum", "a"),
                self._catalog_literal(0),
            ),
            LogicalPredicate(self, "NOT", self._catalog_column("attisdropped", "a")),
        ]
        if not include_hidden:
            conditions.append(
                LogicalPredicate(self, "NOT", self._catalog_column("attisdropped", "a"))
            )

        return self._catalog_select(
            projection, source, conditions, [self._catalog_column("attnum", "a")]
        )

    def format_index_info_query_statement(self, expr: "IndexInfoQueryStatement") -> Tuple[str, tuple]:
        """Format index information query using pg_index.

        Uses pg_index, pg_class, pg_am, and pg_attribute to query index metadata.
        The indkey array contains column positions for the index.
        The amname field indicates the index type (btree, hash, gin, gist, etc.).

        ``a.attnum = ANY(ix.indkey)`` is the join condition that walks an index's
        column list, and it is an :class:`AnyPredicate` over the array column --
        the same node the column query uses for the primary-key test.

        Args:
            expr: Index info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
            LogicalPredicate,
            Subquery,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import AnyPredicate

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self.default_schema

        column_position = FunctionCall(
            self,
            "array_position",
            self._catalog_column("indkey", "ix"),
            self._catalog_column("attnum", "a"),
            alias="column_position",
        )
        projection = [
            self._catalog_column("relname", "i", alias="index_name"),
            self._catalog_column("attname", "a", alias="column_name"),
            column_position,
            self._catalog_column("indisunique", "ix", alias="is_unique"),
            self._catalog_column("indisprimary", "ix", alias="is_primary"),
            self._catalog_column("amname", "am", alias="index_type"),
            self._catalog_column("indpred", "ix", alias="filter_condition"),
        ]

        # The join chain is the order the concatenated FROM clause wrote, so the
        # rendered statement reads the same way from left to right.
        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_index", "ix"),
            right_table=self._catalog_relation("pg_class", "t"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("indrelid", "ix"),
                self._catalog_column("oid", "t"),
            ),
        )
        for relation, alias, left_side, right_side in (
            ("pg_class", "i", ("indexrelid", "ix"), ("oid", "i")),
            ("pg_namespace", "n", ("relnamespace", "t"), ("oid", "n")),
            ("pg_am", "am", ("relam", "i"), ("oid", "am")),
        ):
            source = source.join(
                self._catalog_relation(relation, alias),
                join_type="JOIN",
                condition=ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column(*left_side),
                    self._catalog_column(*right_side),
                ),
            )

        source = source.join(
            self._catalog_relation("pg_attribute", "a"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "a"),
                    self._catalog_column("oid", "t"),
                ),
                AnyPredicate(
                    self,
                    self._catalog_column("attnum", "a"),
                    "=",
                    Subquery(self, self._catalog_column("indkey", "ix")),
                ),
            ),
        )

        return self._catalog_select(
            projection,
            source,
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "t"),
                    self._catalog_value(table_name),
                ),
            ],
            # ``column_position`` names the projection's alias, exactly as the
            # concatenated form ordered by it.
            [
                self._catalog_column("relname", "i"),
                self._catalog_column("column_position"),
            ],
        )

    def format_foreign_key_query_statement(self, expr: "ForeignKeyQueryStatement") -> Tuple[str, tuple]:
        """Format foreign key information query using pg_constraint.

        Uses pg_constraint, pg_class, pg_attribute, and pg_namespace to query
        foreign key metadata. The contype = 'f' filters for foreign key constraints.

        Referential action codes (confdeltype/confupdtype):
        - 'a': NO ACTION
        - 'r': RESTRICT
        - 'c': CASCADE
        - 'n': SET NULL
        - 'd': SET DEFAULT

        The referenced column is picked by a subscript on ``con.confkey``: the
        position of this side's column inside ``conkey`` indexes the other side's
        list. That subscript is an :class:`ArrayOperation`, so the pairing is a
        structure rather than a bracketed fragment of text.

        Args:
            expr: Foreign key expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
            LogicalPredicate,
            Subquery,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import (
            AnyPredicate,
            ArrayOperation,
        )
        from rhosocial.activerecord.backend.expression.types import TextType

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self.default_schema

        column_position = FunctionCall(
            self,
            "array_position",
            self._catalog_column("conkey", "con"),
            self._catalog_column("attnum", "a"),
            alias="column_position",
        )
        referenced_key = ArrayOperation(
            self,
            operation="ACCESS",
            base_expr=self._catalog_column("confkey", "con"),
            index_expr=FunctionCall(
                self,
                "array_position",
                self._catalog_column("conkey", "con"),
                self._catalog_column("attnum", "a"),
            ),
        )

        projection = [
            self._catalog_column("conname", "con", alias="constraint_name"),
            self._catalog_column("attname", "a", alias="column_name"),
            self._catalog_column("relname", "ref_class", alias="referenced_table"),
            self._catalog_column("nspname", "ref_ns", alias="referenced_schema"),
            self._catalog_column("attname", "ref_a", alias="referenced_column"),
            self._catalog_column("confdeltype", "con").cast(TextType(self)).as_("on_delete"),
            self._catalog_column("confupdtype", "con").cast(TextType(self)).as_("on_update"),
            column_position,
        ]

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_constraint", "con"),
            right_table=self._catalog_relation("pg_class", "class"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("conrelid", "con"),
                self._catalog_column("oid", "class"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_namespace", "ns"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "class"),
                self._catalog_column("oid", "ns"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_attribute", "a"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "a"),
                    self._catalog_column("oid", "class"),
                ),
                AnyPredicate(
                    self,
                    self._catalog_column("attnum", "a"),
                    "=",
                    Subquery(self, self._catalog_column("conkey", "con")),
                ),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_class", "ref_class"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("confrelid", "con"),
                self._catalog_column("oid", "ref_class"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_namespace", "ref_ns"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "ref_class"),
                self._catalog_column("oid", "ref_ns"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_attribute", "ref_a"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "ref_a"),
                    self._catalog_column("oid", "ref_class"),
                ),
                ComparisonPredicate(
                    self, "eq", self._catalog_column("attnum", "ref_a"), referenced_key
                ),
            ),
        )

        return self._catalog_select(
            projection,
            source,
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("contype", "con"),
                    self._catalog_literal("f"),
                ),
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "ns"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "class"),
                    self._catalog_value(table_name),
                ),
            ],
            [
                self._catalog_column("conname", "con"),
                self._catalog_column("column_position"),
            ],
        )

    def format_view_list_query_statement(self, expr: "ViewListQueryStatement") -> Tuple[str, tuple]:
        """Format view list query using pg_class.

        Uses pg_class with relkind = 'v' to query view information.
        pg_get_viewdef() returns the view definition SQL.

        Args:
            expr: View list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            LikeOperator,
            LikePredicate,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_system = params.get("include_system", False)

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relkind", "c"),
                self._catalog_literal("v"),
            ),
        ]
        if not include_system:
            conditions.append(
                LikePredicate(
                    self,
                    LikeOperator.NOT_LIKE,
                    self._catalog_column("relname", "c"),
                    self._catalog_literal(self._SYSTEM_PREFIX),
                )
            )

        return self._catalog_select(
            self._view_projection(),
            self._pg_class_join(),
            conditions,
            [self._catalog_column("relname", "c")],
        )

    def _view_projection(self) -> "list":
        """The three columns a view listing and a single-view read both report."""
        from rhosocial.activerecord.backend.expression import FunctionCall

        return [
            self._catalog_column("relname", "c", alias="view_name"),
            FunctionCall(
                self,
                "pg_get_viewdef",
                self._catalog_column("oid", "c"),
                self._catalog_literal(True),
                alias="definition",
            ),
            FunctionCall(
                self, "obj_description", self._catalog_column("oid", "c"), alias="comment"
            ),
        ]

    def format_view_info_query_statement(self, expr: "ViewInfoQueryStatement") -> Tuple[str, tuple]:
        """Format single view information query.

        Query pg_class for view details.

        Args:
            expr: View info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        view_name = params.get("view_name", "")
        schema = params.get("schema") or self.default_schema

        return self._catalog_select(
            self._view_projection(),
            self._pg_class_join(),
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(view_name),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relkind", "c"),
                    self._catalog_literal("v"),
                ),
            ],
        )

    def _matview_source(self):
        """``pg_matviews m JOIN pg_class c ... JOIN pg_namespace n ...``.

        The ``pg_class`` pair carries two comparisons: the name, and the
        namespace it resolves in. The namespace half is what makes this a
        correct read rather than a lucky one -- ``relname`` is unique per
        namespace, not per database, so pairing on the name alone lets a
        same-named row in *any* schema stand in for the one asked about. When
        that row had just been dropped by a concurrent worker the server
        refuses the query with "could not open relation with OID", which is
        what the parallelised suite reported.

        The ``pg_namespace`` join keeps its own two comparisons: the OID pair
        ties the relation to its namespace, and the name pair re-checks the
        schema ``pg_matviews`` reports against it.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
            LogicalPredicate,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import (
            ExistsPredicate,
        )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_matviews", "m"),
            right_table=self._catalog_relation("pg_class", "c"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_column("matviewname", "m"),
                ),
                # c.relnamespace == (the namespace whose name is m.schemaname).
                # Written as a scalar subquery because pg_class carries no
                # namespace name, only its OID, and the comparison has to
                # resolve through pg_namespace to be a real test.
                ExistsPredicate(
                    self,
                    QueryStatement(
                        self,
                        select=[self._catalog_literal(1)],
                        from_=self._catalog_relation("pg_namespace", "n2"),
                        where=WhereClause(
                            self,
                            LogicalPredicate(
                                self,
                                "AND",
                                ComparisonPredicate(
                                    self,
                                    "eq",
                                    self._catalog_column("oid", "n2"),
                                    self._catalog_column("relnamespace", "c"),
                                ),
                                ComparisonPredicate(
                                    self,
                                    "eq",
                                    self._catalog_column("nspname", "n2"),
                                    self._catalog_column("schemaname", "m"),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        return source.join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("oid", "n"),
                    self._catalog_column("relnamespace", "c"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("nspname", "n"),
                    self._catalog_column("schemaname", "m"),
                ),
            ),
        )

    def _matview_projection(self) -> "list":
        """The materialized-view row shape, including the unique-index test.

        ``has_unique_index`` is the EXISTS subquery ``REFRESH ... CONCURRENTLY``
        depends on, and it is a nested :class:`QueryStatement` rather than a
        parenthesised string.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            LogicalPredicate,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import ExistsPredicate

        unique_index = QueryStatement(
            self,
            select=[self._catalog_literal(1)],
            from_=self._catalog_relation("pg_index", "i"),
            where=WhereClause(
                self,
                LogicalPredicate(
                    self,
                    "AND",
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("indrelid", "i"),
                        self._catalog_column("oid", "c"),
                    ),
                    self._catalog_column("indisunique", "i"),
                ),
            ),
        )

        return [
            self._catalog_column("schemaname", "m", alias="schema_name"),
            self._catalog_column("matviewname", "m", alias="view_name"),
            self._catalog_column("definition", "m", alias="definition"),
            self._catalog_column("ispopulated", "m", alias="is_populated"),
            FunctionCall(
                self, "obj_description", self._catalog_column("oid", "c"), alias="comment"
            ),
            ExistsPredicate(self, unique_index).as_("has_unique_index"),
            FunctionCall(
                self,
                "pg_get_viewdef",
                self._catalog_column("oid", "c"),
                self._catalog_literal(True),
                alias="view_definition",
            ),
        ]

    def format_materialized_view_list_query_statement(
        self, expr: "PostgresMaterializedViewListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format materialized view list query.

        Uses ``pg_matviews`` (a view over ``pg_class``/``pg_rewrite``) joined with
        ``pg_index`` so callers can tell whether a materialized view is populated
        and whether it carries the UNIQUE index that ``REFRESH ... CONCURRENTLY``
        requires.

        Args:
            expr: Materialized view list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            LikeOperator,
            LikePredicate,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_system = params.get("include_system", False)

        conditions = [
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("schemaname", "m"),
                self._catalog_value(schema),
            )
        ]
        if not include_system:
            # NOTE: doubled percent -- psycopg treats a bare '%' as a placeholder start.
            conditions.append(
                LikePredicate(
                    self,
                    LikeOperator.NOT_LIKE,
                    self._catalog_column("matviewname", "m"),
                    self._catalog_literal(self._SYSTEM_PREFIX),
                )
            )

        return self._catalog_select(
            self._matview_projection(),
            self._matview_source(),
            conditions,
            [self._catalog_column("matviewname", "m")],
        )

    def format_materialized_view_info_query_statement(
        self, expr: "PostgresMaterializedViewInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format single materialized view information query.

        Args:
            expr: Materialized view info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        view_name = params.get("view_name", "")
        schema = params.get("schema") or self.default_schema

        return self._catalog_select(
            self._matview_projection(),
            self._matview_source(),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("schemaname", "m"),
                    self._catalog_value(schema),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("matviewname", "m"),
                    self._catalog_value(view_name),
                ),
            ],
        )

    def _sequence_source(self, include_pg_sequences: bool = True):
        """The relation the sequence listing reads its objects from.

        ``pg_sequences`` is both the object list and the attribute source on
        PostgreSQL 10+: it lists identity sequences, which
        ``information_schema.sequences`` does not. On 9.x the catalog view does
        not exist and identity columns do not either, so the SQL-standard view
        is the object list; the runtime columns are reported absent rather
        than guessed.
        """
        if include_pg_sequences:
            return self._catalog_relation("pg_sequences", "p")
        return self._catalog_relation("sequences", "s", schema="information_schema")

    def _sequence_projection(self, include_pg_sequences: bool = True) -> "list":
        """The sequence row shape, in the columns the chosen source has.

        ``pg_sequences`` already types its numeric columns as ``bigint``; the
        standard view reports them as ``character_data``, so on 9.x each is
        cast to ``bigint`` -- the type the server actually stores -- rather
        than left for the caller to parse. The projection aliases both shapes
        to the same names, which is what lets one parser read either row.
        """
        from rhosocial.activerecord.backend.expression.types import BigIntType

        if include_pg_sequences:
            return [
                self._catalog_column("schemaname", "p", alias="schema_name"),
                self._catalog_column("sequencename", "p", alias="sequence_name"),
                self._catalog_column("data_type", "p", alias="data_type"),
                self._catalog_column("start_value", "p", alias="start_value"),
                self._catalog_column("min_value", "p", alias="minimum_value"),
                self._catalog_column("max_value", "p", alias="maximum_value"),
                self._catalog_column("increment_by", "p", alias="increment"),
                self._catalog_column("cycle", "p", alias="cycle"),
                self._catalog_column("cache_size", "p", alias="cache_size"),
                self._catalog_column("last_value", "p", alias="last_value"),
            ]

        def numeric(name: str):
            return self._catalog_column(name, "s").cast(BigIntType(self))

        return [
            self._catalog_column("sequence_schema", "s", alias="schema_name"),
            self._catalog_column("sequence_name", "s", alias="sequence_name"),
            self._catalog_column("data_type", "s", alias="data_type"),
            numeric("start_value").as_("start_value"),
            numeric("minimum_value").as_("minimum_value"),
            numeric("maximum_value").as_("maximum_value"),
            numeric("increment").as_("increment"),
            self._catalog_column("cycle_option", "s", alias="cycle"),
        ]

    @staticmethod
    def _sequence_identity_columns(include_pg_sequences: bool) -> "tuple":
        """The schema/name columns and their alias for the chosen source."""
        if include_pg_sequences:
            return "p", "schemaname", "sequencename"
        return "s", "sequence_schema", "sequence_name"

    def format_sequence_list_query_statement(
        self, expr: "PostgresSequenceListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the sequence list query for one schema.

        Args:
            expr: Sequence list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_pg_sequences = params.get("include_pg_sequences", True)
        alias, schema_column, name_column = self._sequence_identity_columns(
            include_pg_sequences
        )

        return self._catalog_select(
            self._sequence_projection(include_pg_sequences),
            self._sequence_source(include_pg_sequences),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column(schema_column, alias),
                    self._catalog_value(schema),
                )
            ],
            [self._catalog_column(name_column, alias)],
        )

    def format_sequence_info_query_statement(
        self, expr: "PostgresSequenceInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the single-sequence query.

        Args:
            expr: Sequence info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        sequence_name = params.get("sequence_name", "")
        schema = params.get("schema") or self.default_schema
        include_pg_sequences = params.get("include_pg_sequences", True)
        alias, schema_column, name_column = self._sequence_identity_columns(
            include_pg_sequences
        )

        return self._catalog_select(
            self._sequence_projection(include_pg_sequences),
            self._sequence_source(include_pg_sequences),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column(schema_column, alias),
                    self._catalog_value(schema),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column(name_column, alias),
                    self._catalog_value(sequence_name),
                ),
            ],
        )

    def _trigger_source(self):
        """``pg_trigger t JOIN pg_class c ... JOIN pg_namespace n ... JOIN pg_proc p ...``."""
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, JoinClause

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_trigger", "t"),
            right_table=self._catalog_relation("pg_class", "c"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("tgrelid", "t"),
                self._catalog_column("oid", "c"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )
        return source.join(
            self._catalog_relation("pg_proc", "p"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("tgfoid", "t"),
                self._catalog_column("oid", "p"),
            ),
        )

    def _trigger_projection(self) -> "list":
        """The trigger row shape, with ``timing`` derived from the tgtype bitmask.

        The bitmask test is a CASE whose conditions compare an arithmetic
        expression against its own mask value: ``tgtype::integer & 64 = 64``
        is ``BinaryExpression`` on the left and a literal on the right, so the
        bit test is a structure rather than a fragment of text.
        """
        from rhosocial.activerecord.backend.expression import (
            BinaryExpression,
            ComparisonPredicate,
            FunctionCall,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression
        from rhosocial.activerecord.backend.expression.types import IntegerType

        def masked(mask: int):
            return BinaryExpression(
                self,
                "&",
                self._catalog_column("tgtype", "t").cast(IntegerType(self)),
                self._catalog_literal(mask),
            )

        def mask_test(mask: int):
            return ComparisonPredicate(
                self, "eq", masked(mask), self._catalog_literal(mask)
            )

        return [
            self._catalog_column("tgname", "t", alias="trigger_name"),
            self._catalog_column("relname", "c", alias="table_name"),
            CaseExpression(
                self,
                cases=[
                    (mask_test(64), self._catalog_literal("INSTEAD OF")),
                    (mask_test(2), self._catalog_literal("BEFORE")),
                ],
                else_result=self._catalog_literal("AFTER"),
                alias="timing",
            ),
            self._catalog_column("proname", "p", alias="function_name"),
            self._catalog_column("tgenabled", "t", alias="enabled"),
            FunctionCall(
                self,
                "pg_get_triggerdef",
                self._catalog_column("oid", "t"),
                self._catalog_literal(True),
                alias="definition",
            ),
            self._catalog_column("tgtype", "t", alias="trigger_type"),
        ]

    def format_trigger_list_query_statement(self, expr: "TriggerListQueryStatement") -> Tuple[str, tuple]:
        """Format trigger list query using pg_trigger.

        Uses pg_trigger, pg_class, pg_namespace, and pg_proc to query trigger metadata.
        The tgisinternal = false condition excludes system-generated triggers.

        Trigger timing is encoded in tgtype bitmask:
        - Bit 1 (2): BEFORE trigger
        - Bit 6 (64): INSTEAD OF trigger
        - Neither: AFTER trigger (default)

        Note: Bits 2-5 encode trigger events (INSERT, DELETE, UPDATE, TRUNCATE),
        not timing.

        Filtering by table does not change the ordering: every trigger row of a
        table is still reported in trigger-name order, which is what the
        concatenated form rendered on both branches (only its leading
        whitespace differed).

        Args:
            expr: Trigger list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, LogicalPredicate

        params = expr.get_params()
        table_name = params.get("table_name")
        schema = params.get("schema") or self.default_schema

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            LogicalPredicate(self, "NOT", self._catalog_column("tgisinternal", "t")),
        ]
        if table_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(table_name),
                )
            )

        return self._catalog_select(
            self._trigger_projection(),
            self._trigger_source(),
            conditions,
            [self._catalog_column("tgname", "t")],
        )

    def format_trigger_info_query_statement(self, expr: "TriggerInfoQueryStatement") -> Tuple[str, tuple]:
        """Format single trigger information query.

        Query pg_trigger for trigger details.

        Args:
            expr: Trigger info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, LogicalPredicate

        params = expr.get_params()
        trigger_name = params.get("trigger_name", "")
        table_name = params.get("table_name")
        schema = params.get("schema") or self.default_schema

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("tgname", "t"),
                self._catalog_value(trigger_name),
            ),
            LogicalPredicate(self, "NOT", self._catalog_column("tgisinternal", "t")),
        ]
        if table_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(table_name),
                )
            )

        return self._catalog_select(self._trigger_projection(), self._trigger_source(), conditions)

    # ========== PostgreSQL database size ==========

    def format_postgres_database_size_function(
        self, expr: "PostgresDatabaseSizeFunction"
    ) -> Tuple[str, tuple]:
        """Format the ``pg_database_size(name)`` call.

        The name is a bound value: PostgreSQL parses the argument, so it travels
        through the driver's placeholder rather than being spelled into the text.

        Args:
            expr: Database size function expression with the database name.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import FunctionCall

        params = expr.get_params()
        return FunctionCall(
            self, "pg_database_size", self._catalog_value(params.get("database_name"))
        ).to_sql()

    def format_database_size_query_statement(
        self, expr: "PostgresDatabaseSizeQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the query that reports a database's size in bytes.

        The SELECT and the ``size_bytes`` alias belong to this query, not to
        the function: the caller reads the row by that column name, so the row
        shape is the query node's contract and the function's spelling stays
        the function node's. The call is the same :class:`FunctionCall` node the
        function formatter writes -- the alias is this query's.

        Args:
            expr: Database size query expression with the database name.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        params = expr.get_params()
        return QueryStatement(
            self,
            select=[
                FunctionCall(
                    self,
                    "pg_database_size",
                    self._catalog_value(params.get("database_name")),
                    alias="size_bytes",
                )
            ],
        ).to_sql()

    # ------------------------------------------------------------------
    # The per-table storage trio. The table listing asks the same three
    # calls in its projection (``_relation_size_call``); these formatters
    # answer them for one relation whose OID a caller already holds.
    # ------------------------------------------------------------------

    def format_postgres_relation_size_function(
        self, expr: "PostgresRelationSizeFunction"
    ) -> Tuple[str, tuple]:
        """Format the ``pg_relation_size(relid)`` call.

        The relation's OID names a catalog row the caller already read, so it is
        a bound value: PostgreSQL reads an ``oid`` where the function's
        ``regclass`` argument is expected without a cast, and nothing about the
        relation is spelled into the text.

        Args:
            expr: Relation size function expression with the relation's OID.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import FunctionCall

        params = expr.get_params()
        return FunctionCall(
            self, "pg_relation_size", self._catalog_value(params.get("relid"))
        ).to_sql()

    def format_postgres_indexes_size_function(
        self, expr: "PostgresIndexesSizeFunction"
    ) -> Tuple[str, tuple]:
        """Format the ``pg_indexes_size(relid)`` call.

        The index side of the split, bound for the same reason the heap side is:
        the OID is data. Together they are the two narrow slots
        ``pg_total_relation_size`` cannot separate.

        Args:
            expr: Indexes size function expression with the relation's OID.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import FunctionCall

        params = expr.get_params()
        return FunctionCall(
            self, "pg_indexes_size", self._catalog_value(params.get("relid"))
        ).to_sql()

    def format_table_storage_query_statement(
        self, expr: "PostgresTableStorageQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the query that reports one relation's storage split.

        The two calls in one statement, aliased to the slot names the parser
        fills: the row shape is the contract, and asking both in one round trip
        is what makes the split a single answer rather than two numbers read at
        two different moments. The OID binds once per call.

        Args:
            expr: Table storage query expression with the relation's OID.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        params = expr.get_params()
        relid = params.get("relid")
        return QueryStatement(
            self,
            select=[
                FunctionCall(
                    self, "pg_relation_size", self._catalog_value(relid), alias="data_size_bytes"
                ),
                FunctionCall(
                    self, "pg_indexes_size", self._catalog_value(relid), alias="index_size_bytes"
                ),
            ],
        ).to_sql()


    # ------------------------------------------------------------------
    # Status and enum queries (org guide §16.2: the SQL text is the
    # formatter's; the status introspector and the enum manager only
    # construct these nodes and execute them).
    # ------------------------------------------------------------------

    def format_postgres_settings_query(
        self, expr: "PostgresSettingsQuery"
    ) -> Tuple[str, tuple]:
        """Read ``pg_settings`` rows, optionally filtered by name.

        The name list is data, so it binds: an ``IN`` list of caller-supplied
        setting names travels as one parameter per name, in the order given.
        """
        from rhosocial.activerecord.backend.expression import InPredicate

        conditions = []
        if expr.names:
            conditions.append(
                InPredicate(
                    self,
                    self._catalog_column("name"),
                    self._catalog_value(list(expr.names)),
                )
            )

        return self._catalog_select(
            [
                self._catalog_column("name"),
                self._catalog_column("setting"),
                self._catalog_column("unit"),
            ],
            self._catalog_relation("pg_settings"),
            conditions,
            [self._catalog_column("name")],
        )

    def format_postgres_activity_count_query(
        self, expr: "PostgresActivityCountQuery"
    ) -> Tuple[str, tuple]:
        """Count rows in ``pg_stat_activity``."""
        from rhosocial.activerecord.backend.expression import QueryStatement, RowCountWildcard
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("total_connections")],
            from_=self._catalog_relation("pg_stat_activity"),
        ).to_sql()

    def format_postgres_database_list_query(
        self, expr: "PostgresDatabaseListQuery"
    ) -> Tuple[str, tuple]:
        """List databases with sizes, non-templates plus the current one."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            LogicalPredicate,
        )

        return self._catalog_select(
            [
                self._catalog_column("datname", alias="name"),
                FunctionCall(
                    self,
                    "pg_database_size",
                    self._catalog_column("datname"),
                    alias="size_bytes",
                ),
            ],
            self._catalog_relation("pg_database"),
            [
                LogicalPredicate(
                    self,
                    "OR",
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("datistemplate"),
                        self._catalog_literal(False),
                    ),
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("datname"),
                        FunctionCall(self, "current_database"),
                    ),
                )
            ],
            [self._catalog_column("datname")],
        )

    def format_postgres_schema_stats_query(
        self, expr: "PostgresSchemaStatsQuery"
    ) -> Tuple[str, tuple]:
        """Per-schema table and view counts."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            InPredicate,
            JoinClause,
            LogicalPredicate,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        def counted(relkind: str):
            return CountFunction(
                self,
                CaseExpression(
                    self,
                    cases=[
                        (
                            ComparisonPredicate(
                                self,
                                "eq",
                                self._catalog_column("relkind", "c"),
                                self._catalog_literal(relkind),
                            ),
                            self._catalog_literal(1),
                        )
                    ]
                ),
            )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_namespace", "n"),
            right_table=self._catalog_relation("pg_class", "c"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )

        return self._catalog_select(
            [
                self._catalog_column("nspname", "n", alias="schema_name"),
                counted("r").as_("table_count"),
                counted("v").as_("view_count"),
            ],
            source,
            [
                LogicalPredicate(
                    self,
                    "NOT",
                    InPredicate(
                        self,
                        self._catalog_column("nspname", "n"),
                        self._catalog_literal(
                            ("pg_catalog", "information_schema", "pg_toast")
                        ),
                    ),
                )
            ],
            [self._catalog_column("nspname", "n")],
            group_by=[self._catalog_column("nspname", "n")],
        )

    def format_postgres_current_database_query(
        self, expr: "PostgresCurrentDatabaseQuery"
    ) -> Tuple[str, tuple]:
        """The current database's name."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "current_database")]).to_sql()

    def format_postgres_role_list_query(
        self, expr: "PostgresRoleListQuery"
    ) -> Tuple[str, tuple]:
        """List roles with superuser flag and owner."""
        from rhosocial.activerecord.backend.expression import FunctionCall

        return self._catalog_select(
            [
                self._catalog_column("rolname", alias="name"),
                self._catalog_column("rolsuper", alias="is_superuser"),
                FunctionCall(
                    self,
                    "pg_catalog.pg_get_userbyid",
                    self._catalog_column("oid"),
                    alias="owner",
                ),
            ],
            self._catalog_relation("pg_roles"),
            order_by=[self._catalog_column("rolname")],
        )

    # ------------------------------------------------------------------
    # The session / WAL / replication / archive reads
    # ------------------------------------------------------------------

    def format_postgres_current_user_query(
        self, expr: "PostgresCurrentUserQuery"
    ) -> Tuple[str, tuple]:
        """The session's role.

        ``CURRENT_USER`` is niladic: the standard writes it without parentheses,
        and PostgreSQL refuses ``CURRENT_USER()`` (measured: syntax error at or
        near ``"("`` on 17.11). ``niladic`` is what tells the call renderer.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(
            self, select=[FunctionCall(self, "current_user", niladic=True)]
        ).to_sql()

    def format_postgres_current_schema_query(
        self, expr: "PostgresCurrentSchemaQuery"
    ) -> Tuple[str, tuple]:
        """The session's current schema."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "current_schema")]).to_sql()

    def format_postgres_client_address_query(
        self, expr: "PostgresClientAddressQuery"
    ) -> Tuple[str, tuple]:
        """The client's address (null over a Unix-domain socket)."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "inet_client_addr")]).to_sql()

    def format_postgres_ssl_session_query(
        self, expr: "PostgresSslSessionQuery"
    ) -> Tuple[str, tuple]:
        """This session's SSL state: three columns of its own ``pg_stat_ssl`` row."""
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, FunctionCall

        return self._catalog_select(
            [
                self._catalog_column("ssl"),
                self._catalog_column("version"),
                self._catalog_column("cipher"),
            ],
            self._catalog_relation("pg_stat_ssl"),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("pid"),
                    FunctionCall(self, "pg_backend_pid"),
                )
            ],
        )

    def format_postgres_wal_position_query(
        self, expr: "PostgresWalPositionQuery"
    ) -> Tuple[str, tuple]:
        """Bytes of WAL written, as the LSN distance from ``0/0``.

        The function pair is version-dependent: ``wal`` from PostgreSQL 10, and
        ``xlog`` before it. The cast to BIGINT is the row's type, not a
        presentation choice -- the difference is a ``numeric``.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement
        from rhosocial.activerecord.backend.expression.types import BigIntType

        current, diff = (
            ("pg_current_wal_lsn", "pg_wal_lsn_diff")
            if self.version >= (10, 0, 0)
            else ("pg_current_xlog_location", "pg_xlog_location_diff")
        )
        return QueryStatement(
            self,
            select=[
                FunctionCall(
                    self,
                    diff,
                    FunctionCall(self, current),
                    self._catalog_literal("0/0"),
                )
                .cast(BigIntType(self))
                .as_("wal_bytes")
            ],
        ).to_sql()

    def format_postgres_wal_file_count_query(
        self, expr: "PostgresWalFileCountQuery"
    ) -> Tuple[str, tuple]:
        """How many files the WAL directory holds.

        ``pg_ls_waldir()`` is a set-returning function, so it is a row source:
        the count reads its rows rather than calling it in the select list.
        It exists from PostgreSQL 10, which is why the version refuses it below.
        """
        from rhosocial.activerecord.backend.expression import QueryStatement, RowCountWildcard
        from rhosocial.activerecord.backend.expression.functions import CountFunction
        from rhosocial.activerecord.backend.expression.sources import TableFunctionSource
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "pg_ls_waldir()",
                "The WAL directory listing was introduced in PostgreSQL 10; "
                "count the files with pg_ls_dir('pg_wal') on older servers.",
            )

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("wal_files")],
            from_=TableFunctionSource(self, "pg_ls_waldir"),
        ).to_sql()

    def format_postgres_checkpoint_count_query(
        self, expr: "PostgresCheckpointCountQuery"
    ) -> Tuple[str, tuple]:
        """Checkpoints run so far: the timed and requested counters, summed.

        The counters moved from ``pg_stat_bgwriter`` to ``pg_stat_checkpointer``
        in PostgreSQL 17, and the column names changed with them; both the
        relation and the names are therefore the server version's.
        """
        from rhosocial.activerecord.backend.expression import (
            FunctionCall,
            QueryStatement,
        )
        from rhosocial.activerecord.backend.expression.operators import ArithmeticOperation

        if self.version >= (17, 0, 0):
            relation, timed, requested = "pg_stat_checkpointer", "num_timed", "num_requested"
        else:
            relation, timed, requested = "pg_stat_bgwriter", "checkpoints_timed", "checkpoints_req"

        def coalesced(column: str):
            return FunctionCall(
                self, "coalesce", self._catalog_column(column), self._catalog_literal(0)
            )

        return QueryStatement(
            self,
            select=[
                ArithmeticOperation(self, "add", coalesced(timed), coalesced(requested)).as_(
                    "checkpoint_count"
                )
            ],
            from_=self._catalog_relation(relation),
        ).to_sql()

    def format_postgres_checkpoint_lsn_query(
        self, expr: "PostgresCheckpointLsnQuery"
    ) -> Tuple[str, tuple]:
        """The last checkpoint's LSN.

        ``pg_control_checkpoint()`` answers one composite row, and its
        ``checkpoint_lsn`` field is read by naming the function as a row source
        and selecting the field off the alias. The earlier form
        ``(pg_control_checkpoint()).checkpoint_lsn`` puts a parenthesised call in
        the select list, which PostgreSQL refuses (measured 17.11: syntax error
        at or near ``"."``).
        """
        from rhosocial.activerecord.backend.expression import QueryStatement
        from rhosocial.activerecord.backend.expression.sources import TableFunctionSource
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        if self.version < (9, 6, 0):
            raise UnsupportedFeatureError(
                self.name,
                "pg_control_checkpoint()",
                "The control-data accessor was introduced in PostgreSQL 9.6.",
            )

        return QueryStatement(
            self,
            select=[self._catalog_column("checkpoint_lsn", "c", alias="checkpoint_info")],
            from_=TableFunctionSource(self, "pg_control_checkpoint", alias="c"),
        ).to_sql()

    def format_postgres_recovery_state_query(
        self, expr: "PostgresRecoveryStateQuery"
    ) -> Tuple[str, tuple]:
        """Whether this server is replaying WAL (a standby)."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "pg_is_in_recovery")]).to_sql()

    def format_postgres_replication_slot_count_query(
        self, expr: "PostgresReplicationSlotCountQuery"
    ) -> Tuple[str, tuple]:
        """How many replication slots exist."""
        from rhosocial.activerecord.backend.expression import QueryStatement, RowCountWildcard
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("slot_count")],
            from_=self._catalog_relation("pg_replication_slots"),
        ).to_sql()

    def format_postgres_active_replication_count_query(
        self, expr: "PostgresActiveReplicationCountQuery"
    ) -> Tuple[str, tuple]:
        """How many standbys are streaming right now."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            QueryStatement,
            RowCountWildcard,
        )
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("active_count")],
            from_=self._catalog_relation("pg_stat_replication"),
            where=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("state"),
                self._catalog_literal("streaming"),
            ),
        ).to_sql()

    def format_postgres_replication_lag_query(
        self, expr: "PostgresReplicationLagQuery"
    ) -> Tuple[str, tuple]:
        """The replay lag in bytes, as the receive/replay LSN distance.

        The three function names moved together in PostgreSQL 10 (``xlog`` to
        ``wal``), so the pair is chosen as a pair: a 9.x server has no
        ``pg_wal_lsn_diff`` at all.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement
        from rhosocial.activerecord.backend.expression.types import BigIntType

        if self.version >= (10, 0, 0):
            diff, receive, replay = (
                "pg_wal_lsn_diff",
                "pg_last_wal_receive_lsn",
                "pg_last_wal_replay_lsn",
            )
        else:
            diff, receive, replay = (
                "pg_xlog_location_diff",
                "pg_last_xlog_receive_location",
                "pg_last_xlog_replay_location",
            )

        return QueryStatement(
            self,
            select=[
                FunctionCall(
                    self,
                    diff,
                    FunctionCall(self, receive),
                    FunctionCall(self, replay),
                )
                .cast(BigIntType(self))
                .as_("lag_bytes")
            ],
        ).to_sql()

    def format_postgres_archive_stats_query(
        self, expr: "PostgresArchiveStatsQuery"
    ) -> Tuple[str, tuple]:
        """The archiver's counters and its last success/failure times."""
        from rhosocial.activerecord.backend.expression import QueryStatement
        from rhosocial.activerecord.backend.expression.types import TextType

        return QueryStatement(
            self,
            select=[
                self._catalog_column("archived_count"),
                self._catalog_column("failed_count"),
                self._catalog_column("last_archived_time")
                .cast(TextType(self))
                .as_("last_archived"),
                self._catalog_column("last_failed_time")
                .cast(TextType(self))
                .as_("last_failed"),
            ],
            from_=self._catalog_relation("pg_stat_archiver"),
        ).to_sql()

    def format_postgres_extension_list_query(
        self, expr: "PostgresExtensionListQuery"
    ) -> Tuple[str, tuple]:
        """Every installed extension with its schema and description."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_extension", "e"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oid", "n"),
                self._catalog_column("extnamespace", "e"),
            ),
        )

        return self._catalog_select(
            [
                self._catalog_column("extname", "e", alias="name"),
                self._catalog_column("extversion", "e", alias="version"),
                self._catalog_column("nspname", "n", alias="schema"),
                FunctionCall(
                    self,
                    "pg_catalog.obj_description",
                    self._catalog_column("oid", "e"),
                    self._catalog_literal("pg_extension"),
                    alias="description",
                ),
            ],
            source,
            order_by=[self._catalog_column("extname", "e")],
        )

    def format_postgres_enum_type_exists_query(
        self, expr: "PostgresEnumTypeExistsQuery"
    ) -> Tuple[str, tuple]:
        """Whether an enum type exists; name (and optional schema) are bound.

        The whole question is one EXISTS over a join between ``pg_type`` and
        ``pg_namespace``, so the select list is the EXISTS predicate itself and
        the subquery is a QueryStatement.

        Raises:
            ValueError: The enum name is blank, which no catalog row can match.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
            LogicalPredicate,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import ExistsPredicate

        if not expr.name:
            raise ValueError(
                "PostgresEnumTypeExistsQuery needs a type name: an empty name "
                "cannot match a catalog row."
            )

        conditions = [
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("typname", "t"),
                self._catalog_value(expr.name),
            )
        ]
        if expr.schema is not None:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("nspname", "n"),
                    self._catalog_value(expr.schema),
                )
            )

        subquery = QueryStatement(
            self,
            select=[self._catalog_literal(1)],
            from_=JoinClause(
                self,
                left_table=self._catalog_relation("pg_type", "t"),
                right_table=self._catalog_relation("pg_namespace", "n"),
                join_type="JOIN",
                condition=ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("typnamespace", "t"),
                    self._catalog_column("oid", "n"),
                ),
            ),
            where=WhereClause(self, LogicalPredicate(self, "AND", *conditions)),
        )
        return QueryStatement(self, select=[ExistsPredicate(self, subquery)]).to_sql()

    def format_postgres_enum_type_values_query(
        self, expr: "PostgresEnumTypeValuesQuery"
    ) -> Tuple[str, tuple]:
        """The labels of an enum type in sort order; name (and schema) are bound.

        Raises:
            ValueError: The enum name is blank, which no catalog row can match.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
        )

        if not expr.name:
            raise ValueError(
                "PostgresEnumTypeValuesQuery needs a type name: an empty name "
                "cannot match a catalog row."
            )

        conditions = [
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("typname", "t"),
                self._catalog_value(expr.name),
            )
        ]
        if expr.schema is not None:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("nspname", "n"),
                    self._catalog_value(expr.schema),
                )
            )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_type", "t"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("typnamespace", "t"),
                self._catalog_column("oid", "n"),
            ),
        ).join(
            self._catalog_relation("pg_enum", "e"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oid", "t"),
                self._catalog_column("enumtypid", "e"),
            ),
        )

        return self._catalog_select(
            [self._catalog_column("enumlabel", "e")],
            source,
            conditions,
            [self._catalog_column("enumsortorder", "e")],
        )


    def format_table_usage_stats_query_statement(
        self, expr: "TableUsageStatsQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the per-table usage-counter listing from pg_stat_user_tables.

        One row per table the collector has seen in the schema, projected from
        its own columns: ``seq_scan`` / ``idx_scan`` for the reads,
        ``n_tup_ins`` / ``n_tup_upd`` / ``n_tup_del`` for the writes,
        ``n_live_tup`` for the row estimate the counters were measured
        against. The view's other facts -- ``n_dead_tup``, the ``last_*``
        access times -- are real and useful but have no common name, so they
        are not projected: this listing is the common shape, and the engine's
        extras belong on a backend reader beside it.

        A table with no statistics row yet is simply absent from
        ``pg_stat_user_tables``; the listing answers for the tables the
        collector has seen, which is the honest scope rather than a
        zero-filled one.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema

        source = self._catalog_relation("pg_stat_user_tables", "s")

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("schemaname", "s"), self._catalog_value(schema)
            ),
        ]
        projections = [
            self._catalog_column("schemaname", "s"),
            self._catalog_column("relname", "s"),
            self._catalog_column("seq_scan", "s"),
            self._catalog_column("idx_scan", "s"),
            self._catalog_column("n_tup_ins", "s"),
            self._catalog_column("n_tup_upd", "s"),
            self._catalog_column("n_tup_del", "s"),
            self._catalog_column("n_live_tup", "s"),
        ]
        return self._catalog_select(
            projections, source, conditions, [self._catalog_column("relname", "s")]
        )


    def format_unused_indexes_query_statement(
        self, expr: "UnusedIndexesQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the unused-indexes listing from ``pg_stat_user_indexes``.

        PostgreSQL ships no vendor view for this, so the family is computed
        from the counters: ``idx_scan = 0`` over valid indexes in the
        requested schema. Only ``idx_scan`` speaks -- an index is what makes
        a scan indexed, so the other counters do not apply.

        The two conditions answer different questions, so they cannot share a
        relation: ``i.idx_scan = 0`` is "no scan in the statistics window",
        while ``ix.indisvalid`` is "the planner would select it". The
        statistics view carries only counters, so the validity flag has to
        come from ``pg_index``, joined on ``indexrelid`` (the index oid on
        both sides -- ``relid`` is the table oid and does not match).

        What the listing is not: proof. It reports "no index scan in the
        window the statistics remember", and that window ends at the last
        crash or ``pg_stat_reset()``.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
            Literal,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self, "eq", self._catalog_column("idx_scan", "i"), Literal(self, 0)
            ),
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("indisvalid", "ix"),
                Literal(self, True, inline=True),
            ),
        ]

        indexes = JoinClause(
            self,
            left_table=self._catalog_relation("pg_stat_user_indexes", "i"),
            right_table=self._catalog_relation("pg_index", "ix"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("indexrelid", "i"),
                self._catalog_column("indexrelid", "ix"),
            ),
        )
        source = indexes.join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("schemaname", "i"),
                self._catalog_column("nspname", "n"),
            ),
        )

        projections = [
            self._catalog_column("nspname", "n").as_("schema_name"),
            self._catalog_column("relname", "i").as_("table_name"),
            self._catalog_column("indexrelname", "i").as_("index_name"),
        ]
        return self._catalog_select(
            projections,
            source,
            conditions,
            [
                self._catalog_column("relname", "i"),
                self._catalog_column("indexrelname", "i"),
            ],
        )


    def format_runtime_metrics_query_statement(
        self, expr: "RuntimeMetricsQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the runtime listing from ``pg_stat_activity``.

        One row per backend process, projected to the fields that describe the
        server rather than one session: the application name, the state, what
        the process is waiting for, and when it started. ``source`` carries
        the wait event rather than a reset window -- the window is the same
        for every row of this listing and belongs in the family's docstring,
        which is where a reader looks for what "now" means.
        """

        projections = [
            self._catalog_column("application_name", "a").as_("name"),
            self._catalog_column("state", "a").as_("value"),
            self._catalog_column("wait_event_type", "a").as_("kind"),
            self._catalog_column("wait_event", "a").as_("description"),
            self._catalog_column("backend_start", "a").as_("source"),
        ]
        return self._catalog_select(
            projections, self._catalog_relation("pg_stat_activity", "a"), None, None
        )

    def format_index_usage_stats_query_statement(
        self, expr: "IndexUsageStatsQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format per-index counters from ``pg_stat_user_indexes``.

        ``idx_scan``, ``idx_tup_read`` and ``idx_tup_fetch`` are the engine's
        own counters; the size comes from ``pg_relation_size`` over the
        index's own oid, so "how much use" and "how much space" land in one
        row instead of two queries a caller has to join.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_stat_user_indexes", "i"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("schemaname", "i"),
                self._catalog_column("nspname", "n"),
            ),
        )
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
        ]
        projections = [
            self._catalog_column("nspname", "n").as_("schema_name"),
            self._catalog_column("relname", "i").as_("table_name"),
            self._catalog_column("indexrelname", "i").as_("index_name"),
            self._catalog_column("idx_scan", "i"),
            self._catalog_column("idx_tup_read", "i").as_("rows_read"),
            self._catalog_column("idx_tup_fetch", "i").as_("rows_fetched"),
            FunctionCall(
                self, "pg_relation_size", self._catalog_column("indexrelid", "i")
            ).as_("index_size_bytes"),
        ]
        return self._catalog_select(
            projections,
            source,
            conditions,
            [self._catalog_column("relname", "i"), self._catalog_column("indexrelname", "i")],
        )

    def format_object_dependency_query_statement(
        self, expr: "ObjectDependencyQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the dependency graph from ``pg_depend`` joined to the names.

        ``pg_depend`` stores edges by oid, so the reader-facing join is
        required. ``deptype`` names the relation; there is no per-edge action
        column in PostgreSQL -- a dependant's behaviour on drop is a property
        of its class -- so ``action`` stays unmapped here rather than being
        guessed from ``deptype``.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        object_name = params.get("object_name")
        # pg_depend r -> d, then the dependant's own class c, then the
        # namespace n that owns both. Chained with source.join, which is the
        # form every multi-table source in this mixin uses.
        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_class", "r"),
            right_table=self._catalog_relation("pg_depend", "d"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oid", "r"),
                self._catalog_column("refobjid", "d"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "r"),
                self._catalog_column("oid", "n"),
            ),
        )
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
        ]
        if object_name:
            conditions.append(
                ComparisonPredicate(
                    self, "eq", self._catalog_column("relname", "r"), self._catalog_value(object_name)
                )
            )
        projections = [
            self._catalog_column("nspname", "n").as_("schema_name"),
            self._catalog_column("relname", "r").as_("referenced"),
            self._catalog_column("deptype", "d").as_("kind"),
            self._catalog_column("classid", "d").as_("referenced_type"),
        ]
        return self._catalog_select(projections, source, conditions, None)

    # ========== Schema-object families (partitions, routines, types, operators)
    #
    # Every statement below is built from the same catalog node helpers the
    # families above use: ``_catalog_relation`` for the relation, ``_catalog_column``
    # for the column, ``_catalog_value`` for caller data and ``_catalog_literal``
    # for a catalog fact. The projection aliases are the parser's contract.

    @staticmethod
    def _type_name_call(
        mixin,
        oid_column,
        alias: Optional[str] = None,
        typmod_column=None,
    ):
        """``pg_catalog.format_type(oid, typmod)`` for one catalog row."""
        from rhosocial.activerecord.backend.expression import FunctionCall

        args = [oid_column]
        args.append(typmod_column if typmod_column is not None else mixin._catalog_literal(None))
        return FunctionCall(mixin, "pg_catalog.format_type", *args, alias=alias)

    @staticmethod
    def _optional_type_name_call(
        mixin,
        oid_column,
        alias: Optional[str] = None,
    ):
        """``format_type(oid, NULL)``, or NULL when the OID position is absent.

        An operator's left or right operand type is OID 0 when the operator
        has no operand there. ``format_type(0, NULL)`` would answer with a
        made-up type, so the absence is kept as NULL.
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression

        return CaseExpression(
            mixin,
            cases=[
                (
                    ComparisonPredicate(
                        mixin, "eq", oid_column, mixin._catalog_literal(0)
                    ),
                    mixin._catalog_literal(None),
                )
            ],
            else_result=mixin._type_name_call(mixin, oid_column),
            alias=alias,
        )

    def format_partition_list_query_statement(
        self, expr: "PostgresPartitionListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the child-partition listing for one partitioned table.

        The parent is named by the caller; ``pg_get_partkeydef`` names its
        strategy and key, and ``pg_get_expr(relpartbound)`` spells each child's
        bound. A partitioned parent is ``relkind = 'p'``; ordinary inheritance
        (``relkind = 'r'``) is not declarative partitioning and is deliberately
        not listed here.

        Args:
            expr: Partition list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).

        Raises:
            UnsupportedFeatureError: The connected server predates PostgreSQL 10.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )
        from rhosocial.activerecord.backend.expression.types import BigIntType

        if not self.supports_table_partitioning():
            raise UnsupportedFeatureError(
                self.name,
                "partition introspection",
                "Partition introspection requires PostgreSQL 10+.",
            )

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self.default_schema
        include_partitions = params.get("include_partitions", True)
        include_statistics = params.get("include_statistics", True)

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_class", "pc"),
            right_table=self._catalog_relation("pg_namespace", "pn"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "pc"),
                self._catalog_column("oid", "pn"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_inherits", "i"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("inhparent", "i"),
                self._catalog_column("oid", "pc"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_class", "ch"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oid", "ch"),
                self._catalog_column("inhrelid", "i"),
            ),
        )

        projection = [
            self._catalog_column("relname", "pc", alias="parent_table"),
            FunctionCall(
                self,
                "pg_get_partkeydef",
                self._catalog_column("oid", "pc"),
                alias="partition_key",
            ),
        ]
        if include_partitions:
            projection.extend(
                [
                    self._catalog_column("relname", "ch", alias="name"),
                    FunctionCall(
                        self,
                        "pg_get_expr",
                        self._catalog_column("relpartbound", "ch"),
                        self._catalog_column("oid", "ch"),
                        alias="bound",
                    ),
                    FunctionCall(
                        self,
                        "pg_catalog.obj_description",
                        self._catalog_column("oid", "ch"),
                        alias="comment",
                    ),
                ]
            )
            if include_statistics:
                projection.extend(
                    [
                        self._catalog_column("reltuples", "ch")
                        .cast(BigIntType(self))
                        .as_("row_count"),
                        FunctionCall(
                            self,
                            "pg_total_relation_size",
                            self._catalog_column("oid", "ch"),
                            alias="size_bytes",
                        ),
                    ]
                )

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("relname", "pc"), self._catalog_value(table_name)
            ),
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "pn"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self, "eq", self._catalog_column("relkind", "pc"), self._catalog_literal("p")
            ),
        ]
        order_by = [self._catalog_column("relname", "ch")] if include_partitions else None
        return self._catalog_select(projection, source, conditions, order_by)

    def format_routine_list_query_statement(
        self, expr: "PostgresRoutineListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the routine listing from ``pg_proc``.

        ``prokind`` distinguishes functions, procedures, aggregates and window
        functions; ``pg_get_function_identity_arguments`` is the overload
        identity, and ``pg_get_function_result`` the return type. The language
        is joined from ``pg_language`` because the catalog stores its OID.

        Args:
            expr: Routine list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        routine_name = params.get("routine_name")

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_proc", "p"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("pronamespace", "p"),
                self._catalog_column("oid", "n"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_language", "l"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("prolang", "p"),
                self._catalog_column("oid", "l"),
            ),
        )

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            )
        ]
        if routine_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("proname", "p"),
                    self._catalog_value(routine_name),
                )
            )

        if self.version_at_least((11, 0, 0)):
            kind_projection = self._catalog_column("prokind", "p", alias="kind")
        else:
            # ``prokind`` arrived in PostgreSQL 11; before it the same fact
            # was two booleans. A procedure cannot exist on those releases,
            # so the two old flags answer the whole question they had.
            from rhosocial.activerecord.backend.expression.advanced_functions import (
                CaseExpression,
            )

            kind_projection = CaseExpression(
                self,
                cases=[
                    (
                        self._catalog_column("proiswindow", "p"),
                        self._catalog_literal("w"),
                    ),
                    (
                        self._catalog_column("proisagg", "p"),
                        self._catalog_literal("a"),
                    ),
                ],
                else_result=self._catalog_literal("f"),
                alias="kind",
            )

        projection = [
            self._catalog_column("proname", "p", alias="name"),
            self._catalog_column("nspname", "n", alias="schema_name"),
            kind_projection,
            FunctionCall(
                self,
                "pg_get_function_identity_arguments",
                self._catalog_column("oid", "p"),
                alias="identity_arguments",
            ),
            FunctionCall(
                self,
                "pg_get_function_result",
                self._catalog_column("oid", "p"),
                alias="result_type",
            ),
            self._catalog_column("lanname", "l", alias="language"),
            self._catalog_column("provolatile", "p", alias="volatility"),
            self._catalog_column("prosecdef", "p", alias="security_definer"),
            FunctionCall(
                self,
                "pg_catalog.obj_description",
                self._catalog_column("oid", "p"),
                self._catalog_literal("pg_proc"),
                alias="comment",
            ),
        ]
        return self._catalog_select(
            projection,
            source,
            conditions,
            [
                self._catalog_column("nspname", "n"),
                self._catalog_column("proname", "p"),
                self._catalog_column("oid", "p"),
            ],
        )

    def format_type_list_query_statement(
        self, expr: "PostgresTypeListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the user-type listing (enum, domain, composite, range).

        One query answers the whole family: ``pg_type.typtype`` names the kind,
        ``pg_enum`` supplies the labels in their declared order, and
        ``pg_attribute`` supplies a composite's fields. The joins are LEFT
        joins because a type has labels *or* fields, never both, and the parser
        groups the rows back into one record per type.

        A multirange's subtype lives on the row whose ``rngmultitypid`` points
        at it, a column that arrived in PostgreSQL 14; the join is written for
        the server in front of us and omits that edge on older releases.

        Args:
            expr: Type list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
            LogicalPredicate,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression
        from rhosocial.activerecord.backend.expression.predicates import InPredicate

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        type_name = params.get("type_name")

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_type", "t"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("typnamespace", "t"),
                self._catalog_column("oid", "n"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_enum", "e"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("enumtypid", "e"),
                self._catalog_column("oid", "t"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_attribute", "a"),
            join_type="LEFT",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "a"),
                    self._catalog_column("typrelid", "t"),
                ),
                ComparisonPredicate(
                    self,
                    "gt",
                    self._catalog_column("attnum", "a"),
                    self._catalog_literal(0),
                ),
                LogicalPredicate(self, "NOT", self._catalog_column("attisdropped", "a")),
            ),
        )
        has_range_types = self.version_at_least((9, 2, 0))
        if has_range_types:
            if self.supports_multirange_type():
                range_condition = LogicalPredicate(
                    self,
                    "OR",
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("rngtypid", "r"),
                        self._catalog_column("oid", "t"),
                    ),
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("rngmultitypid", "r"),
                        self._catalog_column("oid", "t"),
                    ),
                )
            else:
                range_condition = ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("rngtypid", "r"),
                    self._catalog_column("oid", "t"),
                )
            source = source.join(
                self._catalog_relation("pg_range", "r"),
                join_type="LEFT",
                condition=range_condition,
            )
        source = source.join(
            self._catalog_relation("pg_class", "c"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oid", "c"),
                self._catalog_column("typrelid", "t"),
            ),
        )

        base_type_cases = [
            (
                ComparisonPredicate(
                    self, "eq", self._catalog_column("typtype", "t"), self._catalog_literal("d")
                ),
                self._type_name_call(
                    self,
                    self._catalog_column("typbasetype", "t"),
                    typmod_column=self._catalog_column("typtypmod", "t"),
                ),
            ),
        ]
        if has_range_types:
            base_type_cases.append(
                (
                    InPredicate(
                        self,
                        self._catalog_column("typtype", "t"),
                        self._catalog_literal(("r", "m")),
                    ),
                    self._type_name_call(
                        self,
                        self._catalog_column("rngsubtype", "r"),
                    ),
                )
            )
        base_type = CaseExpression(
            self,
            cases=base_type_cases,
            else_result=self._catalog_literal(None),
            alias="base_type",
        )

        projection = [
            self._catalog_column("typname", "t", alias="name"),
            self._catalog_column("nspname", "n", alias="schema_name"),
            self._catalog_column("typtype", "t", alias="type_code"),
            self._catalog_column("enumlabel", "e", alias="label"),
            self._catalog_column("enumsortorder", "e", alias="label_order"),
            self._catalog_column("attname", "a", alias="field_name"),
            self._catalog_column("attnum", "a", alias="field_order"),
            base_type,
            CaseExpression(
                self,
                cases=[
                    (
                        ComparisonPredicate(
                            self,
                            "eq",
                            self._catalog_column("typtype", "t"),
                            self._catalog_literal("d"),
                        ),
                        self._catalog_column("typnotnull", "t"),
                    )
                ],
                else_result=self._catalog_literal(None),
                alias="domain_not_null",
            ),
            self._catalog_column("typdefault", "t", alias="domain_default"),
            FunctionCall(
                self,
                "pg_catalog.obj_description",
                self._catalog_column("oid", "t"),
                self._catalog_literal("pg_type"),
                alias="comment",
            ),
            self._catalog_column("relkind", "c", alias="relation_kind"),
            self._catalog_column("relname", "c", alias="relation_name"),
        ]

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            InPredicate(
                self,
                self._catalog_column("typtype", "t"),
                self._catalog_literal(
                    ("e", "d", "c", "r", "m")
                    if self.supports_multirange_type()
                    else ("e", "d", "c", "r")
                ),
            ),
        ]
        if type_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("typname", "t"),
                    self._catalog_value(type_name),
                )
            )

        return self._catalog_select(
            projection,
            source,
            conditions,
            [
                self._catalog_column("nspname", "n"),
                self._catalog_column("typname", "t"),
                self._catalog_column("enumsortorder", "e"),
                self._catalog_column("attnum", "a"),
            ],
        )

    def format_operator_list_query_statement(
        self, expr: "PostgresOperatorListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the operator listing from ``pg_operator``.

        ``oprkind`` names the shape (binary/prefix/postfix); an operand the
        operator does not have is OID 0 and is reported as NULL rather than
        formatted as a type. The implementing function is joined by OID and
        the owner from ``pg_roles``.

        Args:
            expr: Operator list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        operator_name = params.get("operator_name")

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_operator", "o"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oprnamespace", "o"),
                self._catalog_column("oid", "n"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_proc", "p"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oprcode", "o"),
                self._catalog_column("oid", "p"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_roles", "r"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oprowner", "o"),
                self._catalog_column("oid", "r"),
            ),
        )

        projection = [
            self._catalog_column("oprname", "o", alias="name"),
            self._catalog_column("nspname", "n", alias="schema_name"),
            self._catalog_column("oprkind", "o", alias="operator_kind"),
            self._optional_type_name_call(
                self, self._catalog_column("oprleft", "o"), alias="left_type"
            ),
            self._optional_type_name_call(
                self, self._catalog_column("oprright", "o"), alias="right_type"
            ),
            self._type_name_call(
                self, self._catalog_column("oprresult", "o"), alias="result_type"
            ),
            self._catalog_column("proname", "p", alias="function_name"),
            self._catalog_column("rolname", "r", alias="owner"),
            FunctionCall(
                self,
                "pg_catalog.obj_description",
                self._catalog_column("oid", "o"),
                self._catalog_literal("pg_operator"),
                alias="comment",
            ),
        ]

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            )
        ]
        if operator_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("oprname", "o"),
                    self._catalog_value(operator_name),
                )
            )

        return self._catalog_select(
            projection,
            source,
            conditions,
            [
                self._catalog_column("nspname", "n"),
                self._catalog_column("oprname", "o"),
                self._catalog_column("oid", "o"),
            ],
        )

    def format_operator_class_list_query_statement(
        self, expr: "PostgresOperatorClassListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the operator-class listing from ``pg_opclass``.

        ``opcmethod`` is the index access method (btree, gist, ...), joined
        through ``pg_am``; ``opcintype`` is the input type and ``opckeytype``
        the storage key type, absent when zero.

        Args:
            expr: Operator-class list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        operator_class_name = params.get("operator_class_name")

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_opclass", "oc"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("opcnamespace", "oc"),
                self._catalog_column("oid", "n"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_am", "am"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("opcmethod", "oc"),
                self._catalog_column("oid", "am"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_opfamily", "f"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("opcfamily", "oc"),
                self._catalog_column("oid", "f"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_roles", "r"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("opcowner", "oc"),
                self._catalog_column("oid", "r"),
            ),
        )

        projection = [
            self._catalog_column("opcname", "oc", alias="name"),
            self._catalog_column("nspname", "n", alias="schema_name"),
            self._catalog_column("amname", "am", alias="method"),
            self._type_name_call(
                self, self._catalog_column("opcintype", "oc"), alias="input_type"
            ),
            self._optional_type_name_call(
                self, self._catalog_column("opckeytype", "oc"), alias="key_type"
            ),
            self._catalog_column("opcdefault", "oc", alias="is_default"),
            self._catalog_column("opfname", "f", alias="family"),
            self._catalog_column("rolname", "r", alias="owner"),
            FunctionCall(
                self,
                "pg_catalog.obj_description",
                self._catalog_column("oid", "oc"),
                self._catalog_literal("pg_opclass"),
                alias="comment",
            ),
        ]

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            )
        ]
        if operator_class_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("opcname", "oc"),
                    self._catalog_value(operator_class_name),
                )
            )

        return self._catalog_select(
            projection,
            source,
            conditions,
            [
                self._catalog_column("nspname", "n"),
                self._catalog_column("opcname", "oc"),
                self._catalog_column("oid", "oc"),
            ],
        )

    def format_operator_family_list_query_statement(
        self, expr: "PostgresOperatorFamilyListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the operator-family listing from ``pg_opfamily``.

        Args:
            expr: Operator-family list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        operator_family_name = params.get("operator_family_name")

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_opfamily", "f"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("opfnamespace", "f"),
                self._catalog_column("oid", "n"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_am", "am"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("opfmethod", "f"),
                self._catalog_column("oid", "am"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_roles", "r"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("opfowner", "f"),
                self._catalog_column("oid", "r"),
            ),
        )

        projection = [
            self._catalog_column("opfname", "f", alias="name"),
            self._catalog_column("nspname", "n", alias="schema_name"),
            self._catalog_column("amname", "am", alias="method"),
            self._catalog_column("rolname", "r", alias="owner"),
            FunctionCall(
                self,
                "pg_catalog.obj_description",
                self._catalog_column("oid", "f"),
                self._catalog_literal("pg_opfamily"),
                alias="comment",
            ),
        ]

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            )
        ]
        if operator_family_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("opfname", "f"),
                    self._catalog_value(operator_family_name),
                )
            )

        return self._catalog_select(
            projection,
            source,
            conditions,
            [
                self._catalog_column("nspname", "n"),
                self._catalog_column("opfname", "f"),
                self._catalog_column("oid", "f"),
            ],
        )

    def format_ddl_statement_query_statement(
        self, expr: "DDLStatementQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format an assembled ``CREATE TABLE`` skeleton for one table.

        Assembled, not retrieved: the address comes from the catalog and the
        expressions would come from ``pg_get_expr``, but the storage
        parameters and per-column clauses are not reassembled here -- a
        partial ``CREATE TABLE`` that renders as if it were complete is worse
        than none. So this answers with ``ddl`` empty and ``native=False``,
        which the parser drops, so ``get_ddl_statement`` returns ``None``:
        an honest "cannot reproduce this" rather than a plausible-looking
        fragment.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        object_name = params.get("object_name", "")

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_class", "c"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self, "eq", self._catalog_column("relname", "c"), self._catalog_value(object_name)
            ),
        ]
        projections = [
            self._catalog_column("nspname", "n").as_("schema_name"),
            self._catalog_column("relname", "c").as_("object_name"),
            # ``relkind`` is the catalog's own kind letter, so ``object_type``
            # is read from the row rather than asserted as a constant: the
            # query does not need to know which kind was asked for, and a
            # constant here would be a fact the caller supplied pretending to
            # be one the catalog returned.
            self._catalog_column("relkind", "c").as_("object_type"),
            # No ``ddl`` column is projected: the parser drops rows carrying
            # no text, so ``get_ddl_statement`` returns None -- an honest
            # "cannot reproduce this" rather than a plausible fragment.
        ]
        return self._catalog_select(projections, source, conditions, None)


    # ========== Data Profiles (row-shaped answers) ==========
    #
    # The catalog answers these, not the rows: `pg_stats` holds one row per
    # analysed column, so the whole family costs one query per table however
    # large the table is. Everything it reports is an estimate of the last
    # ANALYZE, which is why the parser labels it `ESTIMATE` and carries the
    # analysis time beside the numbers.

    def _stats_source(self, table_name):
        """The catalog chain a per-column statistics query reads.

        ``pg_stats`` names its table by name, not by oid, so the relation has
        to be joined back through ``pg_namespace``/``pg_class`` to reach the
        row estimate (``reltuples``) and to read the analysis time from
        ``pg_stat_all_tables``. That last join is a LEFT one: a table the
        statistics collector has never seen is still a table, and its profile
        should come back with an unknown vintage rather than vanish.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
            LogicalPredicate,
        )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_class", "c"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_stats", "s"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("schemaname", "s"),
                    self._catalog_column("nspname", "n"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("tablename", "s"),
                    self._catalog_column("relname", "c"),
                ),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_stat_all_tables", "st"),
            join_type="LEFT",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("schemaname", "st"),
                    self._catalog_column("schemaname", "s"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "st"),
                    self._catalog_column("tablename", "s"),
                ),
            ),
        )
        return source

    def _histogram_projection(self):
        """``histogram_bounds`` as JSON, which is the only readable shape.

        ``pg_stats.histogram_bounds`` is declared ``anyarray`` -- a pseudo-type
        standing for "an array of whatever this column is" -- and PostgreSQL
        refuses to subscript it (``cannot subscript type anyarray``) as well as
        ``MIN(anyelement)`` over it, since a type without a btree opclass has no
        aggregate. ``array_to_json`` is declared over ``anyarray``, so that is
        the one call that accepts it as it is: the array travels as JSON and
        the parser takes its endpoints.

        The cost is that a bound arrives as a JSON scalar rather than the
        driver's own object: numbers stay numbers, while a date, a timestamp or
        a UUID reads as its text form. That is a limit of ``pg_stats`` rather
        than a choice -- nothing in the catalog can hand back a typed endpoint
        without knowing the column's type at plan time -- and it is stated
        where a reader will look.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall

        return FunctionCall(
            self,
            "array_to_json",
            self._catalog_column("histogram_bounds", "s"),
            alias="histogram_bounds",
        )

    def format_column_data_profile_query_statement(
        self, expr: "ColumnDataProfileQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the per-column profile from ``pg_stats``.

        One row per analysed column, projected from the statistics the last
        ``ANALYZE`` left: ``null_frac``, ``n_distinct``, the histogram's
        endpoints, the kept most-common values, and the moment the figures
        were collected.

        ``n_distinct`` is projected raw rather than converted here because its
        sign carries the meaning: a negative value is the *fraction* of rows
        that are distinct, so turning it into a count needs ``reltuples`` -- a
        comparison between two columns, not a constant -- and the conversion
        belongs next to the row estimate it uses.

        The three arrays all travel as JSON, for the same reason: ``anyarray``
        is a pseudo-type whose element type is the profiled column's own, so no
        common cast exists and no aggregate is defined over it.
        ``array_to_json`` yields typed JSON scalars, which is the one shape that
        survives every element type; see :meth:`_histogram_projection` for what
        that costs min/max.

        A column with no statistics row simply is not in the answer, and a
        never-analysed table answers with no rows at all -- "not measured" as
        an absence, not as a zero-filled profile.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        table_name = params.get("table_name")

        source = self._stats_source(table_name)
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self, "eq", self._catalog_column("relname", "c"), self._catalog_value(table_name)
            ),
        ]
        projections = [
            self._catalog_column("attname", "s", alias="column_name"),
            self._catalog_column("schemaname", "s", alias="schema_name"),
            self._catalog_column("null_frac", "s", alias="null_fraction"),
            self._catalog_column("n_distinct", "s"),
            self._histogram_projection(),
            self._catalog_column("reltuples", "c", alias="row_count"),
            self._catalog_column("correlation", "s"),
            FunctionCall(
                self,
                "array_to_json",
                self._catalog_column("most_common_vals", "s"),
                alias="most_common_vals",
            ),
            FunctionCall(
                self,
                "array_to_json",
                self._catalog_column("most_common_freqs", "s"),
                alias="most_common_freqs",
            ),
            self._catalog_column("last_analyze", "st"),
            self._catalog_column("last_autoanalyze", "st"),
        ]
        return self._catalog_select(
            projections, source, conditions, [self._catalog_column("attname", "s")]
        )

    def format_column_value_sample_query_statement(
        self, expr: "ColumnValueSampleQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format a value listing as the two arrays ``pg_stats`` already keeps.

        ANALYZE stores the most common values and their frequencies in
        lockstep, so the listing is a projection and not an aggregate -- the
        one engine where this probe is as cheap as the profile.

        The pair is projected whole and paired by position in the parser
        rather than unnested in SQL: the two arrays are of different element
        types, and keeping them side by side means the caller gets the values
        in the column's own types instead of a cast-to-text union.

        The requested ``limit`` is applied on the parser side, because what
        arrives is a fixed-size list -- ANALYZE kept at most
        ``statistics_target`` entries and there is nothing to slice in SQL
        without unnesting.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        table_name = params.get("table_name")
        column_name = params.get("column_name")

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("schemaname", "s"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self, "eq", self._catalog_column("tablename", "s"), self._catalog_value(table_name)
            ),
            ComparisonPredicate(
                self, "eq", self._catalog_column("attname", "s"), self._catalog_value(column_name)
            ),
        ]
        projections = [
            FunctionCall(
                self,
                "array_to_json",
                self._catalog_column("most_common_vals", "s"),
                alias="most_common_vals",
            ),
            FunctionCall(
                self,
                "array_to_json",
                self._catalog_column("most_common_freqs", "s"),
                alias="most_common_freqs",
            ),
        ]
        return self._catalog_select(
            projections, self._catalog_relation("pg_stats", "s"), conditions, None
        )

    def _user_relation(self, table_name: str, alias: str, schema: str):
        """A user table as a FROM source, quoted by the dialect.

        The opposite of :meth:`_catalog_relation`: ``pg_catalog`` spells its
        own names lower-case and never quoted, while a user table is whatever
        its owner declared -- mixed case, a space, a reserved word -- so the
        name, its schema and the alias are all left to the dialect's identifier
        renderer. The alias is what tells the two sides of a coverage
        measurement apart, and it is quoted for the same reason.
        """
        from rhosocial.activerecord.backend.expression.objects import Table
        from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

        return NamedRelationRef(
            self,
            Table(self, table_name, schema_name=schema),
            alias=alias,
        )

    def _coverage_fraction(self, column, source, other_column, other_source):
        """One side of the overlap: the share of *column* the other side holds.

        The numerator counts the *distinct* values of *column* that also occur
        in *other_column*, as ``COUNT(*)`` over a derived table of those
        distinct values; the denominator counts *column*'s distinct non-NULL
        values. Distinct on both sides is what makes the figure a coverage of
        values rather than of rows -- a key repeated a thousand times that
        matches once would otherwise read as well covered.

        ``IS NOT NULL`` is written on both halves rather than relied upon. In
        the numerator it is load-bearing: ``SELECT DISTINCT`` treats a NULL as
        one value, so counting it would credit the tested side with a match it
        does not have. In the divisor ``COUNT(DISTINCT ...)`` skips NULLs on
        its own, but the statement says so explicitly, because the share is of
        the values that could join and that is the fact being measured.
        ``NULLIF(..., 0)`` then makes a column with no value at all answer NULL
        rather than 0, which the parser reads as "no fraction": the difference
        between "nothing matched" and "nothing to match" survives to the
        caller.

        ``* 1.0`` is load-bearing on this engine: PostgreSQL divides integers
        by truncation, so ``COUNT(*) / COUNT(...)`` would answer 0 for every
        share below one.
        """
        from rhosocial.activerecord.backend.expression import (
            FunctionCall,
            InPredicate,
            Literal,
            LogicalPredicate,
            QueryStatement,
            RowCountWildcard,
            SelectModifier,
            Subquery,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.functions import CountFunction
        from rhosocial.activerecord.backend.expression.sources import DerivedTableSource

        overlap = QueryStatement(
            self,
            select=[column],
            from_=source,
            where=WhereClause(
                self,
                LogicalPredicate(
                    self,
                    "AND",
                    column.is_not_null(),
                    InPredicate(
                        self,
                        column,
                        Subquery(
                            self,
                            QueryStatement(
                                self,
                                select=[other_column],
                                from_=other_source,
                                where=WhereClause(self, other_column.is_not_null()),
                            ),
                        ),
                    ),
                ),
            ),
            select_modifier=SelectModifier.DISTINCT,
        )
        covered = Subquery(
            self,
            QueryStatement(
                self,
                select=[CountFunction(self, RowCountWildcard(self))],
                from_=DerivedTableSource(self, overlap, alias="covered"),
            ),
        )
        total = Subquery(
            self,
            QueryStatement(
                self,
                select=[
                    FunctionCall(
                        self,
                        "NULLIF",
                        CountFunction(self, column, distinct=True),
                        Literal(self, 0, inline=True),
                    )
                ],
                from_=source,
                where=WhereClause(self, column.is_not_null()),
            ),
        )
        return (covered * Literal(self, 1.0, inline=True)) / total

    def format_join_coverage_query_statement(
        self, expr: "JoinCoverageQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format a two-sided measurement of two columns' value domains.

        Unlike every other statement in this module, this one names two user
        tables rather than one catalogue relation: the question is a relation
        between two objects, so both sides are carried explicitly and their
        identifiers are the dialect's to quote (see :meth:`_user_relation`).
        ``_catalog_column`` is deliberately not used here -- it writes names
        the way ``pg_catalog`` spells them, which is not how a user's table is
        spelled.

        The row carries both fractions because neither is meaningful alone. A
        left share of 1.0 beside a right share of 0.2 says every tested value
        exists on the other side while most of that other domain is
        unaccounted for; a single similarity score would average the two and
        describe neither. A caller deciding whether a join is worth writing
        needs to know which direction is incomplete.

        Both figures are counted over *distinct* values, with NULLs excluded
        from the numerators and the denominators alike -- a NULL is not a value
        that could ever join, so it belongs in neither half of the fraction.
        The denominators are ``NULLIF``-guarded so a column with nothing to
        match answers NULL, which the parser reports as "no measurement"
        instead of a zero overlap that would read as a real answer. See
        :meth:`_coverage_fraction` for both rules in place.

        The cost is the two sides read for their distinct domains plus an
        intersection of the two -- a double scan, and the reason the probe
        belongs behind an explicit call rather than in any automatic pass.

        Collations are not reconciled. PostgreSQL compares the two columns
        under whichever collation applies, so values that the database would
        treat as equal may be counted as distinct here, and the fractions are
        a *lower* bound across mismatched collations -- never an upper one.
        """
        from rhosocial.activerecord.backend.expression import Column, QueryStatement

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema

        left_source = self._user_relation(params.get("left_table"), "l", schema)
        right_source = self._user_relation(params.get("right_table"), "r", schema)
        left_column = Column(self, params.get("left_column"), table="l")
        right_column = Column(self, params.get("right_column"), table="r")

        return QueryStatement(
            self,
            select=[
                self._coverage_fraction(
                    left_column, left_source, right_column, right_source
                ).as_("left_coverage"),
                self._coverage_fraction(
                    right_column, right_source, left_column, left_source
                ).as_("right_coverage"),
            ],
        ).to_sql()


__all__ = ["PostgresIntrospectionCapabilityMixin"]
