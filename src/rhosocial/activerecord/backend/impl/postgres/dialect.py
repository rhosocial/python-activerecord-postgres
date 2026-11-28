# src/rhosocial/activerecord/backend/impl/postgres/dialect.py
"""
PostgreSQL backend SQL dialect implementation.

This dialect answers protocols for features that PostgreSQL actually supports,
based on the PostgreSQL version provided at initialization.

Conformance is structural: the mixins below provide every protocol member, so
``isinstance(dialect, SomeSupport)`` passes without any Protocol class being a
base. A ``runtime_checkable`` Protocol is a declaration, and inheriting one
would only add its ellipsis-bodied members to the MRO -- members that answer
truthily and make ``isinstance`` pass for behaviour nothing implements.
"""

from typing import Mapping, Tuple, Optional, cast, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.schema.differ import SchemaDiffer

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
from rhosocial.activerecord.backend.dialect.mixins import (
    # Named objects. Each *NameMixin supplies one ``format_<kind>_object`` and
    # subclasses NamespaceMixin, so each has to precede it in the base list.
    NamespaceMixin,
    TableObjectMixin,
    ViewObjectMixin,
    MaterializedViewObjectMixin,
    ForeignTableObjectMixin,
    IndexObjectMixin,
    SequenceObjectMixin,
    TriggerObjectMixin,
    RoutineObjectMixin,
    TypeObjectMixin,
    SynonymObjectMixin,
    SchemaNameMixin,
    DatabaseNameMixin,
    PropertyGraphNameMixin,
    RelationSourceMixin,
    SQLXMLMixin,
    CollationMixin,
    CTEMixin,
    WindowFunctionMixin,
    JSONMixin,
    UUIDMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
    GraphTableMixin,
    MergeMixin,
    TemporalTableMixin,
    UpsertMixin,
    LateralJoinMixin,
    JoinMixin,
    ViewMixin,
    SchemaMixin,
    IndexMixin,
    SequenceMixin,
    TableMixin,
    SetOperationMixin,
    TruncateMixin,
    ILIKEMixin,
    ConstraintMixin,
    PartitionMixin,
    # TRIM/LPAD/RPAD/REPEAT are nodes with default formatters, and PostgreSQL
    # spells all four natively -- ``TRIM([BOTH|LEADING|TRAILING] [chars] FROM
    # str)``, ``LPAD``/``RPAD(str, len, padstr)`` and ``REPEAT(str, n)`` -- so
    # the shared defaults are the answer here and no override is needed.
    LpadMixin,
    RepeatMixin,
    RpadMixin,
    TrimMixin,
    # New Mixins
    PredicateMixin,
    ExpressionMixin,
    DateTimeMixin,
    DQLMixin,
    DMLMixin,
    DDLColumnMixin,
    ColumnAttributeMixin,
    TransactionControlMixin,
    UserDefinedTypeMixin,
    DomainMixin,
)
from .mixins import (
    PostgresHintedStatementsMixin,
    PostgresObjectNameMixin,
    PostgresExtensionMixin,
    PostgresMaterializedViewMixin,
    PostgresTableMixin,
    PostgresPgvectorMixin,
    PostgresPostGISMixin,
    PostgresPostgisRasterMixin,
    PostgresPgroutingMixin,
    PostgresPgTrgmMixin,
    PostgresPgroongaMixin,
    PostgresPgHintPlanMixin,
    PostgresUnaccentMixin,
    PostgresAmcheckMixin,
    PostgresPageinspectMixin,
    PostgresPgstattupleMixin,
    PostgresPgLogicalinspectMixin,
    PostgresPgJsonSchemaMixin,
    PostgreTimescaleDBMixin,
    PostgresHstoreMixin,
    # Native feature mixins
    PostgresPartitionMixin,
    PostgresPropertyGraphQueryMixin,
    PostgresIndexMixin,
    PostgresVacuumMixin,
    PostgresCopyMixin,
    PostgresQueryOptimizationMixin,
    PostgresDataTypeMixin,
    PostgresLogicalReplicationMixin,
    PostgresParallelQueryMixin,
    PostgresColumnTypeMixin,
    # Per-feature mixins
    PostgresCTEMixin,
    PostgresWindowMixin,
    PostgresFilterMixin,
    PostgresReturningMixin,
    PostgresGroupingMixin,
    PostgresExplainMixin,
    PostgresMergeMixin,
    PostgresUpsertMixin,
    PostgresLateralJoinMixin,
    PostgresSetOperationMixin,
    PostgresILIKEMixin,
    PostgresJoinMixin,
    PostgresTruncateMixin,
    PostgresSchemaMixin,
    PostgresNamespaceMixin,
    PostgresDatabaseMixin,
    PostgresSequenceMixin,
    PostgresTransactionMixin,
    PostgresViewMixin,
    PostgresXMLMixin,
    PostgresCollationMixin,
    PostgresOrderedSetAggMixin,
    PostgresFeaturesMixin,
    # SHOW is not a catalog query, so it has its own mixin.
    PostgresShowMixin,
    # Extension feature mixins
    PostgresLtreeMixin,
    PostgresIntarrayMixin,
    PostgresEarthdistanceMixin,
    PostgresTablefuncMixin,
    PostgresPgStatStatementsMixin,
    PostgresCitextMixin,
    PostgresPgcryptoMixin,
    PostgresFuzzystrmatchMixin,
    PostgresCubeMixin,
    PostgresUuidOssMixin,
    PostgresBloomMixin,
    PostgresBtreeGinMixin,
    PostgresBtreeGistMixin,
    PostgresPgCronMixin,
    PostgresPgPartmanMixin,
    PostgresPgSurgeryMixin,
    PostgresPgWalinspectMixin,
    PostgresPgLogicalMixin,
    PostgresPgauditMixin,
    PostgresPgRepackMixin,
    PostgresHypoPgMixin,
    PostgresOrafceMixin,
    PostgresAddressStandardizerMixin,
    # DDL feature mixins
    PostgresTriggerMixin,
    PostgresCommentMixin,
    PostgresTypeMixin,
    PostgresConstraintMixin,
    PostgresPolicyMixin,
    PostgresRlsConfigMixin,
    PostgresAlterTableSettingsMixin,
    PostgresClusterMixin,
    PostgresDomainMixin,
    PostgresRepackMixin,
    PostgresCollationDDLMixin,
    PostgresForeignTableMixin,
    PostgresRoutineMixin,
    PostgresPublicationMixin,
    # Type mixins
    EnumTypeMixin,
    TypesDataTypeMixin,
    MultirangeMixin,
    PostgresFullTextSearchMixin,
    PostgresRangeTypeMixin,
    PostgresJSONBEnhancedMixin,
    PostgresUUIDMixin,
    PostgresArrayEnhancedMixin,
    PostgresTypeFormatSupportMixin,
    # DDL/DML operation mixins (new)
    PostgresExtendedStatisticsMixin,
    PostgresStoredProcedureMixin,
    PostgresAdvisoryLockMixin,
    PostgresLockingMixin,
    # Introspection capability mixin
    PostgresIntrospectionCapabilityMixin,
    PostgresAlterColumnModifierMixin,
    # Newly extracted mixins
    PostgresDateTimeMixin,
    PostgresDQLMixin,
    PostgresJSONMixin,
    PostgresGeneratedColumnMixin,
    PostgresIdentityColumnMixin,
    PostgresAutoIncrementColumnMixin,
    PostgresExpressionMixin,
    PostgresFunctionMixin,
    PostgresLiteralValueMixin,
)
from .suggestion import (
    PostgresColumnReorderSuggestionMixin,
    PostgresCreateTableLayoutSuggestionMixin,
    PostgresDataTypeSuggestionsMixin,
    PostgresStringConcatenationSuggestionMixin,
    PostgresGeneratedValueSuggestionMixin,
    PostgresPartitionAddSuggestionMixin,
    PostgresPartitionSchemeChangeSuggestionMixin,
    PostgresPartitionBuildSuggestionMixin,
    PostgresPartitionDropSuggestionMixin,
    PostgresPartitionMergeSuggestionMixin,
    PostgresPartitionRetentionSuggestionMixin,
    PostgresPartitionSplitSuggestionMixin,
    PostgresPartitionSuggestionBase,
    PostgresPartitionTableAlterSuggestionMixin,
    PostgresPartitionTableDropSuggestionMixin,
    PostgresTableConstraintChangeSuggestionMixin,
    PostgresTableIndexChangeSuggestionMixin,
)

from .reserved_words import POSTGRESQL_RESERVED_WORDS


class PostgresDialect(
    # Backend suggestions first: no core mixin may shadow their answers. The
    # eight partition answer mixins are one per protocol; their shared helper
    # base is listed once, last among the suggestions, so C3 stays unambiguous.
    PostgresCreateTableLayoutSuggestionMixin,
    PostgresPartitionBuildSuggestionMixin,
    PostgresPartitionTableDropSuggestionMixin,
    PostgresPartitionTableAlterSuggestionMixin,
    PostgresPartitionAddSuggestionMixin,
    PostgresPartitionDropSuggestionMixin,
    PostgresPartitionSplitSuggestionMixin,
    PostgresPartitionMergeSuggestionMixin,
    PostgresPartitionRetentionSuggestionMixin,
    # Migration change points: the scheme answer delegates to the eight above.
    PostgresPartitionSchemeChangeSuggestionMixin,
    PostgresColumnReorderSuggestionMixin,
    PostgresTableConstraintChangeSuggestionMixin,
    PostgresTableIndexChangeSuggestionMixin,
    PostgresGeneratedValueSuggestionMixin,
    PostgresStringConcatenationSuggestionMixin,
    PostgresPartitionSuggestionBase,
    # The bases below come in bundles: a backend mixin, then the core bundle
    # it stands in for. The backend's answer has to come first -- a mixin
    # placed after the class it overrides never wins -- so a new mixin goes
    # immediately before the core mixin it overrides, and the pairing is read
    # off the list. Two positions sit outside that pairing and are called out
    # where they occur: a mixin that overrides a method ``SQLDialectBase``
    # defines has to precede the base, and a mixin that subclasses a core
    # mixin has to precede it too (C3: a subclass precedes its base).
    # Ahead of SQLDialectBase, unlike every other mixin here. Those add methods
    # the base does not have, so their position is a readability choice; this
    # one overrides ``format_literal``, and a mixin placed after the class it
    # overrides never wins. It resolves PostgreSQL's three tier-2 spellings --
    # bytea, uuid and interval -- and leaves the base's portable forms and the
    # literal-construction protocol to it.
    PostgresLiteralValueMixin,
    SQLDialectBase,
    PostgresTypeMixin,
    PostgresDomainMixin,
    UserDefinedTypeMixin,
    DomainMixin,
    # Named objects. Each *NameMixin subclasses NamespaceMixin, so each has to
    # precede it in this list (C3: a subclass precedes its base) or the base's
    # format_qualified_name would shadow the per-kind formatter.
    TableObjectMixin,
    ViewObjectMixin,
    MaterializedViewObjectMixin,
    ForeignTableObjectMixin,
    IndexObjectMixin,
    SequenceObjectMixin,
    TriggerObjectMixin,
    RoutineObjectMixin,
    TypeObjectMixin,
    SynonymObjectMixin,
    SchemaNameMixin,
    DatabaseNameMixin,
    PropertyGraphNameMixin,
    RelationSourceMixin,
    PostgresObjectNameMixin,
    # PG-specific mixins (before global mixins to override)
    PostgresDateTimeMixin,
    PostgresDQLMixin,
    PostgresJSONMixin,
    PostgresGeneratedColumnMixin,
    PostgresIdentityColumnMixin,
    PostgresAutoIncrementColumnMixin,
    PostgresExpressionMixin,
    PostgresFunctionMixin,
    # Per-feature mixins
    PostgresCTEMixin,
    PostgresWindowMixin,
    PostgresFilterMixin,
    PostgresReturningMixin,
    PostgresGroupingMixin,
    # One bundle, eight derived-statement formatters: it overrides the four
    # DQL/DML formatters, EXPLAIN, CTAS, the two materialized-view formatters
    # and CREATE VIEW's refusal. Each overridden statement's provider comes
    # after it here -- PostgresExplainMixin / PostgresViewMixin below,
    # PostgresMaterializedViewMixin and TableMixin (CreateTableAsMixin) further
    # down, and DQLMixin/DMLMixin last -- and a mixin placed after the class it
    # overrides never wins. It calls PostgresPgHintPlanMixin's private comment
    # helper through ``self``, so that mixin's later position does not matter
    # for method lookup.
    PostgresHintedStatementsMixin,
    PostgresExplainMixin,
    PostgresMergeMixin,
    PostgresUpsertMixin,
    PostgresLateralJoinMixin,
    PostgresSetOperationMixin,
    PostgresILIKEMixin,
    PostgresJoinMixin,
    PostgresTruncateMixin,
    PostgresSchemaMixin,
    # PostgreSQL's naming side: which namespace levels exist (a database outside,
    # a schema inside) and how they are spelled. Placed before NamespaceMixin
    # because it subclasses it (C3: a subclass precedes its base) -- otherwise
    # core's always-False defaults and core's two-slot spelling would win. After
    # the *NameMixin block for the same reason: those subclass NamespaceMixin too,
    # and each supplies one format_<kind>_object.
    PostgresNamespaceMixin,
    NamespaceMixin,
    PostgresDatabaseMixin,
    PostgresSequenceMixin,
    PostgresTransactionMixin,
    PostgresViewMixin,
    PostgresXMLMixin,
    PostgresCollationMixin,
    PostgresOrderedSetAggMixin,
    PostgresFeaturesMixin,
    # SHOW is not a catalog query: it names one run-time parameter. It lives in
    # its own mixin, the same split MySQL and MariaDB make between their
    # ``show/dialect.py`` and their introspection formatters.
    PostgresShowMixin,
    # TRIM/LPAD/RPAD/REPEAT are nodes with default formatters, and PostgreSQL
    # spells all four natively: ``TRIM([BOTH|LEADING|TRAILING] [chars] FROM
    # str)``, ``LPAD``/``RPAD(str, len, padstr)`` and ``REPEAT(str, n)``
    # (functions-string.html; ``TRIM(BOTH 'xy' FROM 'xyxab')`` live-verified
    # 2026-10-09 as the set form). No override is needed for any of them.
    LpadMixin,
    RepeatMixin,
    RpadMixin,
    TrimMixin,
    SQLXMLMixin,
    CollationMixin,
    SetOperationMixin,
    TruncateMixin,
    ILIKEMixin,
    CTEMixin,
    WindowFunctionMixin,
    PostgresJSONBEnhancedMixin,
    JSONMixin,
    # PG's UUID answer (version 13+ built-in, or uuid-ossp) must win over
    # the core UUIDMixin table, which has a single fixed spelling per operation.
    PostgresUUIDMixin,
    UUIDMixin,
    # Before ArrayMixin: this answers True where the core answers False, and
    # it was listed in the backend block further down, so the core default
    # was what every caller actually saw.
    PostgresArrayEnhancedMixin,
    ArrayMixin,
    ExplainMixin,
    # Must precede GraphMixin/GraphTableMixin so that the explicit-override
    # probes here win the MRO lookup; the core mixins would otherwise answer
    # from their own always-False defaults and ignore graph_feature_overrides.
    PostgresPropertyGraphQueryMixin,
    GraphMixin,
    GraphTableMixin,
    PostgresLockingMixin,
    MergeMixin,
    TemporalTableMixin,
    UpsertMixin,
    LateralJoinMixin,
    JoinMixin,
    PostgresMaterializedViewMixin,  # Before ViewMixin to override format_*_materialized_view_*
    ViewMixin,
    SchemaMixin,
    PostgresIndexMixin,
    IndexMixin,
    SequenceMixin,
    # PostgreSQL-specific mixins
    PostgresExtensionMixin,
    PostgresAlterColumnModifierMixin,  # Before TableMixin/ConstraintMixin to override format_*_action
    PostgresTableMixin,  # Before TableMixin to override supports_create_table_like
    PostgresCommentMixin,  # Before TableMixin to override format_comment_on_statement
    TableMixin,
    PostgresConstraintMixin,
    ConstraintMixin,
    PostgresPartitionMixin,
    PartitionMixin,
    PostgresIntrospectionCapabilityMixin,
    PostgresPgvectorMixin,
    PostgresPostGISMixin,
    PostgresPostgisRasterMixin,
    PostgresPgroutingMixin,
    PostgresPgTrgmMixin,
    PostgresPgroongaMixin,
    PostgresPgHintPlanMixin,
    PostgresUnaccentMixin,
    PostgresAmcheckMixin,
    PostgresPageinspectMixin,
    PostgresPgstattupleMixin,
    PostgresPgLogicalinspectMixin,
    PostgresPgJsonSchemaMixin,
    PostgreTimescaleDBMixin,
    PostgresHstoreMixin,
    # Native feature mixins
    PostgresVacuumMixin,
    PostgresCopyMixin,
    PostgresQueryOptimizationMixin,
    # Suggested data types: the DDL half of the same answer. Ahead of the
    # mixin that withholds the table, so this dialect's answers win.
    PostgresDataTypeSuggestionsMixin,
    PostgresDataTypeMixin,
    PostgresLogicalReplicationMixin,
    PostgresParallelQueryMixin,
    # Column types. After the DataType mixin above because that is the
    # sibling decision -- this one says which operations a value carries, the
    # other says how it is spelled. Nothing else in the list can answer for a
    # common Python type, so the MRO leaves the placement free; putting the two
    # side by side is what makes the separation legible to the next reader.
    PostgresColumnTypeMixin,
    # Extension feature mixins
    PostgresLtreeMixin,
    PostgresIntarrayMixin,
    PostgresEarthdistanceMixin,
    PostgresTablefuncMixin,
    PostgresPgStatStatementsMixin,
    PostgresCitextMixin,
    PostgresPgcryptoMixin,
    PostgresFuzzystrmatchMixin,
    PostgresCubeMixin,
    PostgresUuidOssMixin,
    PostgresBloomMixin,
    PostgresBtreeGinMixin,
    PostgresBtreeGistMixin,
    PostgresPgCronMixin,
    PostgresPgPartmanMixin,
    PostgresPgSurgeryMixin,
    PostgresPgWalinspectMixin,
    PostgresPgLogicalMixin,
    PostgresPgauditMixin,
    PostgresPgRepackMixin,
    PostgresHypoPgMixin,
    PostgresOrafceMixin,
    PostgresAddressStandardizerMixin,
    # DDL feature mixins
    PostgresTriggerMixin,
    PostgresPolicyMixin,
    PostgresRlsConfigMixin,
    PostgresAlterTableSettingsMixin,
    PostgresClusterMixin,
    PostgresRepackMixin,
    PostgresCollationDDLMixin,
    PostgresForeignTableMixin,
    PostgresRoutineMixin,
    PostgresPublicationMixin,
    # Type mixins
    EnumTypeMixin,
    TypesDataTypeMixin,
    MultirangeMixin,
    PostgresFullTextSearchMixin,
    PostgresRangeTypeMixin,
    PostgresTypeFormatSupportMixin,
    # DDL/DML operation mixins (new)
    PostgresExtendedStatisticsMixin,
    PostgresStoredProcedureMixin,
    PostgresAdvisoryLockMixin,
    # New Mixins
    PredicateMixin,
    ExpressionMixin,
    DateTimeMixin,
    DQLMixin,
    DMLMixin,
    DDLColumnMixin,
    ColumnAttributeMixin,
    TransactionControlMixin,
):
    """
    PostgreSQL dialect implementation that adapts to the PostgreSQL version.

    PostgreSQL features and support based on version:
    - Basic and recursive CTEs (since 8.4)
    - Window functions (since 8.4)
    - RETURNING clause (since 8.2)
    - JSON operations (since 9.2, JSONB since 9.4)
    - FILTER clause (since 9.4)
    - UPSERT (ON CONFLICT) (since 9.5)
    - MERGE statement (since 15)
    - Advanced grouping (CUBE, ROLLUP, GROUPING SETS) (since 9.5)
    - Array types (since early versions)
    - LATERAL joins (since 9.3)
    - Parallel query execution (since 9.6)
    - Stored procedures with CALL (since 11)
    - Extended statistics (since 10)

    PostgreSQL-specific features:
    - Table inheritance (INHERITS)
    - CONCURRENTLY refresh for materialized views (since 9.4)
    - Extension detection (PostGIS, pgvector, pg_trgm, hstore, etc.)

    Note: Extension features require the extension to be installed in the database.
    Use introspect_and_adapt() to detect installed extensions automatically.
    """

    # PostgreSQL function version support: function_name -> (min_version, max_version)
    # Function version requirements are defined in function_versions.py,
    # categorized by topic (JSON Path, Range, hstore, pgvector, PostGIS, etc.)
    # and assembled into POSTGRES_FUNCTION_VERSIONS.
    from .function_versions import POSTGRES_FUNCTION_VERSIONS as _FV

    _POSTGRES_FUNCTION_VERSIONS = _FV

    def __init__(
        self,
        version: Optional[Tuple[int, int, int]] = None,
        *,
        graph_feature_overrides: Optional[Mapping[str, bool]] = None,
    ):
        """
        Initialize PostgreSQL dialect with specific version.

        Args:
            version: PostgreSQL version tuple (major, minor, patch).
                If None, the dialect must be adapted via
                backend.introspect_and_adapt() before version-dependent
                features can be used.
            graph_feature_overrides: Explicit opt-ins for SQL/PGQ property graph
                features, keyed by ``"graph_match"`` / ``"graph_table"``.

                Left unset, property graph support stays off for every version.
                This is deliberate: SQL/PGQ was withdrawn in PostgreSQL 19 Beta 4
                and shipped in no earlier release, so a version check can never
                be evidence of support. Pass these only when the target server
                (or a compatibility layer in front of it) genuinely provides the
                feature.

        """
        super().__init__()
        self._reserved_words = POSTGRESQL_RESERVED_WORDS
        if version is not None:
            self.version = version

        overrides = dict(graph_feature_overrides or {})
        if any(not isinstance(name, str) for name in overrides):
            raise TypeError("Graph feature override names must be strings")
        unknown = overrides.keys() - self.GRAPH_FEATURE_NAMES
        if unknown:
            raise ValueError(f"Unknown graph feature overrides: {', '.join(sorted(unknown))}")
        if any(type(enabled) is not bool for enabled in overrides.values()):
            raise TypeError("Graph feature override values must be booleans")
        self._graph_feature_overrides = overrides

    def get_parameter_placeholder(self, position: int = 0) -> str:
        """psycopg uses '%s' for placeholders."""
        return "%s"

    def get_server_version(self) -> Tuple[int, int, int]:
        """Return the PostgreSQL version this dialect is configured for."""
        return self.version

    def supports_transactional_ddl(self) -> bool:
        """PostgreSQL DDL participates in transactions."""
        return True

    def supports_database_assigned_integer_column(self) -> bool:
        """Whether an integer column's value comes from the database.

        ``True``: SERIAL is a pseudo-type whose backing sequence supplies the
        value, and its observable behaviour is the one a declaration asks
        about -- omit the value on INSERT, the database supplies it, and the
        row can return it. The spelling is not ``AUTO_INCREMENT``
        (``supports_auto_increment_column`` stays ``False``), which is why the
        capability and the mechanism are separate questions.

        Not guaranteed: the MySQL counter behaviour. Inserting an explicit
        value larger than the sequence's current one raises MySQL's counter to
        ``max + 1``; a SERIAL column leaves its sequence untouched, so a later
        generated value may collide.
        """
        return True

    def supports_table_function(self) -> bool:
        """PostgreSQL reads a set-returning function as a row source.

        ``pg_ls_waldir()``, ``pg_control_checkpoint()`` and the rest of the
        server-information family are ordinary FROM sources, so the capability
        belongs to this dialect's surface. Core defaults it to False because
        several engines have no such form, and this dialect outranks the
        ``RelationSourceMixin`` default it would otherwise inherit.
        """
        return True

    def create_schema_differ(self) -> "SchemaDiffer":
        """Return the PostgreSQL schema differ for this dialect."""
        from rhosocial.activerecord.backend.impl.postgres.schema.differ import (
            PostgresSchemaDiffer,
        )

        return cast("SchemaDiffer", PostgresSchemaDiffer())
