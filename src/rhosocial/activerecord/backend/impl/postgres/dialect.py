# src/rhosocial/activerecord/backend/impl/postgres/dialect.py
"""
PostgreSQL backend SQL dialect implementation.

This dialect implements protocols for features that PostgreSQL actually supports,
based on the PostgreSQL version provided at initialization.
"""

from typing import Mapping, Tuple, Optional, cast, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.schema.differ import SchemaDiffer

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
from rhosocial.activerecord.backend.dialect.mixins import (
    # Named objects. Each *NameMixin supplies one ``format_<kind>_object`` and
    # subclasses NamespaceMixin, so each has to precede it in the base list.
    NamespaceMixin,
    TableNameMixin,
    ViewNameMixin,
    MaterializedViewNameMixin,
    ForeignTableNameMixin,
    IndexNameMixin,
    SequenceNameMixin,
    TriggerNameMixin,
    FunctionNameMixin,
    ProcedureNameMixin,
    TypeNameMixin,
    DomainNameMixin,
    SynonymNameMixin,
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
    # New Mixins
    PredicateMixin,
    ExpressionMixin,
    DateTimeMixin,
    DQLMixin,
    DMLMixin,
    DDLColumnMixin,
    TransactionControlMixin,
    UserDefinedTypeMixin,
    DomainMixin,
)
from rhosocial.activerecord.backend.dialect.protocols import (
    AlterDatabaseSupport,
    AlterDomainSupport,
    AlterTableSupport,
    AlterTypeSupport,
    CreateDatabaseSupport,
    CreateDomainSupport,
    CreateIndexSupport,
    CreateRoutineSupport,
    CreateSchemaSupport,
    CreateTableSupport,
    CreateTriggerSupport,
    CreateTypeSupport,
    CreateViewSupport,
    DropDatabaseSupport,
    DropDomainSupport,
    DropIndexSupport,
    DropRoutineSupport,
    DropSchemaSupport,
    DropTableSupport,
    DropTriggerSupport,
    DropTypeSupport,
    DropViewSupport,
    FulltextIndexSupport,
    MaterializedViewObjectSupport,
    MaterializedViewSupport,
    NamespaceSupport,
    RoutineObjectSupport,
    SequenceObjectSupport,
    TableObjectSupport,
    TriggerObjectSupport,
    TypeObjectSupport,
    IndexObjectSupport,
    ViewObjectSupport,
    ForeignTableObjectSupport,
    SQLXMLSupport,
    SQLXMLParsingSupport,
    SQLXMLSerializationSupport,
    SQLXMLConstructionSupport,
    SQLXMLAggregationSupport,
    SQLXMLQueryingSupport,
    CollationSupport,
    CTESupport,
    FilterClauseSupport,
    WindowFunctionSupport,
    ReturningSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ExplainSupport,
    GraphSupport,
    GraphTableSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    TemporalTableSupport,
    UpsertSupport,
    LateralJoinSupport,
    WildcardSupport,
    JoinSupport,
    SetOperationSupport,
    TruncateSupport,
    ILIKESupport,
    IntrospectionSupport,
    TransactionControlSupport,
    SQLFunctionSupport,
    DDLTypeSupport,
)
from .mixins import (
    PostgresObjectNameMixin,
    PostgresExtensionMixin,
    PostgresMaterializedViewMixin,
    PostgresTableMixin,
    PostgresPgvectorMixin,
    PostgresPostGISMixin,
    PostgresPostgisRasterMixin,
    PostgresPgroutingMixin,
    PostgresPgTrgmMixin,
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
    PostgresColumnSuggestionMixin,
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
    PostgresAutoIncrementMixin,
    PostgresExpressionMixin,
    PostgresFunctionMixin,
)

from .protocols.ddl.type import PostgresTypeSupport
from .protocols.ddl.domain import PostgresDomainSupport
from .reserved_words import POSTGRESQL_RESERVED_WORDS

# PostgreSQL-specific imports
from .protocols import (
    PostgresExtensionSupport,
    PostgresMaterializedViewSupport,
    PostgresTableSupport,
    PostgresPgvectorSupport,
    PostgresPostGISSupport,
    PostgresPostgisRasterSupport,
    PostgresPgroutingSupport,
    PostgresPgTrgmSupport,
    PostgresHstoreSupport,
    # Native feature protocols
    PostgresPartitionSupport,
    PostgresIndexSupport,
    PostgresVacuumSupport,
    PostgresCopySupport,
    PostgresQueryOptimizationSupport,
    PostgresDataTypeSupport,
    PostgresLogicalReplicationSupport,
    # Per-feature protocols
    PostgresCTESupport,
    PostgresWindowSupport,
    PostgresFilterSupport,
    PostgresReturningSupport,
    PostgresGroupingSupport,
    PostgresExplainSupport,
    PostgresMergeSupport,
    PostgresUpsertSupport,
    PostgresLateralJoinSupport,
    PostgresSetOperationSupport,
    PostgresILIKESupport,
    PostgresJoinSupport,
    PostgresTruncateSupport,
    PostgresSchemaSupport,
    PostgresNamespaceSupport,
    PostgresSequenceSupport,
    PostgresIdentitySupport,
    PostgresAutoIncrementSupport,
    PostgresTransactionSupport,
    PostgresViewSupport,
    PostgresXMLSupport,
    PostgresCollationSupport,
    PostgresOrderedSetAggSupport,
    PostgresFeaturesSupport,
    # Extension feature protocols
    PostgresLtreeSupport,
    PostgresIntarraySupport,
    PostgresEarthdistanceSupport,
    PostgresTablefuncSupport,
    PostgresPgStatStatementsSupport,
    PostgresCitextSupport,
    PostgresPgcryptoSupport,
    PostgresFuzzystrmatchSupport,
    PostgresCubeSupport,
    PostgresUuidOssSupport,
    PostgresBloomSupport,
    PostgresBtreeGinSupport,
    PostgresBtreeGistSupport,
    PostgresPgCronSupport,
    PostgresPgPartmanSupport,
    PostgresPgSurgerySupport,
    PostgresPgWalinspectSupport,
    PostgresPgLogicalSupport,
    PostgresPgauditSupport,
    PostgresPgRepackSupport,
    PostgresHypoPgSupport,
    PostgresOrafceSupport,
    PostgresAddressStandardizerSupport,
    # DDL feature protocols
    PostgresTriggerSupport,
    PostgresCommentSupport,
    PostgresConstraintSupport,
    PostgresPolicySupport,
    PostgresRlsConfigSupport,
    PostgresAlterTableSettingsSupport,
    PostgresClusterSupport,
    PostgresRepackSupport,
    PostgresCollationDDLSupport,
    PostgresForeignTableDDLSupport,
    PostgresRoutineDDLSupport,
    PostgresPublicationSupport,
    # Type feature protocols
    PostgresMultirangeSupport,
    PostgresEnumTypeSupport,
    PostgresFullTextSearchSupport,
    PostgresRangeTypeSupport,
    PostgresJSONBEnhancedSupport,
    PostgresArrayEnhancedSupport,
    # New feature protocols
    PostgresParallelQuerySupport,
    PostgresStoredProcedureSupport,
    PostgresExtendedStatisticsSupport,
    PostgresAdvisoryLockSupport,
    PostgresLockingSupport,
)


class PostgresDialect(
    SQLDialectBase,
    PostgresTypeMixin,
    PostgresDomainMixin,
    UserDefinedTypeMixin,
    DomainMixin,
    # Named objects. Each *NameMixin subclasses NamespaceMixin, so each has to
    # precede it in this list (C3: a subclass precedes its base) or the base's
    # format_qualified_name would shadow the per-kind formatter.
    TableNameMixin,
    ViewNameMixin,
    MaterializedViewNameMixin,
    ForeignTableNameMixin,
    IndexNameMixin,
    SequenceNameMixin,
    TriggerNameMixin,
    FunctionNameMixin,
    ProcedureNameMixin,
    TypeNameMixin,
    DomainNameMixin,
    SynonymNameMixin,
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
    PostgresAutoIncrementMixin,
    PostgresExpressionMixin,
    PostgresFunctionMixin,
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
    PostgresCommentMixin,  # Before TableMixin to override format_comment_statement
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
    PostgresHstoreMixin,
    # Native feature mixins
    PostgresVacuumMixin,
    PostgresCopyMixin,
    PostgresQueryOptimizationMixin,
    PostgresDataTypeMixin,
    PostgresLogicalReplicationMixin,
    PostgresParallelQueryMixin,
    # Column-type suggestions. After the DataType mixin above because that is the
    # sibling decision -- this one says which operations a value carries, the
    # other says how it is spelled. Nothing else in the list can answer for a
    # common Python type, so the MRO leaves the placement free; putting the two
    # side by side is what makes the separation legible to the next reader.
    PostgresColumnSuggestionMixin,
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
    TransactionControlMixin,
    # Protocol supports
    SQLXMLSupport,
    SQLXMLParsingSupport,
    SQLXMLSerializationSupport,
    SQLXMLConstructionSupport,
    SQLXMLAggregationSupport,
    SQLXMLQueryingSupport,
    CollationSupport,
    SetOperationSupport,
    TruncateSupport,
    ILIKESupport,
    CTESupport,
    FilterClauseSupport,
    WindowFunctionSupport,
    # JSONSupport is not listed: PostgresJSONBEnhancedSupport derives from it
    # below, and listing a base ahead of its own subclass is not a consistent
    # MRO. This is the shape every other derived protocol uses here --
    # PostgresTableSupport(TableSupport), PostgresIndexSupport(IndexSupport) --
    # and JSONSupport still reaches the MRO, so isinstance(dialect, JSONSupport)
    # is unchanged. Listing both, while each was standalone, also put
    # supports_json_type in the MRO four times: two abstract protocol
    # declarations, the core mixin default, and the one implementation.
    ReturningSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ExplainSupport,
    GraphSupport,
    GraphTableSupport,
    MergeSupport,
    OrderedSetAggregationSupport,
    QualifyClauseSupport,
    TemporalTableSupport,
    UpsertSupport,
    LateralJoinSupport,
    WildcardSupport,
    JoinSupport,
    # Type and domain DDL protocols. These derive TypeObjectSupport, so they
    # precede the object protocols further down.
    PostgresTypeSupport,
    PostgresDomainSupport,
    # These three declare the PostgreSQL-only additions to the per-statement
    # protocols below, so they derive Create/Drop/AlterTableSupport,
    # Create/DropIndexSupport + FulltextIndexSupport and
    # Create/DropTriggerSupport. A subclass precedes its base (C3), so each
    # has to come before the core protocols it extends.
    PostgresTableSupport,
    PostgresIndexSupport,
    PostgresTriggerSupport,
    # Per-statement DDL protocols. Core split each former umbrella protocol
    # into one protocol per statement expression, so a dialect that implements
    # CREATE/DROP TABLE declares those two rather than the old umbrella.
    CreateTableSupport,
    DropTableSupport,
    AlterTableSupport,
    CreateViewSupport,
    DropViewSupport,
    MaterializedViewSupport,
    CreateIndexSupport,
    DropIndexSupport,
    FulltextIndexSupport,
    # CREATE/DROP/ALTER SEQUENCE are declared once, on PostgresSequenceSupport,
    # which derives from the three core per-statement protocols. Listing them
    # here as well would put a base before the subclass that restates it and
    # break C3; the dialect still satisfies each through that derivation.
    CreateTriggerSupport,
    DropTriggerSupport,
    CreateTypeSupport,
    AlterTypeSupport,
    DropTypeSupport,
    CreateDomainSupport,
    AlterDomainSupport,
    DropDomainSupport,
    CreateSchemaSupport,
    DropSchemaSupport,
    CreateDatabaseSupport,
    DropDatabaseSupport,
    AlterDatabaseSupport,
    CreateRoutineSupport,
    DropRoutineSupport,
    # Introspection protocol
    IntrospectionSupport,
    # Transaction control protocol
    TransactionControlSupport,
    # PostgreSQL-specific protocols
    PostgresExtensionSupport,
    PostgresMaterializedViewSupport,
    PostgresPgvectorSupport,
    PostgresPostGISSupport,
    PostgresPostgisRasterSupport,
    PostgresPgroutingSupport,
    PostgresPgTrgmSupport,
    PostgresHstoreSupport,
    # Native feature protocols
    PostgresPartitionSupport,
    PostgresVacuumSupport,
    PostgresCopySupport,
    PostgresQueryOptimizationSupport,
    PostgresDataTypeSupport,
    PostgresLogicalReplicationSupport,
    # Per-feature protocols
    PostgresCTESupport,
    PostgresWindowSupport,
    PostgresFilterSupport,
    PostgresReturningSupport,
    PostgresGroupingSupport,
    PostgresExplainSupport,
    PostgresMergeSupport,
    PostgresUpsertSupport,
    PostgresLateralJoinSupport,
    PostgresSetOperationSupport,
    PostgresILIKESupport,
    PostgresJoinSupport,
    PostgresTruncateSupport,
    PostgresSchemaSupport,
    PostgresNamespaceSupport,
    PostgresSequenceSupport,
    PostgresIdentitySupport,
    PostgresAutoIncrementSupport,
    PostgresTransactionSupport,
    PostgresViewSupport,
    PostgresXMLSupport,
    PostgresCollationSupport,
    PostgresOrderedSetAggSupport,
    PostgresFeaturesSupport,
    # Extension feature protocols
    PostgresLtreeSupport,
    PostgresIntarraySupport,
    PostgresEarthdistanceSupport,
    PostgresTablefuncSupport,
    PostgresPgStatStatementsSupport,
    PostgresCitextSupport,
    PostgresPgcryptoSupport,
    PostgresFuzzystrmatchSupport,
    PostgresCubeSupport,
    PostgresUuidOssSupport,
    PostgresBloomSupport,
    PostgresBtreeGinSupport,
    PostgresBtreeGistSupport,
    PostgresPgCronSupport,
    PostgresPgPartmanSupport,
    PostgresPgSurgerySupport,
    PostgresPgWalinspectSupport,
    PostgresPgLogicalSupport,
    PostgresPgauditSupport,
    PostgresPgRepackSupport,
    PostgresHypoPgSupport,
    PostgresOrafceSupport,
    PostgresAddressStandardizerSupport,
    # DDL feature protocols
    PostgresCommentSupport,
    PostgresConstraintSupport,
    PostgresPolicySupport,
    PostgresRlsConfigSupport,
    PostgresAlterTableSettingsSupport,
    PostgresClusterSupport,
    PostgresRepackSupport,
    PostgresCollationDDLSupport,
    PostgresForeignTableDDLSupport,
    PostgresRoutineDDLSupport,
    PostgresPublicationSupport,
    # Type feature protocols
    PostgresMultirangeSupport,
    PostgresEnumTypeSupport,
    PostgresFullTextSearchSupport,
    PostgresRangeTypeSupport,
    PostgresJSONBEnhancedSupport,
    PostgresArrayEnhancedSupport,
    # Type and domain DDL protocols. These derive TypeObjectSupport /
    # NamespaceSupport, so they precede the object protocols below.
    # Named objects. Each derives NamespaceSupport, and PostgresTableSupport,
    # PostgresIndexSupport and PostgresTriggerSupport derive from three of
    # them, so the whole block sits after the PostgreSQL-specific protocols
    # that restate them.
    ViewObjectSupport,
    MaterializedViewObjectSupport,
    TableObjectSupport,
    ForeignTableObjectSupport,
    IndexObjectSupport,
    SequenceObjectSupport,
    TriggerObjectSupport,
    TypeObjectSupport,
    RoutineObjectSupport,
    NamespaceSupport,
    # DataType Support Protocol
    DDLTypeSupport,
    # New feature protocols
    PostgresParallelQuerySupport,
    PostgresStoredProcedureSupport,
    PostgresExtendedStatisticsSupport,
    PostgresAdvisoryLockSupport,
    PostgresLockingSupport,
    # Function support protocol
    SQLFunctionSupport,
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

    @staticmethod
    def _validate_data_type(data_type: str) -> bool:
        """Validate data type for safe embedding in SQL.

        PostgreSQL supports array types like TEXT[], INTEGER[].

        Note: rhosocial-activerecord base dialect will include this change in
        a future release. This override can be removed after upgrading to that
        version (expected: include brackets [] in the allowlist pattern).
        """
        import re

        return bool(re.fullmatch(r"[A-Za-z0-9\s(),\[\]]+", data_type))

    def get_parameter_placeholder(self, position: int = 0) -> str:
        """psycopg uses '%s' for placeholders."""
        return "%s"

    def get_server_version(self) -> Tuple[int, int, int]:
        """Return the PostgreSQL version this dialect is configured for."""
        return self.version

    def create_schema_differ(self) -> "SchemaDiffer":
        """Return the PostgreSQL schema differ for this dialect."""
        from rhosocial.activerecord.backend.impl.postgres.schema.differ import (
            PostgresSchemaDiffer,
        )

        return cast("SchemaDiffer", PostgresSchemaDiffer())
