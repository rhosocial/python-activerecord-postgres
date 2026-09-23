# src/rhosocial/activerecord/backend/impl/postgres/dialect.py
"""
PostgreSQL backend SQL dialect implementation.

This dialect implements protocols for features that PostgreSQL actually supports,
based on the PostgreSQL version provided at initialization.
"""

from typing import Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.collation import CollateExpression
    from .function_versions import FunctionSupportInfo, FunctionVersionRequirement

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
from rhosocial.activerecord.backend.dialect.mixins import (
    SQLXMLMixin,
    CollationMixin,
    CTEMixin,

    WindowFunctionMixin,
    JSONMixin,

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
)
from rhosocial.activerecord.backend.dialect.protocols import (
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
    JSONSupport,
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
    ViewSupport,
    SchemaSupport,
    SequenceSupport,
    SetOperationSupport,
    TruncateSupport,
    ILIKESupport,
    IntrospectionSupport,
    TransactionControlSupport,
    SQLFunctionSupport,
    DDLTypeSupport,
)
from .mixins import (
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
    PostgresQueryOptimizationMixin,
    PostgresDataTypeMixin,
    PostgresLogicalReplicationMixin,
    PostgresParallelQueryMixin,
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
    PostgresExpressionMixin,
    PostgresFunctionMixin,
)

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
    PostgresSequenceSupport,
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
    PostgresTypeSupport,
    PostgresConstraintSupport,
    PostgresPolicySupport,
    PostgresRlsConfigSupport,
    PostgresAlterTableSettingsSupport,
    PostgresClusterSupport,
    PostgresDomainSupport,
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
    # PG-specific mixins (before global mixins to override)
    PostgresDateTimeMixin,
    PostgresDQLMixin,
    PostgresJSONMixin,
    PostgresGeneratedColumnMixin,
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

    ArrayMixin,
    ExplainMixin,
    PostgresPropertyGraphQueryMixin,
    GraphMixin,
    GraphTableMixin,
    PostgresLockingMixin,

    MergeMixin,

    TemporalTableMixin,
    UpsertMixin,
    LateralJoinMixin,
    JoinMixin,
    ViewMixin,
    SchemaMixin,
    PostgresIndexMixin,
    IndexMixin,
    SequenceMixin,
    # PostgreSQL-specific mixins
    PostgresExtensionMixin,
    PostgresMaterializedViewMixin,
    PostgresAlterColumnModifierMixin,  # Before TableMixin/ConstraintMixin to override format_*_action
    PostgresTableMixin,  # Before TableMixin to override supports_create_table_like
    PostgresCommentMixin,  # Before TableMixin to override format_comment_statement
    TableMixin,
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
    PostgresQueryOptimizationMixin,
    PostgresDataTypeMixin,
    PostgresLogicalReplicationMixin,
    PostgresParallelQueryMixin,
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
    PostgresTypeMixin,
    PostgresConstraintMixin,
    PostgresPolicyMixin,
    PostgresRlsConfigMixin,
    PostgresAlterTableSettingsMixin,
    PostgresClusterMixin,
    PostgresDomainMixin,
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
    PostgresArrayEnhancedMixin,
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
    JSONSupport,
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
    ViewSupport,
    SchemaSupport,
    SequenceSupport,
    # Introspection protocol
    IntrospectionSupport,
    # Transaction control protocol
    TransactionControlSupport,
    # PostgreSQL-specific protocols
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
    PostgresSequenceSupport,
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
    PostgresTypeSupport,
    PostgresConstraintSupport,
    PostgresPolicySupport,
    PostgresRlsConfigSupport,
    PostgresAlterTableSettingsSupport,
    PostgresClusterSupport,
    PostgresDomainSupport,
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

    def __init__(self, version: Optional[Tuple[int, int, int]] = None):
        """
        Initialize PostgreSQL dialect with specific version.

        Args:
            version: PostgreSQL version tuple (major, minor, patch).
                If None, the dialect must be adapted via
                backend.introspect_and_adapt() before version-dependent
                features can be used.

        """
        super().__init__()
        self._reserved_words = POSTGRESQL_RESERVED_WORDS
        if version is not None:
            self.version = version

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

    def create_schema_differ(self):
        """Return the PostgreSQL schema differ for this dialect."""
        from rhosocial.activerecord.backend.impl.postgres.schema.differ import (
            PostgresSchemaDiffer,
        )

        return PostgresSchemaDiffer()
