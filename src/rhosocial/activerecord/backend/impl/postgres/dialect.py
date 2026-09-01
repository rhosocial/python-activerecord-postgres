# src/rhosocial/activerecord/backend/impl/postgres/dialect.py
"""
PostgreSQL backend SQL dialect implementation.

This dialect implements protocols for features that PostgreSQL actually supports,
based on the PostgreSQL version provided at initialization.
"""

from typing import Any, Dict, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.collation import CollateExpression
    from .function_versions import FunctionSupportInfo, FunctionVersionRequirement

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins import (
    SQLXMLMixin,
    CollationMixin,
    CTEMixin,
    FilterClauseMixin,
    WindowFunctionMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
    GraphTableMixin,
    LockingMixin,
    MergeMixin,
    OrderedSetAggregationMixin,
    QualifyClauseMixin,
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
    IdentifierMixin,
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
    PostgresTypeSuggestionMixin,
    # DDL/DML operation mixins (new)
    PostgresExtendedStatisticsMixin,
    PostgresStoredProcedureMixin,
    PostgresAdvisoryLockMixin,
    PostgresLockingMixin,
    # Introspection capability mixin
    PostgresIntrospectionCapabilityMixin,
    PostgresAlterColumnModifierMixin,
)

from .collation import validate_postgres_collation_name

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

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        CreateTableExpression,
        CreateViewExpression,
        DropViewExpression,
        TruncateExpression,
        CreateMaterializedViewExpression,
        DropMaterializedViewExpression,
        RefreshMaterializedViewExpression,
        ExplainExpression,
        AddTableConstraint,
        AddIndex,
        DropIndex as DropIndexAction,  # noqa: F401
        TableConstraint,
    )
    from rhosocial.activerecord.backend.expression.transaction import (
        BeginTransactionExpression,
        CommitTransactionExpression,
        RollbackTransactionExpression,
        SavepointExpression,
        ReleaseSavepointExpression,
        SetTransactionExpression,
    )

class PostgresDialect(
    SQLDialectBase,
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
    FilterClauseMixin,
    WindowFunctionMixin,
    PostgresJSONBEnhancedMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    PostgresPropertyGraphQueryMixin,
    GraphMixin,
    GraphTableMixin,
    PostgresLockingMixin,
    LockingMixin,
    MergeMixin,
    OrderedSetAggregationMixin,
    QualifyClauseMixin,
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
    PostgresTableMixin,  # Before TableMixin to override supports_table_like_syntax
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
    PostgresArrayEnhancedMixin,
    PostgresTypeFormatSupportMixin,
    PostgresTypeSuggestionMixin,
    # DDL/DML operation mixins (new)
    PostgresExtendedStatisticsMixin,
    PostgresStoredProcedureMixin,
    PostgresAdvisoryLockMixin,
    # New Mixins
    IdentifierMixin,
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

    def format_datetime_diff_expression(self, expr: "Any") -> Tuple[str, Tuple]:
        start_sql, start_params = expr.start.to_sql()
        end_sql, end_params = expr.end.to_sql()
        seconds_sql = f"EXTRACT(EPOCH FROM ({end_sql} - {start_sql}))"
        factors = {
            "second": "1",
            "minute": "60",
            "hour": "3600",
            "day": "86400",
            "week": "604800",
        }
        if expr.unit.value in factors:
            sql = f"({seconds_sql} / {factors[expr.unit.value]})"
            params = end_params + start_params
        elif expr.unit.value == "month":
            sql = (
                f"((EXTRACT(YEAR FROM {end_sql}) - "
                f"EXTRACT(YEAR FROM {start_sql})) * 12 + "
                f"(EXTRACT(MONTH FROM {end_sql}) - "
                f"EXTRACT(MONTH FROM {start_sql})))"
            )
            params = end_params + start_params + end_params + start_params
        else:
            sql = (
                f"(EXTRACT(YEAR FROM {end_sql}) - "
                f"EXTRACT(YEAR FROM {start_sql}))"
            )
            params = end_params + start_params
        return self._apply_value_expression_modifiers(sql, params, expr)

    def validate_collation_name(self, expr: "CollateExpression") -> str:
        """Validate PostgreSQL collation names and return their SQL representation."""
        schema = expr.collation_options.get("schema")
        unsupported = set(expr.collation_options) - {"schema"}
        if unsupported:
            options = ", ".join(sorted(unsupported))
            raise UnsupportedFeatureError(self.name, f"COLLATE options: {options}")
        validate_postgres_collation_name(expr.collation_name, getattr(self, "version", None))
        if schema is not None:
            return f"{self.format_identifier(str(schema))}.{self.format_identifier(expr.collation_name)}"
        return self.format_identifier(expr.collation_name)

    def format_explain_statement(self, explain_expr: "ExplainExpression") -> tuple:
        """Build the PostgreSQL EXPLAIN SQL string and return (sql, params).

        PostgreSQL syntax: ``EXPLAIN [ ( option [, ...] ) ] statement``

        Supported options:
        - ``ANALYZE``
        - ``FORMAT { TEXT | XML | JSON | YAML }``
        - ``QUERY PLAN`` type — silently omitted (plain EXPLAIN is equivalent).

        Args:
            explain_expr: ExplainExpression instance

        Returns:
            Tuple of (SQL string, params tuple)

        """
        from rhosocial.activerecord.backend.expression.statements import ExplainType

        statement_sql, statement_params = explain_expr.statement.to_sql()
        options = explain_expr.options
        if options is None:
            return f"EXPLAIN {statement_sql}", statement_params

        opts: list = []

        if options.analyze:
            opts.append("ANALYZE")

        if options.format is not None:
            fmt_name = options.format.name if hasattr(options.format, "name") else str(options.format)
            opts.append(f"FORMAT {fmt_name.upper()}")
        elif options.type is not None and options.type == ExplainType.QUERY_PLAN:
            # PostgreSQL has no QUERY PLAN keyword; plain EXPLAIN is equivalent
            pass

        if opts:
            return "EXPLAIN (" + ", ".join(opts) + ") " + statement_sql, statement_params
        return f"EXPLAIN {statement_sql}", statement_params

    def supports_for_update(self) -> bool:
        """Whether FOR UPDATE clause is supported in SELECT statements.

        PostgreSQL supports FOR UPDATE since early versions. The clause locks
        selected rows preventing other transactions from modifying them.
        PostgreSQL also supports FOR UPDATE OF, FOR UPDATE NOWAIT, and
        FOR UPDATE SKIP LOCKED (since 9.5).
        """
        return True

    def supports_json_arrow_operators(self) -> bool:
        """PostgreSQL supports -> and ->> operators for JSON/JSONB access."""
        return True

    def supports_lock_strength(self, strength) -> bool:
        """
        Check if a specific lock strength is supported.

        PostgreSQL lock strength support by version:
        - FOR UPDATE: All versions
        - FOR NO KEY UPDATE: PostgreSQL 9.0+
        - FOR SHARE: PostgreSQL 9.0+
        - FOR KEY SHARE: PostgreSQL 9.3+

        Args:
            strength: The LockStrength enum value to check

        Returns:
            True if the lock strength is supported, False otherwise

        """
        from rhosocial.activerecord.backend.impl.postgres.expression.locking import LockStrength

        if strength == LockStrength.UPDATE:
            return True  # All PostgreSQL versions support FOR UPDATE
        elif strength == LockStrength.NO_KEY_UPDATE:
            return self.version >= (9, 0, 0)
        elif strength == LockStrength.SHARE:
            return self.version >= (9, 0, 0)
        elif strength == LockStrength.KEY_SHARE:
            return self.version >= (9, 3, 0)
        return False

    # region ILIKE Support

    def format_ilike_expression(self, column: Any, pattern: str, negate: bool = False) -> Tuple[str, Tuple]:
        """Format ILIKE expression for PostgreSQL.

        Args:
            column: Column name string or expression with ``to_sql()``.
            pattern: Right-hand-side pattern (replaced with ``%s`` placeholder).
            negate: If ``True``, produces ``NOT ILIKE`` instead of ``ILIKE``.

        Returns:
            Tuple of (SQL string, (pattern,) params tuple)

        """
        if isinstance(column, str):
            col_sql = self.format_identifier(column)
        else:
            col_sql, col_params = column.to_sql() if hasattr(column, "to_sql") else (str(column), ())

        if negate:
            sql = f"{col_sql} NOT ILIKE %s"
        else:
            sql = f"{col_sql} ILIKE %s"

        return sql, (pattern,)

    # endregion

    # region Truncate Support

    def format_truncate_statement(self, expr: "TruncateExpression") -> Tuple[str, tuple]:
        """Format TRUNCATE statement for PostgreSQL.

        - ``expr.table_name`` — target table.
        - ``expr.restart_identity`` — add ``RESTART IDENTITY`` (PG 8.4+).
        - ``expr.cascade`` — add ``CASCADE``.

        Args:
            expr: TruncateExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        parts = ["TRUNCATE TABLE"]
        parts.append(self.format_identifier(expr.table_name))

        if expr.restart_identity and self.supports_truncate_restart_identity():
            parts.append("RESTART IDENTITY")

        if expr.cascade:
            parts.append("CASCADE")

        return " ".join(parts), ()

    # endregion

    # region Custom Implementations for PostgreSQL-specific behavior
    def format_identifier(self, identifier: str) -> str:
        """
        Format identifier using PostgreSQL's double quote quoting mechanism.

        Args:
            identifier: Raw identifier string

        Returns:
            Quoted identifier with escaped internal quotes

        """
        # Escape any internal double quotes by doubling them
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def format_column(self, name: str, table: Optional[str] = None,
                      alias: Optional[str] = None,
                      schema_name: Optional[str] = None) -> Tuple[str, Tuple]:
        """Format column reference for PostgreSQL.

        PostgreSQL rules for column references:
        - When the table has an alias (used in FROM/JOIN), column references
          must use the alias, not the schema-qualified name. For example,
          when FROM clause has ``public.users AS "users"``, the column
          reference must be ``"users"."id"``, not ``"public"."users"."id"``.
        - When no alias is present but schema is specified, use the
          full three-segment form: ``"schema"."table"."column"``.
        - Otherwise use the standard two-segment or single-segment form.
        """
        if table:
            # In PostgreSQL, when a table has an alias, column references
            # must use the alias — schema_name is irrelevant in this context.
            if schema_name and not alias:
                col_sql = (
                    f"{self.format_identifier(schema_name)}."
                    f"{self.format_identifier(table)}."
                    f"{self.format_identifier(name)}"
                )
            else:
                col_sql = f"{self.format_identifier(table)}.{self.format_identifier(name)}"
        else:
            col_sql = self.format_identifier(name)

        if alias:
            col_sql = f"{col_sql} AS {self.format_identifier(alias)}"

        return col_sql, ()

    def format_on_conflict_clause(self, expr) -> Tuple[str, tuple]:
        """Format ON CONFLICT clause for PostgreSQL.

        Overrides the base implementation to handle EXCLUDED pseudo-table
        references without quoting, as EXCLUDED is a special PostgreSQL
        keyword in ON CONFLICT context and must not be double-quoted.

        - ``expr.conflict_target`` — optional list of column names / expressions.
        - ``expr.do_nothing`` — ``DO NOTHING``.
        - ``expr.update_assignments`` — dict of ``{column: expression}`` for ``DO UPDATE SET``.
        - ``expr.update_where`` — optional WHERE predicate for the update.

        Args:
            expr: OnConflictClause expression instance

        Returns:
            Tuple of (SQL string, params tuple)

        """
        from rhosocial.activerecord.backend.expression import bases
        from rhosocial.activerecord.backend.expression.core import Column

        all_params = []
        parts = ["ON CONFLICT"]

        # Add conflict target if specified
        if expr.conflict_target:
            target_parts = []
            for target in expr.conflict_target:
                if isinstance(target, str):
                    target_parts.append(self.format_identifier(target))
                elif hasattr(target, 'to_sql'):
                    target_sql, target_params = target.to_sql()
                    target_parts.append(target_sql)
                    all_params.extend(target_params)
                else:
                    target_parts.append(self.format_identifier(str(target)))
            if target_parts:
                parts.append(f"({', '.join(target_parts)})")

        # Add DO NOTHING or DO UPDATE
        if expr.do_nothing:
            parts.append("DO NOTHING")
        elif expr.update_assignments:
            update_parts = []
            for col, expr_val in expr.update_assignments.items():
                if isinstance(expr_val, Column) and getattr(expr_val, 'table', None) == 'EXCLUDED':
                    # EXCLUDED is a special pseudo-table in PostgreSQL ON CONFLICT.
                    # It must NOT be double-quoted, only the column name should be quoted.
                    val_sql = f'EXCLUDED.{self.format_identifier(expr_val.name)}'
                    update_parts.append(f"{self.format_identifier(col)} = {val_sql}")
                elif isinstance(expr_val, bases.BaseExpression):
                    val_sql, val_params = expr_val.to_sql()
                    update_parts.append(f"{self.format_identifier(col)} = {val_sql}")
                    all_params.extend(val_params)
                else:
                    update_parts.append(f"{self.format_identifier(col)} = {self.get_parameter_placeholder()}")
                    all_params.append(expr_val)

            parts.append(f"DO UPDATE SET {', '.join(update_parts)}")

            # Add WHERE clause if specified
            if expr.update_where:
                where_sql, where_params = expr.update_where.to_sql()
                parts.append(f"WHERE {where_sql}")
                all_params.extend(where_params)
        else:
            parts.append("DO NOTHING")

        return " ".join(parts), tuple(all_params)

    # region View Support

    def format_create_view_statement(self, expr: "CreateViewExpression") -> Tuple[str, tuple]:
        """Format CREATE VIEW statement for PostgreSQL.

        - ``expr.temporary`` — add ``TEMPORARY``.
        - ``expr.replace`` — add ``OR REPLACE``.
        - ``expr.view_name`` — view name (identifier).
        - ``expr.column_aliases`` — optional list of column aliases.
        - ``expr.query`` — source SELECT expression.
        - ``expr.options.check_option`` — ``WITH {LOCAL|CASCADED} CHECK OPTION``.

        Args:
            expr: CreateViewExpression instance

        Returns:
            Tuple of (SQL string, params tuple)

        """
        parts = ["CREATE"]

        if expr.temporary:
            parts.append("TEMPORARY")

        if expr.replace:
            parts.append("OR REPLACE")

        parts.append("VIEW")
        parts.append(self.format_identifier(expr.view_name))

        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")

        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")

        if expr.options and expr.options.check_option:
            check_option = expr.options.check_option.value
            parts.append(f"WITH {check_option} CHECK OPTION")

        return " ".join(parts), query_params

    def format_drop_view_statement(self, expr: "DropViewExpression") -> Tuple[str, tuple]:
        """Format DROP VIEW statement for PostgreSQL.

        - ``expr.if_exists`` — add ``IF EXISTS``.
        - ``expr.view_name`` — view name (identifier).
        - ``expr.cascade`` — add ``CASCADE``.

        Args:
            expr: DropViewExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        parts = ["DROP VIEW"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.view_name))
        if expr.cascade:
            parts.append("CASCADE")
        return " ".join(parts), ()

    def format_create_materialized_view_statement(self, expr: "CreateMaterializedViewExpression") -> Tuple[str, tuple]:
        """Format CREATE MATERIALIZED VIEW statement for PostgreSQL.

        - ``expr.view_name`` — view name (identifier).
        - ``expr.column_aliases`` — optional list of column aliases.
        - ``expr.tablespace`` — optional tablespace.
        - ``expr.storage_options`` — optional dict of storage parameters (``WITH (… )``).
        - ``expr.query`` — source SELECT expression.
        - ``expr.with_data`` — ``WITH DATA`` / ``WITH NO DATA``.

        Args:
            expr: CreateMaterializedViewExpression instance

        Returns:
            Tuple of (SQL string, params tuple)

        """
        parts = ["CREATE MATERIALIZED VIEW"]
        parts.append(self.format_identifier(expr.view_name))

        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")

        if expr.tablespace and self.supports_materialized_view_tablespace():
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")

        if expr.storage_options and self.supports_materialized_view_storage_options():
            storage_parts = []
            for key, value in expr.storage_options.items():
                storage_parts.append(f"{key.upper()} = {value}")
            parts.append(f"WITH ({', '.join(storage_parts)})")

        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")

        if expr.with_data:
            parts.append("WITH DATA")
        else:
            parts.append("WITH NO DATA")

        return " ".join(parts), query_params

    def format_drop_materialized_view_statement(self, expr: "DropMaterializedViewExpression") -> Tuple[str, tuple]:
        """Format DROP MATERIALIZED VIEW statement for PostgreSQL.

        - ``expr.if_exists`` — add ``IF EXISTS``.
        - ``expr.view_name`` — view name (identifier).
        - ``expr.cascade`` — add ``CASCADE``.

        Args:
            expr: DropMaterializedViewExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        parts = ["DROP MATERIALIZED VIEW"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.view_name))
        if expr.cascade:
            parts.append("CASCADE")
        return " ".join(parts), ()

    def format_refresh_materialized_view_statement(
        self, expr: "RefreshMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Format REFRESH MATERIALIZED VIEW statement for PostgreSQL.

        - ``expr.concurrent`` — add ``CONCURRENTLY`` (PG 9.4+).
        - ``expr.view_name`` — view name (identifier).
        - ``expr.with_data`` — ``WITH DATA`` / ``WITH NO DATA``.

        Args:
            expr: RefreshMaterializedViewExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        parts = ["REFRESH MATERIALIZED VIEW"]
        if expr.concurrent and self.supports_materialized_view_concurrent_refresh():
            parts.append("CONCURRENTLY")
        parts.append(self.format_identifier(expr.view_name))
        if expr.with_data is not None:
            parts.append("WITH DATA" if expr.with_data else "WITH NO DATA")
        return " ".join(parts), ()

    # endregion

    # region Index Support

    def format_add_index_action(self, action: "AddIndex") -> Tuple[str, tuple]:
        """PostgreSQL does not support ALTER TABLE ADD INDEX.

        Raises UnsupportedFeatureError — use CREATE INDEX instead.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )

        raise UnsupportedFeatureError(
            self.name,
            "ALTER TABLE ADD INDEX",
            suggestion="Use CREATE INDEX to create an index on the table.",
        )

    def format_drop_index_action(self, action: "DropIndex") -> Tuple[str, tuple]:  # noqa: F821
        """PostgreSQL does not support ALTER TABLE DROP INDEX.

        Raises UnsupportedFeatureError — use DROP INDEX statement instead.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )

        raise UnsupportedFeatureError(
            self.name,
            "ALTER TABLE DROP INDEX",
            suggestion="Use DROP INDEX statement to remove an index.",
        )

    # endregion

    # region Table Support

    # Constraint capability overrides

    def format_create_table_statement(self, expr: "CreateTableExpression") -> Tuple[str, tuple]:
        """
        Format CREATE TABLE statement for PostgreSQL.

        This formatter handles PostgreSQL-specific CREATE TABLE behavior before
        delegating ordinary table generation to the base dialect:

        * PostgreSQL LIKE syntax with INCLUDING/EXCLUDING options.
        * Declarative partitioning validation for ``partition`` expressions.

        PostgreSQL LIKE syntax supports INCLUDING/EXCLUDING options to control what
        gets copied: DEFAULTS, CONSTRAINTS, INDEXES, IDENTITY, GENERATED, ALL,
        COMMENTS, STORAGE, COMPRESSION.

        The behavior is controlled by the `dialect_options` parameter in
        CreateTableExpression:

        1. When `dialect_options` contains 'like_table' key:
           - LIKE syntax takes highest priority
           - All other parameters (columns, indexes, constraints, etc.) are IGNORED
           - Only temporary and if_not_exists flags are considered
           - Additional 'like_options' key controls INCLUDING/EXCLUDING behavior

        2. When `dialect_options` does NOT contain 'like_table':
           - Applies PostgreSQL partition validation when `partition` is present
           - Falls back to base class implementation for SQL generation
           - Standard CREATE TABLE and CREATE TABLE ... PARTITION BY statements
             are generated by the base formatter

        Partition validation follows PostgreSQL declarative partitioning rules:

        - RANGE and LIST partitioned tables require PostgreSQL 10+.
        - HASH partitioned tables require PostgreSQL 11+.
        - MySQL-specific partition methods such as KEY and RANGE COLUMNS are rejected.
        - The API is `partition=PartitionClause(...)`; PartitionClause is the
          standard expression type for partition specification.

        The 'like_options' key supports two formats:

        a) Dictionary format (recommended):
           {
               'including': ['DEFAULTS', 'CONSTRAINTS', 'INDEXES'],
               'excluding': ['COMMENTS']
           }

        b) List format (for backwards compatibility):
           ['DEFAULTS', 'CONSTRAINTS']  # Defaults to INCLUDING
           or
           [('INCLUDING', 'DEFAULTS'), ('EXCLUDING', 'INDEXES')]

        Usage Examples:
            # Basic LIKE syntax
            CreateTableExpression(
                dialect=postgres_dialect,
                table_name="users_copy",
                columns=[],  # Ignored when like_table is present
                dialect_options={'like_table': 'users'}
            )
            # Generates: CREATE TABLE "users_copy" (LIKE "users")

            # LIKE with INCLUDING options (dictionary format - recommended)
            CreateTableExpression(
                dialect=postgres_dialect,
                table_name="users_copy",
                columns=[...],  # Will be ignored
                dialect_options={
                    'like_table': 'users',
                    'like_options': {
                        'including': ['DEFAULTS', 'CONSTRAINTS', 'INDEXES'],
                        'excluding': ['COMMENTS']
                    }
                }
            )
            # Generates: CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS,
            #           INCLUDING CONSTRAINTS, INCLUDING INDEXES, EXCLUDING COMMENTS)

            # LIKE with schema-qualified source table
            CreateTableExpression(
                dialect=postgres_dialect,
                table_name="users_copy",
                columns=[],
                dialect_options={'like_table': ('public', 'users')}
            )
            # Generates: CREATE TABLE "users_copy" (LIKE "public"."users")

            # LIKE with TEMPORARY and IF NOT EXISTS
            CreateTableExpression(
                dialect=postgres_dialect,
                table_name="temp_users",
                columns=[],
                temporary=True,
                if_not_exists=True,
                dialect_options={'like_table': 'users'}
            )
            # Generates: CREATE TEMPORARY TABLE IF NOT EXISTS "temp_users" (LIKE "users")

            # LIKE with INCLUDING ALL
            CreateTableExpression(
                dialect=postgres_dialect,
                table_name="users_copy",
                columns=[],
                dialect_options={
                    'like_table': 'users',
                    'like_options': {'including': ['ALL']}
                }
            )
            # Generates: CREATE TABLE "users_copy" (LIKE "users", INCLUDING ALL)

            # PostgreSQL declarative partitioned table
            CreateTableExpression(
                dialect=postgres_dialect,
                table="events",
                columns=[
                    ColumnDefinition("id", "BIGINT NOT NULL"),
                    ColumnDefinition("created_at", "TIMESTAMP NOT NULL"),
                ],
                partition=PartitionClause(
                    dialect=postgres_dialect,
                    method="RANGE",
                    keys=[Column(postgres_dialect, "created_at")],
                ),
            )
            # Generates: CREATE TABLE "events" (...) PARTITION BY RANGE ("created_at")
            # PostgreSQL dialect validates version support before delegating to
            # the base formatter.

        Args:
            expr: CreateTableExpression instance

        Returns:
            Tuple of (SQL string, parameters tuple)

        """
        # Check for LIKE syntax in dialect_options (highest priority)
        if "like_table" in expr.dialect_options:
            like_table = expr.dialect_options["like_table"]
            like_options = expr.dialect_options.get("like_options", [])

            parts = ["CREATE"]

            if expr.temporary:
                parts.append("TEMPORARY")

            parts.append("TABLE")

            if expr.if_not_exists:
                parts.append("IF NOT EXISTS")

            parts.append(self.format_identifier(expr.table_name))

            # Build LIKE clause with options
            like_parts = []

            # Handle schema-qualified table name: ('schema', 'table')
            if isinstance(like_table, tuple):
                schema, table = like_table
                like_table_str = f"{self.format_identifier(schema)}.{self.format_identifier(table)}"
            else:
                like_table_str = self.format_identifier(like_table)

            like_parts.append(f"LIKE {like_table_str}")

            # Add INCLUDING/EXCLUDING options
            # Format: dictionary with 'including' and 'excluding' keys
            # Example: {'including': ['DEFAULTS', 'CONSTRAINTS'], 'excluding': ['INDEXES']}
            if isinstance(like_options, dict):
                # Handle dictionary format
                including = like_options.get("including", [])
                excluding = like_options.get("excluding", [])

                for option in including:
                    like_parts.append(f"INCLUDING {option.upper()}")

                for option in excluding:
                    like_parts.append(f"EXCLUDING {option.upper()}")
            elif isinstance(like_options, list):
                # Handle list format for backwards compatibility
                for option in like_options:
                    if isinstance(option, tuple):
                        action, feature = option
                        like_parts.append(f"{action.upper()} {feature.upper()}")
                    else:
                        # Default to INCLUDING if just feature name provided
                        like_parts.append(f"INCLUDING {option.upper()}")

            parts.append(f"({', '.join(like_parts)})")

            return " ".join(parts), ()

        if getattr(expr, "partition", None) is not None:
            # Validate through the PartitionClause → format_partition_clause chain.
            expr.partition.to_sql()

        # Otherwise, delegate to base implementation
        return super().format_create_table_statement(expr)

    # endregion

    # region Type Casting Support (PostgreSQL-specific)
    def format_cast_expression(
        self, expr_sql: str, target_type: str, expr_params: tuple, alias: Optional[str] = None
    ) -> Tuple[str, Tuple]:
        """Format type cast expression using PostgreSQL :: syntax.

        PostgreSQL supports both standard CAST(expr AS type) syntax and the
        PostgreSQL-specific expr::type syntax. This method uses the more
        concise :: syntax which is idiomatic in PostgreSQL.

        Args:
            expr_sql: SQL expression string to be cast
            target_type: Target PostgreSQL type name (e.g., 'integer', 'varchar(100)')
            expr_params: Parameters tuple for the expression
            alias: Optional alias for the result

        Returns:
            Tuple of (SQL string, parameters)

        Example:
            >>> dialect.format_cast_expression('price', 'numeric', ())
            # Returns: ('price::numeric', ())
            >>> dialect.format_cast_expression('amount', 'money', ())
            # Returns: ('amount::money', ())
            >>> dialect.format_cast_expression('value', 'integer', (), 'int_val')
            # Returns: ('value::integer AS "int_val"', ())

        Note:
            For chained type conversions, each ::type is appended:
            >>> col.cast('money').cast('numeric').cast('float8')
            # Generates: col::money::numeric::float8

        """
        sql = f"{expr_sql}::{target_type}"
        if alias:
            sql = f"{sql} AS {self.format_identifier(alias)}"
        return sql, expr_params

    # endregion

    # region Operator Formatting (PostgreSQL-specific)

    def format_binary_operator(
        self, op: str, left_sql: str, right_sql: str, left_params: tuple, right_params: tuple
    ) -> Tuple[str, Tuple]:
        """Format binary operator with psycopg placeholder escaping.

        psycopg uses %s as parameter placeholder. When the SQL operator itself
        contains % (e.g., pg_trgm similarity operator), it must be escaped as %%
        to prevent psycopg from interpreting it as a placeholder prefix.
        """
        # Escape % in operators for psycopg compatibility
        escaped_op = op.replace('%', '%%') if '%' in op else op
        sql = f"{left_sql} {escaped_op} {right_sql}"
        return sql, left_params + right_params

    # endregion

    # region Constraint DDL Support (PostgreSQL-specific)

    def format_add_table_constraint_action(
        self, action: "AddTableConstraint",
    ) -> Tuple[str, tuple]:
        """Format ADD CONSTRAINT action with PostgreSQL-specific extensions.

        Extends the base class implementation with:
        - EXCLUDE constraint support (PG-specific constraint type)
        - NOT VALID suffix (PG-specific: skip validation of existing rows)
        """
        from rhosocial.activerecord.backend.expression.statements import (
            TableConstraintType, ConstraintValidation,
        )

        # Handle EXCLUDE constraint (PG-specific, not in base class)
        if action.constraint.constraint_type == TableConstraintType.EXCLUDE:
            parts = []
            exclude_sql, params = self.format_exclude_constraint(action.constraint)
            parts.append(exclude_sql)

            # NOT VALID suffix
            if action.constraint.dialect_options:
                validation = action.constraint.dialect_options.get('validation')
                if validation == ConstraintValidation.NOVALIDATE:
                    parts.append("NOT VALID")

            return f"ADD {' '.join(parts)}", tuple(params)

        # Use base class for standard formatting (includes DEFERRABLE)
        sql, params = super().format_add_table_constraint_action(action)

        # PostgreSQL NOT VALID suffix
        if action.constraint.dialect_options:
            validation = action.constraint.dialect_options.get('validation')
            if validation == ConstraintValidation.NOVALIDATE:
                sql += " NOT VALID"

        return sql, params

    def format_exclude_constraint(
        self, constraint: "TableConstraint",
    ) -> Tuple[str, tuple]:
        """Format EXCLUDE constraint (PostgreSQL-specific).

        EXCLUDE constraints use the dialect_options dict to specify:
        - 'exclude_elements': List of (expression, operator) tuples
          e.g., [('range', '&&')] for EXCLUDE USING gist (range WITH &&)
        - 'using': The index access method (default 'gist')
          e.g., 'gist', 'btree', 'spgist'
        - 'where': Optional predicate for partial exclusion constraints

        Example:
            TableConstraint(
                constraint_type=TableConstraintType.EXCLUDE,
                name='exclude_range_overlap',
                dialect_options={
                    'exclude_elements': [('range', '&&')],
                    'using': 'gist',
                }
            )
            # Generates: EXCLUDE USING gist (range WITH &&)

        """
        params: list = []

        if constraint.name:
            parts = ["CONSTRAINT", self.format_identifier(constraint.name)]
        else:
            parts = []

        # USING clause - validate index access method.
        valid_using = frozenset({"gist", "btree", "spgist", "hash", "gin", "brin"})
        using = constraint.dialect_options.get("using", "gist") if constraint.dialect_options else "gist"
        if using not in valid_using:
            raise ValueError(
                f"Invalid index access method '{using}': must be one of {valid_using}"
            )
        parts.append(f"EXCLUDE USING {using}")

        # Elements: (expression, operator) pairs - validate operators.
        valid_ops = frozenset({
            "=", "<", "<=", ">", ">=", "<>",
            "&&", "@>", "<@", "<<", ">>", "&<", "&>",
            "~=", "@@", "?|", "?&", "is", "is not",
        })
        exclude_elements = []
        if constraint.dialect_options and "exclude_elements" in constraint.dialect_options:
            for expr, op in constraint.dialect_options["exclude_elements"]:
                if op not in valid_ops:
                    raise ValueError(
                        f"Invalid exclude operator '{op}': must be one of {valid_ops}"
                    )
                if isinstance(expr, str):
                    exclude_elements.append(f"{self.format_identifier(expr)} WITH {op}")
                else:
                    expr_sql, expr_params = expr.to_sql()
                    params.extend(expr_params)
                    exclude_elements.append(f"{expr_sql} WITH {op}")

        if exclude_elements:
            parts.append(f"({', '.join(exclude_elements)})")

        # WHERE clause for partial exclusion constraint
        if constraint.dialect_options and 'where' in constraint.dialect_options:
            where_expr = constraint.dialect_options['where']
            where_sql, where_params = where_expr.to_sql()
            params.extend(where_params)
            parts.append(f"WHERE ({where_sql})")

        # DEFERRABLE / NOT DEFERRABLE
        if constraint.deferrable is True:
            if constraint.initially_deferred is True:
                parts.append("DEFERRABLE INITIALLY DEFERRED")
            elif constraint.initially_deferred is False:
                parts.append("DEFERRABLE INITIALLY IMMEDIATE")
            else:
                parts.append("DEFERRABLE")
        elif constraint.deferrable is False:
            parts.append("NOT DEFERRABLE")

        return ' '.join(parts), tuple(params)

    # endregion

    # region Transaction Control Support

    # PostgreSQL function version support: function_name -> (min_version, max_version)
    # min_version: minimum supported version (inclusive), None = all versions
    # Function version requirements are defined in function_versions.py,
    # categorized by topic (JSON Path, Range, hstore, pgvector, PostGIS, etc.)
    # and assembled into POSTGRES_FUNCTION_VERSIONS.
    from .function_versions import POSTGRES_FUNCTION_VERSIONS as _FV
    _POSTGRES_FUNCTION_VERSIONS = _FV

    def supports_functions(self) -> Dict[str, "FunctionSupportInfo"]:
        """Return supported SQL functions with detailed support information.

        This method combines:
        1. Core functions from rhosocial.activerecord.backend.expression.functions
        2. PostgreSQL-specific functions from rhosocial.activerecord.backend.impl.postgres.functions

        Each function is mapped to a FunctionSupportInfo indicating:
        - Whether the function is supported
        - If not, the reason why (PG version, extension status, etc.)

        Returns:
            Dict mapping function names to FunctionSupportInfo.

        """
        from .function_versions import FunctionSupportInfo
        from rhosocial.activerecord.backend.expression.functions import (
            __all__ as core_functions,
        )
        from rhosocial.activerecord.backend.impl.postgres import functions as postgres_functions

        expression_constructors = {
            "xmlagg",
            "xmlattributes",
            "xmlcomment",
            "xmlconcat",
            "xmlelement",
            "xmlexists",
            "xmlforest",
            "xmlparse",
            "xmlpi",
            "xmlquery",
            "xmlroot",
            "xmlserialize",
            "xmltable",
        }
        result: Dict[str, FunctionSupportInfo] = {}
        for func_name in core_functions:
            if func_name not in expression_constructors:
                result[func_name] = self._check_function_support(func_name)

        postgres_funcs = getattr(postgres_functions, "__all__", [])
        for func_name in postgres_funcs:
            if func_name not in result:
                result[func_name] = self._check_function_support(func_name)

        return result

    def _check_function_support(self, func_name: str) -> "FunctionSupportInfo":
        """Check function support status and return detailed information.

        Args:
            func_name: Name of the function to check

        Returns:
            FunctionSupportInfo with support status and reason if unsupported

        """
        from .function_versions import FunctionSupportInfo

        requirement = self._POSTGRES_FUNCTION_VERSIONS.get(func_name)
        if requirement is None:
            return FunctionSupportInfo(supported=True)

        # Check PostgreSQL server version
        if requirement.min_pg_version is not None and self.version < requirement.min_pg_version:
            return FunctionSupportInfo(supported=False, reason="pg_version_too_low")
        if requirement.max_pg_version is not None and self.version > requirement.max_pg_version:
            return FunctionSupportInfo(supported=False, reason="pg_version_too_high")

        # Check extension requirements
        if requirement.extension is not None:
            if not hasattr(self, "_extensions"):
                return FunctionSupportInfo(supported=False, reason="extension_not_probed")

            if requirement.ext_feature is not None:
                if not self.check_extension_feature(requirement.extension, requirement.ext_feature):
                    if not self.is_extension_installed(requirement.extension):
                        return FunctionSupportInfo(supported=False, reason="extension_not_installed")
                    return FunctionSupportInfo(supported=False, reason="extension_version_insufficient")
            else:
                if not self.is_extension_installed(requirement.extension):
                    return FunctionSupportInfo(supported=False, reason="extension_not_installed")
                if requirement.min_ext_version is not None:
                    installed = self.get_extension_version(requirement.extension)
                    if installed is None or self._compare_versions(installed, requirement.min_ext_version) < 0:
                        return FunctionSupportInfo(supported=False, reason="extension_version_insufficient")

        return FunctionSupportInfo(supported=True)

    def _is_postgres_function_supported(self, func_name: str) -> bool:
        """Check if a PostgreSQL function is supported based on version and extensions.

        Checks:
        1. PostgreSQL server version (for built-in and version-gated functions)
        2. Extension installation and version (for extension-provided functions)

        For extension functions, requires _extensions to have been populated
        via introspect_and_adapt(). If _extensions is not available,
        extension functions return False (cannot confirm availability).

        Args:
            func_name: Name of the PostgreSQL function

        Returns:
            True if supported, False otherwise

        """
        requirement = self._POSTGRES_FUNCTION_VERSIONS.get(func_name)
        if requirement is None:
            return True  # Unregistered functions default to supported

        # Step 1: Check PostgreSQL server version
        if requirement.min_pg_version is not None and self.version < requirement.min_pg_version:
            return False
        if requirement.max_pg_version is not None and self.version > requirement.max_pg_version:
            return False

        # Step 2: Check extension requirements
        if requirement.extension is not None:
            return self._check_extension_requirement(requirement)

        return True

    def _check_extension_requirement(
        self, requirement: "FunctionVersionRequirement"
    ) -> bool:
        """Check extension requirements based on introspect_and_adapt() results.

        Requires _extensions to have been populated via introspect_and_adapt().
        If _extensions is not available, returns False (cannot confirm
        extension availability without probing the database).
        """
        ext_name = requirement.extension

        # _extensions is populated by introspect_and_adapt()
        # If the attribute doesn't exist, introspect_and_adapt() hasn't been called
        if not hasattr(self, "_extensions"):
            return False

        # Use feature-level check if ext_feature is specified
        if requirement.ext_feature is not None:
            return self.check_extension_feature(ext_name, requirement.ext_feature)

        # Check extension installed
        if not self.is_extension_installed(ext_name):
            return False

        # Check extension version
        if requirement.min_ext_version is not None:
            installed = self.get_extension_version(ext_name)
            if installed is None:
                return False
            return self._compare_versions(installed, requirement.min_ext_version) >= 0

        return True

    def format_begin_transaction(
        self, expr: "BeginTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format BEGIN TRANSACTION statement for PostgreSQL.

        PostgreSQL syntax:
        BEGIN [ ISOLATION LEVEL { READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE } ]
              [ { READ WRITE | READ ONLY } ]
              [ { NOT DEFERRABLE | DEFERRABLE } ]

        DEFERRABLE is only meaningful for SERIALIZABLE isolation level.
        """
        params = expr.get_params()
        parts = ["BEGIN"]

        isolation = params.get("isolation_level")
        if isolation:
            level_str = self.get_isolation_level_name(isolation)
            parts.append(f"ISOLATION LEVEL {level_str}")

        mode = params.get("mode")
        if mode:
            mode_name = mode.name if hasattr(mode, "name") else str(mode)
            if mode_name == "READ_ONLY":
                parts.append("READ ONLY")
            elif mode_name == "READ_WRITE":
                parts.append("READ WRITE")

        deferrable = params.get("deferrable")
        if deferrable is not None and isolation:
            isolation_name = isolation.name if hasattr(isolation, "name") else str(isolation)
            if isolation_name == "SERIALIZABLE":
                parts.append("DEFERRABLE" if deferrable else "NOT DEFERRABLE")

        return " ".join(parts), ()

    def format_commit_transaction(
        self, expr: "CommitTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format COMMIT TRANSACTION statement for PostgreSQL.

        Always returns ``COMMIT``; ``expr`` is ignored.

        Args:
            expr: CommitTransactionExpression instance (unused)

        Returns:
            Tuple of ("COMMIT", ())

        """
        return "COMMIT", ()

    def format_rollback_transaction(
        self, expr: "RollbackTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format ROLLBACK TRANSACTION statement for PostgreSQL.

        Supports ROLLBACK [ TO SAVEPOINT savepoint_name ].
        """
        params = expr.get_params()
        savepoint = params.get("savepoint")
        if savepoint:
            return f"ROLLBACK TO SAVEPOINT {self.format_identifier(savepoint)}", ()
        return "ROLLBACK", ()

    def format_savepoint(
        self, expr: "SavepointExpression"
    ) -> Tuple[str, tuple]:
        """Format SAVEPOINT statement for PostgreSQL.

        ``expr.get_params()["name"]`` — savepoint name (identifier).

        Args:
            expr: SavepointExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        params = expr.get_params()
        name = params.get("name", "")
        return f"SAVEPOINT {self.format_identifier(name)}", ()

    def format_release_savepoint(
        self, expr: "ReleaseSavepointExpression"
    ) -> Tuple[str, tuple]:
        """Format RELEASE SAVEPOINT statement for PostgreSQL.

        ``expr.get_params()["name"]`` — savepoint name (identifier).

        Args:
            expr: ReleaseSavepointExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        params = expr.get_params()
        name = params.get("name", "")
        return f"RELEASE SAVEPOINT {self.format_identifier(name)}", ()

    def format_set_transaction(
        self, expr: "SetTransactionExpression"
    ) -> Tuple[str, tuple]:
        """Format SET TRANSACTION statement for PostgreSQL.

        PostgreSQL supports setting transaction characteristics for the current
        transaction or for subsequent transactions.

        Syntax:
        SET TRANSACTION { ISOLATION LEVEL { ... } | { READ WRITE | READ ONLY } | [ NOT ] DEFERRABLE } [, ...]
        SET SESSION CHARACTERISTICS AS TRANSACTION { ... }
        """
        params = expr.get_params()
        parts = []

        if params.get("session"):
            parts.append("SET SESSION CHARACTERISTICS AS TRANSACTION")
        else:
            parts.append("SET TRANSACTION")

        options = []

        isolation = params.get("isolation_level")
        if isolation:
            level_str = self.get_isolation_level_name(isolation)
            options.append(f"ISOLATION LEVEL {level_str}")

        mode = params.get("mode")
        if mode:
            mode_name = mode.name if hasattr(mode, "name") else str(mode)
            if mode_name == "READ_ONLY":
                options.append("READ ONLY")
            elif mode_name == "READ_WRITE":
                options.append("READ WRITE")

        deferrable = params.get("deferrable")
        if deferrable is not None:
            options.append("DEFERRABLE" if deferrable else "NOT DEFERRABLE")

        if options:
            parts.append(" ".join(options))

        return " ".join(parts), ()

    # endregion
