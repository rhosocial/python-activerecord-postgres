# src/rhosocial/activerecord/backend/impl/postgres/dialect.py
"""
PostgreSQL backend SQL dialect implementation.

This dialect implements protocols for features that PostgreSQL actually supports,
based on the PostgreSQL version provided at initialization.
"""

from typing import Any, Tuple, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
from rhosocial.activerecord.backend.dialect.mixins import (
    CTEMixin,
    FilterClauseMixin,
    WindowFunctionMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
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
)
from rhosocial.activerecord.backend.dialect.protocols import (
    CTESupport,
    FilterClauseSupport,
    WindowFunctionSupport,
    JSONSupport,
    ReturningSupport,
    AdvancedGroupingSupport,
    ArraySupport,
    ExplainSupport,
    GraphSupport,
    LockingSupport,
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
    IndexSupport,
    SequenceSupport,
    TableSupport,
    SetOperationSupport,
    TruncateSupport,
    ILIKESupport,
    IntrospectionSupport,
)
from .mixins import (
    PostgresExtensionMixin,
    PostgresMaterializedViewMixin,
    PostgresTableMixin,
    PostgresPgvectorMixin,
    PostgresPostGISMixin,
    PostgresPgTrgmMixin,
    PostgresHstoreMixin,
    # Native feature mixins
    PostgresPartitionMixin,
    PostgresIndexMixin,
    PostgresVacuumMixin,
    PostgresQueryOptimizationMixin,
    PostgresDataTypeMixin,
    PostgresSQLSyntaxMixin,
    PostgresLogicalReplicationMixin,
    # Extension feature mixins
    PostgresLtreeMixin,
    PostgresIntarrayMixin,
    PostgresEarthdistanceMixin,
    PostgresTablefuncMixin,
    PostgresPgStatStatementsMixin,
    # DDL feature mixins
    PostgresTriggerMixin,
    PostgresCommentMixin,
    PostgresTypeMixin,
    # Type mixins
    EnumTypeMixin,
    TypesDataTypeMixin,
    MultirangeMixin,
    # DDL/DML operation mixins (new)
    PostgresExtendedStatisticsMixin,
    PostgresStoredProcedureMixin,
    # Introspection capability mixin
    PostgresIntrospectionCapabilityMixin,
)

# PostgreSQL-specific imports
from .protocols import (
    PostgresExtensionSupport,
    PostgresMaterializedViewSupport,
    PostgresTableSupport,
    PostgresPgvectorSupport,
    PostgresPostGISSupport,
    PostgresPgTrgmSupport,
    PostgresHstoreSupport,
    # Native feature protocols
    PostgresPartitionSupport,
    PostgresIndexSupport,
    PostgresVacuumSupport,
    PostgresQueryOptimizationSupport,
    PostgresDataTypeSupport,
    PostgresSQLSyntaxSupport,
    PostgresLogicalReplicationSupport,
    # Extension feature protocols
    PostgresLtreeSupport,
    PostgresIntarraySupport,
    PostgresEarthdistanceSupport,
    PostgresTablefuncSupport,
    PostgresPgStatStatementsSupport,
    # DDL feature protocols
    PostgresTriggerSupport,
    PostgresCommentSupport,
    PostgresTypeSupport,
    # Type feature protocols
    MultirangeSupport,
    EnumTypeSupport,
    # New feature protocols
    PostgresParallelQuerySupport,
    PostgresStoredProcedureSupport,
    PostgresExtendedStatisticsSupport,
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
    )


class PostgresDialect(
    SQLDialectBase,
    SetOperationMixin,
    TruncateMixin,
    ILIKEMixin,
    CTEMixin,
    FilterClauseMixin,
    WindowFunctionMixin,
    JSONMixin,
    ReturningMixin,
    AdvancedGroupingMixin,
    ArrayMixin,
    ExplainMixin,
    GraphMixin,
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
    # Introspection capability
    PostgresIntrospectionCapabilityMixin,
    # PostgreSQL-specific mixins
    PostgresExtensionMixin,
    PostgresMaterializedViewMixin,
    PostgresTableMixin,
    PostgresPgvectorMixin,
    PostgresPostGISMixin,
    PostgresPgTrgmMixin,
    PostgresHstoreMixin,
    # Native feature mixins
    PostgresPartitionMixin,
    PostgresIndexMixin,
    PostgresVacuumMixin,
    PostgresQueryOptimizationMixin,
    PostgresDataTypeMixin,
    PostgresSQLSyntaxMixin,
    PostgresLogicalReplicationMixin,
    # Extension feature mixins
    PostgresLtreeMixin,
    PostgresIntarrayMixin,
    PostgresEarthdistanceMixin,
    PostgresTablefuncMixin,
    PostgresPgStatStatementsMixin,
    # DDL feature mixins
    PostgresTriggerMixin,
    PostgresCommentMixin,
    PostgresTypeMixin,
    # Type mixins
    EnumTypeMixin,
    TypesDataTypeMixin,
    MultirangeMixin,
    # DDL/DML operation mixins (new)
    PostgresExtendedStatisticsMixin,
    PostgresStoredProcedureMixin,
    # Protocol supports
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
    LockingSupport,
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
    IndexSupport,
    SequenceSupport,
    TableSupport,
    # Introspection protocol
    IntrospectionSupport,
    # PostgreSQL-specific protocols
    PostgresExtensionSupport,
    PostgresMaterializedViewSupport,
    PostgresTableSupport,
    PostgresPgvectorSupport,
    PostgresPostGISSupport,
    PostgresPgTrgmSupport,
    PostgresHstoreSupport,
    # Native feature protocols
    PostgresPartitionSupport,
    PostgresIndexSupport,
    PostgresVacuumSupport,
    PostgresQueryOptimizationSupport,
    PostgresDataTypeSupport,
    PostgresSQLSyntaxSupport,
    PostgresLogicalReplicationSupport,
    # Extension feature protocols
    PostgresLtreeSupport,
    PostgresIntarraySupport,
    PostgresEarthdistanceSupport,
    PostgresTablefuncSupport,
    PostgresPgStatStatementsSupport,
    # DDL feature protocols
    PostgresTriggerSupport,
    PostgresCommentSupport,
    PostgresTypeSupport,
    # Type feature protocols
    MultirangeSupport,
    EnumTypeSupport,
    # New feature protocols
    PostgresParallelQuerySupport,
    PostgresStoredProcedureSupport,
    PostgresExtendedStatisticsSupport,
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

    def __init__(self, version: Tuple[int, int, int] = (13, 0, 0)):
        """
        Initialize PostgreSQL dialect with specific version.

        Args:
            version: PostgreSQL version tuple (major, minor, patch)
        """
        self.version = version
        super().__init__()

    def get_parameter_placeholder(self, position: int = 0) -> str:
        """psycopg uses '%s' for placeholders."""
        return "%s"

    def get_server_version(self) -> Tuple[int, int, int]:
        """Return the PostgreSQL version this dialect is configured for."""
        return self.version

    # region Protocol Support Checks based on version
    def supports_basic_cte(self) -> bool:
        """Basic CTEs are supported since PostgreSQL 8.4."""
        return True  # Supported in all modern versions

    def supports_recursive_cte(self) -> bool:
        """Recursive CTEs are supported since PostgreSQL 8.4."""
        return True  # Supported in all modern versions

    def supports_materialized_cte(self) -> bool:
        """MATERIALIZED hint is supported since PostgreSQL 12."""
        return self.version >= (12, 0, 0)

    def supports_returning_clause(self) -> bool:
        """RETURNING clause is supported since PostgreSQL 8.2."""
        return True  # Supported in all modern versions

    def supports_window_functions(self) -> bool:
        """Window functions are supported since PostgreSQL 8.4."""
        return True  # Supported in all modern versions

    def supports_window_frame_clause(self) -> bool:
        """Whether window frame clauses (ROWS/RANGE) are supported, since PostgreSQL 8.4."""
        return True  # Supported in all modern versions

    def supports_filter_clause(self) -> bool:
        """FILTER clause for aggregate functions is supported since PostgreSQL 9.4."""
        return self.version >= (9, 4, 0)

    def supports_json_type(self) -> bool:
        """JSON is supported since PostgreSQL 9.2."""
        return self.version >= (9, 2, 0)

    def get_json_access_operator(self) -> str:
        """PostgreSQL uses '->' for JSON access."""
        return "->"

    def supports_json_table(self) -> bool:
        """JSON_TABLE function is supported since PostgreSQL 12."""
        return self.version >= (12, 0, 0)

    def supports_rollup(self) -> bool:
        """ROLLUP is supported since PostgreSQL 9.5."""
        return self.version >= (9, 5, 0)

    def supports_cube(self) -> bool:
        """CUBE is supported since PostgreSQL 9.5."""
        return self.version >= (9, 5, 0)

    def supports_grouping_sets(self) -> bool:
        """GROUPING SETS is supported since PostgreSQL 9.5."""
        return self.version >= (9, 5, 0)

    def supports_array_type(self) -> bool:
        """PostgreSQL has native array types support."""
        return True  # Supported in all modern versions

    def supports_array_constructor(self) -> bool:
        """ARRAY constructor is supported."""
        return True  # Supported in all modern versions

    def supports_array_access(self) -> bool:
        """Array subscript access is supported."""
        return True  # Supported in all modern versions

    def supports_explain_analyze(self) -> bool:
        """Whether EXPLAIN ANALYZE is supported."""
        return True  # Supported in all modern versions

    def supports_explain_format(self, format_type: str) -> bool:
        """Check if specific EXPLAIN format is supported."""
        format_type_upper = format_type.upper()
        # PostgreSQL supports TEXT, XML, JSON, and YAML formats
        supported_formats = ["TEXT", "XML", "JSON", "YAML"]
        return format_type_upper in supported_formats

    def format_explain_statement(self, explain_expr: "ExplainExpression") -> str:
        """Build the PostgreSQL EXPLAIN prefix using bracket-option syntax.

        PostgreSQL syntax: ``EXPLAIN [ ( option [, ...] ) ] statement``

        Supported options assembled here:
        - ``ANALYZE``
        - ``FORMAT { TEXT | XML | JSON | YAML }``
        - ``VERBOSE``  (passed through ``ExplainOptions.verbose`` if present)

        The prefix is returned without a trailing newline; the caller appends
        the inner SQL separated by a single space.
        """
        from rhosocial.activerecord.backend.expression.statements import ExplainType

        options = explain_expr.options
        if options is None:
            return "EXPLAIN"

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
            return "EXPLAIN (" + ", ".join(opts) + ")"
        return "EXPLAIN"

    def supports_graph_match(self) -> bool:
        """Whether graph query MATCH clause is supported."""
        # PostgreSQL doesn't have native MATCH clause like some other systems
        # Though graph querying can be done with extensions like Apache AGE
        return False

    def supports_for_update_skip_locked(self) -> bool:
        """Whether FOR UPDATE SKIP LOCKED is supported."""
        return self.version >= (9, 5, 0)  # Supported since 9.5

    def supports_merge_statement(self) -> bool:
        """Whether MERGE statement is supported."""
        return self.version >= (15, 0, 0)  # MERGE added in PostgreSQL 15

    def supports_temporal_tables(self) -> bool:
        """Whether temporal tables are supported."""
        # PostgreSQL doesn't have built-in temporal tables
        return False

    def supports_qualify_clause(self) -> bool:
        """Whether QUALIFY clause is supported."""
        # PostgreSQL doesn't have QUALIFY clause (though can be simulated with subqueries)
        return False

    def supports_upsert(self) -> bool:
        """Whether UPSERT (ON CONFLICT) is supported."""
        return self.version >= (9, 5, 0)  # ON CONFLICT added in 9.5

    def get_upsert_syntax_type(self) -> str:
        """
        Get UPSERT syntax type.

        Returns:
            'ON CONFLICT' (PostgreSQL) or 'ON DUPLICATE KEY' (MySQL)
        """
        return "ON CONFLICT"

    def supports_lateral_join(self) -> bool:
        """Whether LATERAL joins are supported."""
        return self.version >= (9, 3, 0)  # LATERAL joins added in 9.3

    def supports_ordered_set_aggregation(self) -> bool:
        """Whether ordered-set aggregate functions are supported."""
        return self.version >= (9, 4, 0)  # Supported since 9.4

    def supports_multirange(self) -> bool:
        """Whether multirange types are supported (PostgreSQL 14+)."""
        return self.version >= (14, 0, 0)

    def supports_multirange_constructor(self) -> bool:
        """Whether multirange constructor functions are supported (PostgreSQL 14+)."""
        return self.version >= (14, 0, 0)

    def supports_inner_join(self) -> bool:
        """INNER JOIN is supported."""
        return True

    def supports_left_join(self) -> bool:
        """LEFT JOIN is supported."""
        return True

    def supports_right_join(self) -> bool:
        """RIGHT JOIN is supported."""
        return True

    def supports_full_join(self) -> bool:
        """FULL JOIN is supported."""
        return True

    def supports_cross_join(self) -> bool:
        """CROSS JOIN is supported."""
        return True

    def supports_natural_join(self) -> bool:
        """NATURAL JOIN is supported."""
        return True

    def supports_wildcard(self) -> bool:
        """Wildcard (*) is supported."""
        return True

    # region ILIKE Support
    def supports_ilike(self) -> bool:
        """ILIKE is supported."""
        return True

    def format_ilike_expression(self, column: Any, pattern: str, negate: bool = False) -> Tuple[str, Tuple]:
        """Format ILIKE expression for PostgreSQL."""
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

    # region Set Operation Support
    def supports_union(self) -> bool:
        """UNION is supported."""
        return True

    def supports_union_all(self) -> bool:
        """UNION ALL is supported."""
        return True

    def supports_intersect(self) -> bool:
        """INTERSECT is supported."""
        return True

    def supports_except(self) -> bool:
        """EXCEPT is supported."""
        return True

    def supports_set_operation_order_by(self) -> bool:
        """ORDER BY is supported for set operations."""
        return True

    def supports_set_operation_limit_offset(self) -> bool:
        """LIMIT/OFFSET is supported for set operations."""
        return True

    def supports_set_operation_for_update(self) -> bool:
        """FOR UPDATE is supported for set operations."""
        return True

    # endregion

    # region Truncate Support
    def supports_truncate(self) -> bool:
        """TRUNCATE is supported."""
        return True

    def supports_truncate_table_keyword(self) -> bool:
        """TABLE keyword is supported in TRUNCATE."""
        return True

    def supports_truncate_restart_identity(self) -> bool:
        """RESTART IDENTITY is supported since PostgreSQL 8.4."""
        return self.version >= (8, 4, 0)

    def supports_truncate_cascade(self) -> bool:
        """CASCADE is supported in TRUNCATE."""
        return True

    def format_truncate_statement(self, expr: "TruncateExpression") -> Tuple[str, tuple]:
        """Format TRUNCATE statement for PostgreSQL."""
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

    def supports_jsonb(self) -> bool:
        """Check if PostgreSQL version supports JSONB type (introduced in 9.4)."""
        return self.version >= (9, 4, 0)

    # region View Support
    def supports_or_replace_view(self) -> bool:
        """Whether CREATE OR REPLACE VIEW is supported."""
        return True  # PostgreSQL supports OR REPLACE

    def supports_temporary_view(self) -> bool:
        """Whether TEMPORARY views are supported."""
        return True  # PostgreSQL supports TEMPORARY views

    def supports_materialized_view(self) -> bool:
        """Whether materialized views are supported."""
        return True  # PostgreSQL supports materialized views since 9.3

    def supports_refresh_materialized_view(self) -> bool:
        """Whether REFRESH MATERIALIZED VIEW is supported."""
        return True  # PostgreSQL supports REFRESH MATERIALIZED VIEW

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """Whether concurrent refresh for materialized views is supported."""
        return self.version >= (9, 4, 0)  # CONCURRENTLY added in 9.4

    def supports_materialized_view_tablespace(self) -> bool:
        """Whether tablespace specification for materialized views is supported."""
        return True

    def supports_materialized_view_storage_options(self) -> bool:
        """Whether storage options for materialized views are supported."""
        return True

    def supports_if_exists_view(self) -> bool:
        """Whether DROP VIEW IF EXISTS is supported."""
        return True  # PostgreSQL supports IF EXISTS

    def supports_view_check_option(self) -> bool:
        """Whether WITH CHECK OPTION is supported."""
        return True  # PostgreSQL supports WITH CHECK OPTION

    def supports_cascade_view(self) -> bool:
        """Whether DROP VIEW CASCADE is supported."""
        return True  # PostgreSQL supports CASCADE

    def format_create_view_statement(self, expr: "CreateViewExpression") -> Tuple[str, tuple]:
        """Format CREATE VIEW statement for PostgreSQL."""
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
        """Format DROP VIEW statement for PostgreSQL."""
        parts = ["DROP VIEW"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.view_name))
        if expr.cascade:
            parts.append("CASCADE")
        return " ".join(parts), ()

    def format_create_materialized_view_statement(self, expr: "CreateMaterializedViewExpression") -> Tuple[str, tuple]:
        """Format CREATE MATERIALIZED VIEW statement for PostgreSQL."""
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
        """Format DROP MATERIALIZED VIEW statement for PostgreSQL."""
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
        """Format REFRESH MATERIALIZED VIEW statement for PostgreSQL."""
        parts = ["REFRESH MATERIALIZED VIEW"]
        if expr.concurrent and self.supports_materialized_view_concurrent_refresh():
            parts.append("CONCURRENTLY")
        parts.append(self.format_identifier(expr.view_name))
        if expr.with_data is not None:
            parts.append("WITH DATA" if expr.with_data else "WITH NO DATA")
        return " ".join(parts), ()

    # endregion

    # region Schema Support
    def supports_create_schema(self) -> bool:
        """Whether CREATE SCHEMA is supported."""
        return True

    def supports_drop_schema(self) -> bool:
        """Whether DROP SCHEMA is supported."""
        return True

    def supports_schema_if_not_exists(self) -> bool:
        """Whether CREATE SCHEMA IF NOT EXISTS is supported."""
        return True  # PostgreSQL 9.3+ supports IF NOT EXISTS

    def supports_schema_if_exists(self) -> bool:
        """Whether DROP SCHEMA IF EXISTS is supported."""
        return True

    def supports_schema_cascade(self) -> bool:
        """Whether DROP SCHEMA CASCADE is supported."""
        return True

    # endregion

    # region Index Support
    def supports_create_index(self) -> bool:
        """Whether CREATE INDEX is supported."""
        return True

    def supports_drop_index(self) -> bool:
        """Whether DROP INDEX is supported."""
        return True

    def supports_unique_index(self) -> bool:
        """Whether UNIQUE indexes are supported."""
        return True

    def supports_index_if_not_exists(self) -> bool:
        """Whether CREATE INDEX IF NOT EXISTS is supported."""
        return True  # PostgreSQL 9.5+ supports IF NOT EXISTS

    def supports_index_if_exists(self) -> bool:
        """Whether DROP INDEX IF EXISTS is supported."""
        return True

    # endregion

    # region Sequence Support
    def supports_create_sequence(self) -> bool:
        """Whether CREATE SEQUENCE is supported."""
        return True

    def supports_drop_sequence(self) -> bool:
        """Whether DROP SEQUENCE is supported."""
        return True

    # endregion

    # region Table Support
    def supports_if_not_exists_table(self) -> bool:
        """Whether CREATE TABLE IF NOT EXISTS is supported."""
        return True

    def supports_if_exists_table(self) -> bool:
        """Whether DROP TABLE IF EXISTS is supported."""
        return True

    def supports_temporary_table(self) -> bool:
        """Whether TEMPORARY tables are supported."""
        return True

    def supports_table_inheritance(self) -> bool:
        """Whether table inheritance is supported."""
        return True  # PostgreSQL supports INHERITS

    def supports_table_partitioning(self) -> bool:
        """Whether table partitioning is supported."""
        return True  # PostgreSQL supports partitioning

    def supports_table_tablespace(self) -> bool:
        """Whether tablespace specification is supported."""
        return True

    def format_create_table_statement(self, expr: "CreateTableExpression") -> Tuple[str, tuple]:
        """
        Format CREATE TABLE statement for PostgreSQL, including LIKE syntax support.

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
           - Falls back to base class implementation
           - Standard CREATE TABLE with column definitions is generated

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

    # region Trigger Support (PostgreSQL-specific)
    def supports_trigger(self) -> bool:
        return True

    def supports_create_trigger(self) -> bool:
        return True

    def supports_drop_trigger(self) -> bool:
        return True

    # endregion
