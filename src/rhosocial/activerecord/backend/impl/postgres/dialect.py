# src/rhosocial/activerecord/backend/impl/postgres/dialect.py
"""
PostgreSQL backend SQL dialect implementation.

This dialect implements protocols for features that PostgreSQL actually supports,
based on the PostgreSQL version provided at initialization.
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
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
)
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
)
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class PostgresDialect(
    SQLDialectBase,
    # Include mixins for features that PostgreSQL supports (with version-dependent implementations)
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
    # Protocols for type checking
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
    # endregion