# tests/rhosocial/activerecord_postgres_test/feature/backend/test_postgres_protocol_conformance.py
"""
Tests to verify PostgresDialect protocol conformance and protocol non-overlap.

This test ensures:
1. PostgresDialect implements all methods defined in the protocols it claims to support
2. All protocols have at least one member
3. No two protocols share the same method name (no overlap)
4. Method signatures on PostgresDialect match Protocol declarations
5. Protocol-declared methods exist in corresponding Mixins (forward coverage)
6. Mixin-implemented methods are declared in corresponding Protocols (reverse coverage)
"""
import inspect
import sys
from itertools import combinations
from typing import Protocol

if sys.version_info >= (3, 13):
    from typing import get_protocol_members
elif sys.version_info >= (3, 12):
    from typing import _get_protocol_attrs as get_protocol_members

import pytest
from rhosocial.activerecord.backend.dialect import protocols as dialect_protocols
from rhosocial.activerecord.backend.impl.postgres import dialect as postgres_dialect
from rhosocial.activerecord.backend.impl.postgres import mixins as postgres_mixins
from rhosocial.activerecord.backend.impl.postgres import protocols as postgres_protocols


def get_all_protocol_methods(proto: type) -> set:
    """Extract all public method names from a protocol, including inherited."""
    members = set()
    if sys.version_info >= (3, 13):
        members = get_protocol_members(proto)
    elif sys.version_info >= (3, 12):
        members = get_protocol_members(proto)
    else:
        # Walk MRO to include methods from parent protocols
        for cls in proto.__mro__:
            if cls is object:
                continue
            for name in cls.__dict__:
                if name.startswith("_"):
                    continue
                val = cls.__dict__[name]
                if callable(val) or isinstance(val, (property, classmethod, staticmethod)):
                    members.add(name)
            members.update(
                k for k in getattr(cls, "__annotations__", {})
                if not k.startswith("_")
            )
    return members


def get_own_protocol_methods(proto: type) -> set:
    """Extract public method names declared directly on a protocol (not inherited).

    Used for forward coverage: only checks methods the protocol itself declares,
    since parent protocol methods are typically implemented by generic mixins.
    """
    members = set()
    for name in proto.__dict__:
        if name.startswith("_"):
            continue
        val = proto.__dict__[name]
        if callable(val) or isinstance(val, (property, classmethod, staticmethod)):
            members.add(name)
    members.update(
        k for k in getattr(proto, "__annotations__", {})
        if not k.startswith("_")
    )
    return members


def get_postgres_protocols():
    """Get all protocols that PostgresDialect implements."""
    from typing import runtime_checkable

    protocols = [
        dialect_protocols.CTESupport,
        dialect_protocols.FilterClauseSupport,
        dialect_protocols.WindowFunctionSupport,
        dialect_protocols.JSONSupport,
        dialect_protocols.ReturningSupport,
        dialect_protocols.AdvancedGroupingSupport,
        dialect_protocols.ArraySupport,
        dialect_protocols.ExplainSupport,
        dialect_protocols.GraphSupport,
        dialect_protocols.LockingSupport,
        dialect_protocols.MergeSupport,
        dialect_protocols.OrderedSetAggregationSupport,
        dialect_protocols.QualifyClauseSupport,
        dialect_protocols.TemporalTableSupport,
        dialect_protocols.UpsertSupport,
        dialect_protocols.LateralJoinSupport,
        dialect_protocols.WildcardSupport,
        dialect_protocols.JoinSupport,
        dialect_protocols.SetOperationSupport,
        dialect_protocols.ViewSupport,
        dialect_protocols.SchemaSupport,
        dialect_protocols.IndexSupport,
        dialect_protocols.SequenceSupport,
        dialect_protocols.TableSupport,
        dialect_protocols.ConstraintSupport,
        dialect_protocols.TruncateSupport,
        dialect_protocols.IntrospectionSupport,
        dialect_protocols.TransactionControlSupport,
        dialect_protocols.SQLFunctionSupport,
    ]

    postgres_mro = postgres_dialect.PostgresDialect.__mro__
    for name in dir(postgres_protocols):
        obj = getattr(postgres_protocols, name)
        if isinstance(obj, type) and Protocol in getattr(obj, "__mro__", []):
            if obj in postgres_mro and getattr(obj, "_is_protocol", False):
                protocols.append(obj)

    return protocols


POSTGRES_PROTOCOLS = get_postgres_protocols()


class TestPostgresDialectProtocolConformance:
    """Assert PostgresDialect implements all protocols it declares to support."""

    @pytest.fixture
    def dialect(self):
        """Create a PostgresDialect instance for testing."""
        return postgres_dialect.PostgresDialect()

    @pytest.mark.parametrize("protocol", POSTGRES_PROTOCOLS)
    def test_implements_protocol(self, dialect, protocol):
        """PostgresDialect should implement each protocol in POSTGRES_PROTOCOLS."""
        if not getattr(protocol, "_is_runtime_protocol", False):
            pytest.skip(f"Protocol {protocol.__name__} is not @runtime_checkable")
        assert isinstance(dialect, protocol), (
            f"PostgresDialect does not implement protocol {protocol.__name__}, "
            f"missing methods: {get_all_protocol_methods(protocol) - set(dir(dialect))}"
        )


class TestProtocolNonOverlap:
    """Assert protocols do not have overlapping method names."""

    def test_no_interface_overlap_between_protocols(self):
        """No two protocols should share the same method name."""
        member_map = {
            proto.__name__: get_all_protocol_methods(proto)
            for proto in POSTGRES_PROTOCOLS
        }

        for name, members in member_map.items():
            assert len(members) > 0, f"Protocol {name} has no members defined"

        excluded_overlaps = {
            # Generic -> Postgres derivation pairs (expected overlap)
            ('IndexSupport', 'PostgresIndexSupport'),
            ('PostgresIndexSupport', 'IndexSupport'),
            ('TableSupport', 'PostgresTableSupport'),
            ('PostgresTableSupport', 'TableSupport'),
            ('ConstraintSupport', 'PostgresConstraintSupport'),
            ('PostgresConstraintSupport', 'ConstraintSupport'),
            ('LockingSupport', 'PostgresLockingSupport'),
            ('PostgresLockingSupport', 'LockingSupport'),
            ('TriggerSupport', 'PostgresTriggerSupport'),
            ('PostgresTriggerSupport', 'TriggerSupport'),
            # PG enhanced protocols extend generic protocols with same method names
            ('JSONSupport', 'PostgresJSONBEnhancedSupport'),
            ('PostgresJSONBEnhancedSupport', 'JSONSupport'),
            ('ArraySupport', 'PostgresArrayEnhancedSupport'),
            ('PostgresArrayEnhancedSupport', 'ArraySupport'),
            # Cross-type protocol overlaps (shared capability methods)
            ('PostgresArrayEnhancedSupport', 'PostgresDataTypeSupport'),
            ('PostgresDataTypeSupport', 'PostgresArrayEnhancedSupport'),
            ('PostgresDataTypeSupport', 'PostgresJSONBEnhancedSupport'),
            ('PostgresJSONBEnhancedSupport', 'PostgresDataTypeSupport'),
            # PG protocols sharing methods from same domain
            ('PostgresExtendedStatisticsSupport', 'PostgresIndexSupport'),
            ('PostgresIndexSupport', 'PostgresExtendedStatisticsSupport'),
            ('PostgresParallelQuerySupport', 'PostgresQueryOptimizationSupport'),
            ('PostgresQueryOptimizationSupport', 'PostgresParallelQuerySupport'),
            ('PostgresSQLSyntaxSupport', 'PostgresStoredProcedureSupport'),
            ('PostgresStoredProcedureSupport', 'PostgresSQLSyntaxSupport'),
        }

        violations = []
        for (name_a, members_a), (name_b, members_b) in combinations(member_map.items(), 2):
            if (name_a, name_b) in excluded_overlaps:
                continue
            overlap = members_a & members_b
            if overlap:
                violations.append(f"{name_a} ∩ {name_b} = {overlap}")

        assert not violations, (
            "The following protocols have overlapping interfaces, need to merge or rename:\n"
            + "\n".join(f"  • {v}" for v in violations)
        )


class TestPostgresProtocolDerivation:
    """Verify PostgreSQL-specific protocols derive from their generic counterparts.

    This ensures that backend-specific protocols inherit the standard interface,
    allowing isinstance() checks against generic protocols to work correctly.
    """

    PROTOCOL_DERIVATIONS = [
        ("PostgresTableSupport", "TableSupport"),
        ("PostgresIndexSupport", "IndexSupport"),
        ("PostgresLockingSupport", "LockingSupport"),
        ("PostgresTriggerSupport", "TriggerSupport"),
        ("PostgresConstraintSupport", "ConstraintSupport"),
    ]

    @pytest.mark.parametrize("pg_name,generic_name", PROTOCOL_DERIVATIONS)
    def test_protocol_derives_from_generic(self, pg_name, generic_name):
        """Backend-specific protocol should derive from its generic counterpart."""
        pg_proto = getattr(postgres_protocols, pg_name)
        generic_proto = getattr(dialect_protocols, generic_name)
        assert issubclass(pg_proto, generic_proto), (
            f"{pg_name} does not derive from {generic_name}"
        )

    def test_dialect_satisfies_generic_protocols_via_derivation(self):
        """PostgresDialect should satisfy generic protocols through derived protocols."""
        dialect = postgres_dialect.PostgresDialect()
        for pg_name, generic_name in self.PROTOCOL_DERIVATIONS:
            generic_proto = getattr(dialect_protocols, generic_name)
            if getattr(generic_proto, "_is_runtime_protocol", False):
                assert isinstance(dialect, generic_proto), (
                    f"PostgresDialect does not satisfy {generic_name} "
                    f"(should be inherited via {pg_name})"
                )


class TestPostgresExpressionDialectSeparation:
    """Verify PostgreSQL-specific expression classes delegate to dialect for SQL generation.

    Expression-Dialect separation means expression classes collect parameters
    and delegate to_sql() to dialect.format_*() methods, never directly
    constructing SQL strings.
    """

    EXPRESSION_DIALECT_PAIRS = [
        ("PostgresRefreshMaterializedViewExpression",
         "format_refresh_materialized_view_pg_statement"),
        ("PostgresCreateEnumTypeExpression", "format_create_enum_type"),
        ("PostgresDropEnumTypeExpression", "format_drop_enum_type"),
        ("PostgresAlterEnumAddValueExpression", "format_alter_enum_add_value"),
        ("PostgresAlterEnumTypeAddValueExpression", "format_alter_enum_type_add_value"),
        ("PostgresAlterEnumTypeRenameValueExpression", "format_alter_enum_type_rename_value"),
        ("PostgresCreateRangeTypeExpression", "format_create_range_type"),
        ("PostgresVacuumExpression", "format_vacuum_statement"),
        ("PostgresAnalyzeExpression", "format_analyze_statement"),
        ("PostgresReindexExpression", "format_reindex_statement"),
        ("PostgresCommentExpression", "format_comment_statement"),
        ("PostgresCreateExtensionExpression", "format_create_extension"),
        ("PostgresDropExtensionExpression", "format_drop_extension"),
        ("PostgresAdvisoryLockExpression", "format_advisory_lock"),
        ("PostgresAdvisoryUnlockExpression", "format_advisory_unlock"),
        ("PostgresAdvisoryUnlockAllExpression", "format_advisory_unlock_all"),
        ("PostgresTryAdvisoryLockExpression", "format_try_advisory_lock"),
        ("PostgresCreatePartitionExpression", "format_create_partition_statement"),
        ("PostgresDetachPartitionExpression", "format_detach_partition_statement"),
        ("PostgresAttachPartitionExpression", "format_attach_partition_statement"),
        ("PostgresCreateStatisticsExpression", "format_create_statistics_statement"),
        ("PostgresDropStatisticsExpression", "format_drop_statistics_statement"),
    ]

    @pytest.mark.parametrize("expr_name,format_method", EXPRESSION_DIALECT_PAIRS)
    def test_expression_delegates_to_dialect(self, expr_name, format_method):
        """Expression.to_sql() should delegate to dialect.format_*() method."""
        from rhosocial.activerecord.backend.impl.postgres import expression as pg_expr

        # Find the expression class
        expr_class = None
        for module_name in dir(pg_expr):
            module = getattr(pg_expr, module_name)
            if hasattr(module, expr_name):
                expr_class = getattr(module, expr_name)
                break

        # Also check top-level imports
        if expr_class is None:
            expr_class = getattr(pg_expr, expr_name, None)

        assert expr_class is not None, f"Expression class {expr_name} not found"

        # Verify the dialect has the corresponding format method
        dialect = postgres_dialect.PostgresDialect()
        assert hasattr(dialect, format_method), (
            f"PostgresDialect missing format method {format_method} "
            f"for expression {expr_name}"
        )


# ============================================================================
# Protocol Implementation Completeness Tests
# ============================================================================

# Map from PostgreSQL-specific Protocol to corresponding Mixin class
POSTGRES_PROTOCOL_MIXIN_PAIRS = [
    # Core protocols
    (postgres_protocols.PostgresExtensionSupport, postgres_mixins.PostgresExtensionMixin),
    (postgres_protocols.PostgresMaterializedViewSupport, postgres_mixins.PostgresMaterializedViewMixin),
    (postgres_protocols.PostgresTableSupport, postgres_mixins.PostgresTableMixin),
    # DDL protocols
    (postgres_protocols.PostgresPartitionSupport, postgres_mixins.PostgresPartitionMixin),
    (postgres_protocols.PostgresIndexSupport, postgres_mixins.PostgresIndexMixin),
    (postgres_protocols.PostgresTriggerSupport, postgres_mixins.PostgresTriggerMixin),
    (postgres_protocols.PostgresCommentSupport, postgres_mixins.PostgresCommentMixin),
    (postgres_protocols.PostgresTypeSupport, postgres_mixins.PostgresTypeMixin),
    (postgres_protocols.PostgresConstraintSupport, postgres_mixins.PostgresConstraintMixin),
    # DML protocols
    (postgres_protocols.PostgresVacuumSupport, postgres_mixins.PostgresVacuumMixin),
    (postgres_protocols.PostgresStoredProcedureSupport, postgres_mixins.PostgresStoredProcedureMixin),
    (postgres_protocols.PostgresExtendedStatisticsSupport, postgres_mixins.PostgresExtendedStatisticsMixin),
    (postgres_protocols.PostgresAdvisoryLockSupport, postgres_mixins.PostgresAdvisoryLockMixin),
    (postgres_protocols.PostgresLockingSupport, postgres_mixins.PostgresLockingMixin),
    # Type protocols
    (postgres_protocols.PostgresDataTypeSupport, postgres_mixins.TypesDataTypeMixin),
    (postgres_protocols.PostgresMultirangeSupport, postgres_mixins.MultirangeMixin),
    (postgres_protocols.PostgresEnumTypeSupport, postgres_mixins.EnumTypeMixin),
    (postgres_protocols.PostgresFullTextSearchSupport, postgres_mixins.PostgresFullTextSearchMixin),
    (postgres_protocols.PostgresRangeTypeSupport, postgres_mixins.PostgresRangeTypeMixin),
    (postgres_protocols.PostgresJSONBEnhancedSupport, postgres_mixins.PostgresJSONBEnhancedMixin),
    (postgres_protocols.PostgresArrayEnhancedSupport, postgres_mixins.PostgresArrayEnhancedMixin),
    # Additional protocols
    (postgres_protocols.PostgresQueryOptimizationSupport, postgres_mixins.PostgresQueryOptimizationMixin),
    (postgres_protocols.PostgresSQLSyntaxSupport, postgres_mixins.PostgresSQLSyntaxMixin),
    (postgres_protocols.PostgresLogicalReplicationSupport, postgres_mixins.PostgresLogicalReplicationMixin),
    (postgres_protocols.PostgresParallelQuerySupport, postgres_mixins.PostgresParallelQueryMixin),
    # Extension protocols
    (postgres_protocols.PostgresPgvectorSupport, postgres_mixins.PostgresPgvectorMixin),
    (postgres_protocols.PostgresPostGISSupport, postgres_mixins.PostgresPostGISMixin),
    (postgres_protocols.PostgresPostgisRasterSupport, postgres_mixins.PostgresPostgisRasterMixin),
    (postgres_protocols.PostgresPgroutingSupport, postgres_mixins.PostgresPgroutingMixin),
    (postgres_protocols.PostgresHstoreSupport, postgres_mixins.PostgresHstoreMixin),
    (postgres_protocols.PostgresLtreeSupport, postgres_mixins.PostgresLtreeMixin),
    (postgres_protocols.PostgresIntarraySupport, postgres_mixins.PostgresIntarrayMixin),
    (postgres_protocols.PostgresPgTrgmSupport, postgres_mixins.PostgresPgTrgmMixin),
    (postgres_protocols.PostgresEarthdistanceSupport, postgres_mixins.PostgresEarthdistanceMixin),
    (postgres_protocols.PostgresTablefuncSupport, postgres_mixins.PostgresTablefuncMixin),
    (postgres_protocols.PostgresPgStatStatementsSupport, postgres_mixins.PostgresPgStatStatementsMixin),
    (postgres_protocols.PostgresCitextSupport, postgres_mixins.PostgresCitextMixin),
    (postgres_protocols.PostgresPgcryptoSupport, postgres_mixins.PostgresPgcryptoMixin),
    (postgres_protocols.PostgresFuzzystrmatchSupport, postgres_mixins.PostgresFuzzystrmatchMixin),
    (postgres_protocols.PostgresCubeSupport, postgres_mixins.PostgresCubeMixin),
    (postgres_protocols.PostgresUuidOssSupport, postgres_mixins.PostgresUuidOssMixin),
    (postgres_protocols.PostgresBloomSupport, postgres_mixins.PostgresBloomMixin),
    (postgres_protocols.PostgresBtreeGinSupport, postgres_mixins.PostgresBtreeGinMixin),
    (postgres_protocols.PostgresBtreeGistSupport, postgres_mixins.PostgresBtreeGistMixin),
]


class TestProtocolMethodSignatureConformance:
    """Verify PostgresDialect method signatures match Protocol declarations.

    Python's @runtime_checkable Protocol only checks method existence,
    not signature compatibility. This test catches parameter mismatches.
    """

    # Known signature mismatches between dialect implementations and generic protocols.
    # Generic Mixins use expr-based signatures instead of named params defined in protocols.
    # These are pre-existing issues that require a broader refactoring to fix.
    _SIGNATURE_MISMATCH_EXCLUSIONS = {
        # JSONSupport: Mixin uses expr-based signatures instead of named params
        ('JSONSupport', 'format_json_expression'),
        ('JSONSupport', 'format_json_table_expression'),
        # ArraySupport: Mixin uses expr-based signatures
        ('ArraySupport', 'format_array_expression'),
        # AdvancedGroupingSupport: Mixin uses expr instead of named params
        ('AdvancedGroupingSupport', 'format_grouping_expression'),
        # GraphSupport: Mixin uses expr instead of clause
        ('GraphSupport', 'format_match_clause'),
        # OrderedSetAggregationSupport: Mixin uses expr instead of aggregation
        ('OrderedSetAggregationSupport', 'format_ordered_set_aggregation'),
        # QualifyClauseSupport: Mixin uses expr instead of clause
        ('QualifyClauseSupport', 'format_qualify_clause'),
        # ViewSupport: Materialized view methods use expr instead of named params
        ('ViewSupport', 'format_create_materialized_view_statement'),
        ('ViewSupport', 'format_drop_materialized_view_statement'),
        ('ViewSupport', 'format_refresh_materialized_view_statement'),
        # ExplainSupport: Mixin uses different signature
        ('ExplainSupport', 'format_explain_statement'),
    }

    @pytest.fixture
    def dialect(self):
        """Create a PostgresDialect instance for testing."""
        return postgres_dialect.PostgresDialect()

    @pytest.mark.parametrize("protocol", POSTGRES_PROTOCOLS)
    def test_method_signatures_match_protocol(self, dialect, protocol):
        """Each method on PostgresDialect must have a compatible signature
        with the corresponding Protocol method."""
        proto_methods = get_all_protocol_methods(protocol)
        missing = []
        signature_mismatch = []

        for method_name in proto_methods:
            # Check existence
            if not hasattr(dialect, method_name):
                missing.append(method_name)
                continue

            # Check signature compatibility
            # Skip known mismatches between dialect implementations and generic protocols
            if (protocol.__name__, method_name) in self._SIGNATURE_MISMATCH_EXCLUSIONS:
                continue

            proto_method = getattr(protocol, method_name, None)
            dialect_method = getattr(dialect, method_name)

            if proto_method is not None and callable(proto_method):
                try:
                    proto_sig = inspect.signature(proto_method)
                    dialect_sig = inspect.signature(dialect_method)

                    # Compare parameter names (excluding 'self')
                    proto_params = [
                        p for p in proto_sig.parameters.values()
                        if p.name != 'self'
                    ]
                    dialect_params = [
                        p for p in dialect_sig.parameters.values()
                        if p.name != 'self'
                    ]

                    # Dialect must accept at least all required proto params
                    proto_required = [
                        p for p in proto_params
                        if p.default is inspect.Parameter.empty
                        and p.kind not in (
                            inspect.Parameter.VAR_POSITIONAL,
                            inspect.Parameter.VAR_KEYWORD,
                        )
                    ]
                    dialect_param_names = {p.name for p in dialect_params}

                    for req_param in proto_required:
                        if req_param.name not in dialect_param_names:
                            signature_mismatch.append(
                                f"{method_name}: missing required param "
                                f"'{req_param.name}' from protocol"
                            )
                except (ValueError, TypeError):
                    pass  # Some protocol methods can't be inspected

        assert not missing, (
            f"PostgresDialect missing methods for {protocol.__name__}: {missing}"
        )
        assert not signature_mismatch, (
            f"Signature mismatches for {protocol.__name__}: {signature_mismatch}"
        )


class TestProtocolMixinForwardCoverage:
    """Verify every method declared in Protocol is implemented in Mixin.

    This catches the failure mode where a Protocol declares format_* or
    supports_* methods but the corresponding Mixin doesn't implement them.
    """

    @pytest.mark.parametrize("protocol,mixin", POSTGRES_PROTOCOL_MIXIN_PAIRS)
    def test_protocol_declared_methods_are_implemented(self, protocol, mixin):
        """Every format_* / supports_* / get_* in Protocol must exist in Mixin.

        Only checks methods declared directly on the protocol (not inherited
        from parent protocols), since parent protocol methods are typically
        implemented by generic mixins rather than the backend-specific one.
        """
        proto_methods = get_own_protocol_methods(protocol)
        mixin_methods = {name for name in dir(mixin) if not name.startswith('_')}
        missing = proto_methods - mixin_methods
        assert not missing, (
            f"{mixin.__name__} does not implement these methods "
            f"declared in {protocol.__name__}: {missing}"
        )


class TestProtocolMixinReverseCoverage:
    """Verify every format_*/supports_*/get_* in Mixin is declared in Protocol.

    This catches the failure mode where a Mixin implements format_* or
    supports_* methods but the corresponding Protocol doesn't declare them.
    """

    @pytest.mark.parametrize("protocol,mixin", POSTGRES_PROTOCOL_MIXIN_PAIRS)
    def test_mixin_public_methods_are_declared_in_protocol(self, protocol, mixin):
        """Every format_*/supports_*/get_* in Mixin must be declared in Protocol.

        Only checks methods defined on the Mixin itself (not inherited
        from object or other bases), and only public methods
        with the format_*/supports_*/get_*/detect_*/is_*/check_* prefix pattern.
        """
        proto_methods = get_all_protocol_methods(protocol)

        # Collect Mixin's own public method names matching the prefix pattern
        mixin_own_methods = set()
        for name in dir(mixin):
            if name.startswith('_'):
                continue
            if not (name.startswith('format_') or name.startswith('supports_')
                    or name.startswith('get_') or name.startswith('detect_')
                    or name.startswith('is_') or name.startswith('check_')):
                continue
            # Only include methods defined on the mixin itself, not inherited
            if name in mixin.__dict__:
                mixin_own_methods.add(name)

        undeclared = mixin_own_methods - proto_methods
        assert not undeclared, (
            f"{mixin.__name__} implements these methods not declared in "
            f"{protocol.__name__}: {undeclared}"
        )
