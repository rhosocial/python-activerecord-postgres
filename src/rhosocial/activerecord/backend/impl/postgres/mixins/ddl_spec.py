# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl_spec.py
"""PostgreSQL ``build_spec`` implementation (DDL feature-spec claiming).

Composed into ``PostgresDialect``. Claims the PostgreSQL-specific Specs
defined in ``..ddl_spec`` via ``isinstance`` and translates them into the
PostgreSQL expression layer (core ``PartitionClause`` for partitions, a
raw ``nextval`` default for sequence defaults). All other Specs fall through
to the generic ``DDLSpecBuildingMixin`` base translation.
"""

from typing import Any, Optional

from rhosocial.activerecord.backend.dialect.mixins.ddl_spec import DDLSpecBuildingMixin
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.statements import (
    ColumnConstraint,
    ColumnConstraintType,
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.expression.statements.ddl_spec import (
    ColumnPatchSpec,
    DDLSpec,
)

from ..ddl_spec import (
    PostgresArrayColumnSpec,
    PostgresHashPartition,
    PostgresHstoreColumnSpec,
    PostgresJsonbColumnSpec,
    PostgresListPartition,
    PostgresNetworkColumnSpec,
    PostgresRangePartition,
    PostgresSequenceDefault,
    PostgresTsVectorColumnSpec,
)


class PostgresDDLSpecMixin(DDLSpecBuildingMixin):
    """PostgreSQL-specific ``build_spec`` claiming and translation."""

    def build_spec(self, spec: "DDLSpec") -> Optional[Any]:
        """Claim PostgreSQL Specs; otherwise defer to the generic build."""
        if isinstance(spec, PostgresRangePartition):
            return self._build_postgres_partition(spec, PartitionStrategy.RANGE)
        if isinstance(spec, PostgresListPartition):
            return self._build_postgres_partition(spec, PartitionStrategy.LIST)
        if isinstance(spec, PostgresHashPartition):
            return self._build_postgres_partition(spec, PartitionStrategy.HASH)
        if isinstance(spec, PostgresSequenceDefault):
            return self._build_postgres_sequence_default(spec)
        if isinstance(spec, PostgresHstoreColumnSpec):
            return self._build_postgres_column_type(spec, "PostgresHstoreType")
        if isinstance(spec, PostgresJsonbColumnSpec):
            return self._build_postgres_column_type(spec, "JsonBType")
        if isinstance(spec, PostgresTsVectorColumnSpec):
            return self._build_postgres_column_type(spec, "PostgresTSVectorType")
        if isinstance(spec, PostgresNetworkColumnSpec):
            return self._build_postgres_network_column(spec)
        if isinstance(spec, PostgresArrayColumnSpec):
            return self._build_postgres_array_column(spec)
        return super().build_spec(spec)

    def _build_postgres_partition(self, spec, strategy: PartitionStrategy):
        """Translate a PG partition Spec to a core ``PartitionClause``."""
        return PartitionClause(
            self,
            method=strategy,
            keys=[Column(self, spec.column)],
        )

    def _build_postgres_sequence_default(self, spec: "PostgresSequenceDefault"):
        """Translate a sequence default Spec to a DEFAULT column constraint.

        The sequence name is taken from the spec, falling back to
        ``<column>_seq`` (the ``serial``-style convention).
        """
        from ..expression.sequence import (
            PostgresSequenceValueExpression,
        )

        sequence = spec.sequence or f"{spec.column}_seq"
        value = PostgresSequenceValueExpression(self, sequence)
        return ColumnConstraint(
            constraint_type=ColumnConstraintType.DEFAULT,
            name=None,
            default_value=value,
        )

    def _build_postgres_column_type(self, spec, type_name: str) -> Optional[Any]:
        """Translate a simple native-type Spec to a column patch."""
        from rhosocial.activerecord.backend.expression.types import JsonBType
        from rhosocial.activerecord.backend.impl.postgres.expression import types as _t

        # JsonBType lives in the core type vocabulary; the rest are PG-native.
        if type_name == "JsonBType":
            return ColumnPatchSpec(
                column=spec.column,
                patched_data_type=JsonBType(self),
            )
        type_cls = getattr(_t, type_name, None)
        if type_cls is None:
            return None
        return ColumnPatchSpec(
            column=spec.column,
            patched_data_type=type_cls(self),
        )

    def _build_postgres_network_column(self, spec: "PostgresNetworkColumnSpec"):
        """Translate a network-address Spec to a column patch."""
        from rhosocial.activerecord.backend.impl.postgres.expression import types as _t

        kinds = {
            "INET": "PostgresInetType",
            "CIDR": "PostgresCidrType",
            "MACADDR": "PostgresMacAddrType",
            "MACADDR8": "PostgresMacAddr8Type",
        }
        type_cls = getattr(_t, kinds[spec.kind])
        return ColumnPatchSpec(
            column=spec.column,
            patched_data_type=type_cls(self),
        )

    def _build_postgres_array_column(self, spec: "PostgresArrayColumnSpec"):
        """Translate an array Spec to a column patch."""
        from rhosocial.activerecord.backend.impl.postgres.expression import types as _t

        return ColumnPatchSpec(
            column=spec.column,
            patched_data_type=_t.PostgresArrayType(
                self, element_type=spec.element_type, dimensions=spec.dimensions
            ),
        )