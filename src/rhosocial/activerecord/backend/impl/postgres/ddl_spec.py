# src/rhosocial/activerecord/backend/impl/postgres/ddl_spec.py
"""PostgreSQL-specific DDL feature specs.

Plain declaration objects (no dialect at definition time) recognized by the
PostgreSQL dialect's ``build_spec`` via ``isinstance``. Only the PostgreSQL
dialect claims these specs; every other backend silently ignores them
(``build_spec`` returns ``None``).
"""

from typing import Optional

from rhosocial.activerecord.backend.expression.statements.ddl_spec import (
    ColumnTypeSpec,
    DDLSpec,
    PartitionSpec,
)


class PostgresRangePartition(PartitionSpec):
    """PostgreSQL declarative ``PARTITION BY RANGE`` declaration.

    Example::

        PostgresRangePartition(column="created_at")
    """

    __slots__ = ("column",)

    def __init__(self, column: str):
        if not column:
            raise ValueError("PostgresRangePartition requires a partition column")
        self.column = column


class PostgresListPartition(PartitionSpec):
    """PostgreSQL declarative ``PARTITION BY LIST`` declaration."""

    __slots__ = ("column",)

    def __init__(self, column: str):
        if not column:
            raise ValueError("PostgresListPartition requires a partition column")
        self.column = column


class PostgresHashPartition(PartitionSpec):
    """PostgreSQL declarative ``PARTITION BY HASH`` declaration."""

    __slots__ = ("column",)

    def __init__(self, column: str):
        if not column:
            raise ValueError("PostgresHashPartition requires a partition column")
        self.column = column


class PostgresSequenceDefault(DDLSpec):
    """A PostgreSQL sequence-backed column default (``nextval('seq')``).

    Example::

        PostgresSequenceDefault(column="id", sequence="users_id_seq")

    Renders as ``DEFAULT nextval('users_id_seq')``. When ``sequence`` is
    omitted the derived name ``<column>_seq`` is used.
    """

    __slots__ = ("column", "sequence")

    def __init__(self, column: str, sequence: Optional[str] = None):
        if not column:
            raise ValueError("PostgresSequenceDefault requires a column name")
        self.column = column
        self.sequence = sequence


class PostgresHstoreColumnSpec(ColumnTypeSpec):
    """A PostgreSQL ``hstore`` column (key/value store)."""

    __slots__ = ()


class PostgresJsonbColumnSpec(ColumnTypeSpec):
    """A PostgreSQL ``jsonb`` column (binary JSON)."""

    __slots__ = ()


class PostgresTsVectorColumnSpec(ColumnTypeSpec):
    """A PostgreSQL ``tsvector`` column (full-text search vector)."""

    __slots__ = ()


class PostgresNetworkColumnSpec(ColumnTypeSpec):
    """A PostgreSQL network-address column (``inet`` / ``cidr`` / ``macaddr`` /
    ``macaddr8``)."""

    __slots__ = ("kind",)

    def __init__(self, column: str, kind: str = "INET"):
        super().__init__(column)
        kind = (kind or "INET").upper()
        valid = {"INET", "CIDR", "MACADDR", "MACADDR8"}
        if kind not in valid:
            raise ValueError(
                f"Invalid PostgreSQL network kind {kind!r}; expected one of {sorted(valid)}"
            )
        self.kind = kind


class PostgresArrayColumnSpec(ColumnTypeSpec):
    """A PostgreSQL array column.

    ``element_type`` is a ready ``DataType`` instance (e.g. ``TextType()``);
    ``dimensions`` selects the array dimensionality.
    """

    __slots__ = ("element_type", "dimensions")

    def __init__(self, column: str, element_type: "object", *, dimensions: int = 1):
        super().__init__(column)
        if element_type is None:
            raise ValueError("PostgresArrayColumnSpec requires an element type")
        if dimensions <= 0:
            raise ValueError("PostgresArrayColumnSpec requires a positive dimension count")
        self.element_type = element_type
        self.dimensions = dimensions