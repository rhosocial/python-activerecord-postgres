# src/rhosocial/activerecord/backend/impl/postgres/introspection/types.py
"""PostgreSQL-specific introspection result types.

Core introspection covers the objects every backend shares: tables, views,
columns, indexes, foreign keys and triggers. A sequence is not one of those --
``information_schema`` has no sequence entry point that core models -- so its
row shape lives here, next to the code that reads it.

The type is frozen: a row read from the catalog is a snapshot, and a caller that
edited it would be lying about what the server reported.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.introspection.types import ColumnDataProfile

__all__ = [
    "SequenceInfo",
    "PostgresColumnDataProfile",
    "PostgresPartitionStrategy",
    "PostgresPartitionBound",
    "PostgresPartitionInfo",
    "PostgresRoutineKind",
    "PostgresRoutineInfo",
    "PostgresTypeKind",
    "PostgresTypeInfo",
    "PostgresOperatorInfo",
    "PostgresOperatorClassInfo",
    "PostgresOperatorFamilyInfo",
]


@dataclass(frozen=True)
class SequenceInfo:
    """One sequence, as ``information_schema.sequences`` describes it.

    ``last_value`` comes from ``pg_sequences`` (PostgreSQL 10+) rather than
    from ``information_schema``, whose sequence view has no such column. It is
    ``None`` in three situations, none of which is an error:

    * the sequence has never handed out or written a value in this database;
    * the connected role has neither ``SELECT`` nor ``USAGE`` on it, so the
      catalog view withholds the value;
    * the server predates ``pg_sequences`` (PostgreSQL 9.x), where the join
      has no source and the query reports the column as absent.

    ``cache_size`` is withheld under the same conditions, for the same reasons.
    """

    name: str
    schema: str
    data_type: str = ""
    start_value: Optional[int] = None
    minimum_value: Optional[int] = None
    maximum_value: Optional[int] = None
    increment: Optional[int] = None
    cycle: bool = False
    cache_size: Optional[int] = None
    last_value: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)


class PostgresPartitionStrategy(str, Enum):
    """How PostgreSQL distributes rows across a partitioned table.

    ``RELKIND`` is not the strategy: ``pg_get_partkeydef`` returns the strategy
    as the leading word of its text (``RANGE (created_at)``), and that is the
    only place the catalog states it. ``UNKNOWN`` is an answer, not an error --
    a future strategy this parser does not know is still a partition.
    """

    RANGE = "range"
    LIST = "list"
    HASH = "hash"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PostgresPartitionBound:
    """A partition's bound, decoded from ``pg_get_expr(relpartbound)``.

    The bound is text in the catalog. Its shape is one of three forms, and
    which one applies follows the parent's strategy:

    * ``FOR VALUES FROM (...) TO (...)`` -- ``RANGE``; ``lower`` and ``upper``
      hold the two endpoints.
    * ``FOR VALUES IN (...)`` -- ``LIST``; ``values`` holds the members.
    * ``FOR VALUES WITH (MODULUS n, REMAINDER r)`` -- ``HASH``; ``modulus``
      and ``remainder`` hold the two numbers.

    ``DEFAULT`` is a partition too, and it fits none of the three. It keeps
    ``text`` and leaves the typed slots empty, which says "this partition
    catches what the others do not" rather than inventing a bound.
    """

    lower: Optional[str] = None
    upper: Optional[str] = None
    values: List[str] = field(default_factory=list)
    modulus: Optional[int] = None
    remainder: Optional[int] = None
    text: Optional[str] = None


@dataclass(frozen=True)
class PostgresPartitionInfo:
    """One child partition of a partitioned PostgreSQL table.

    The parent is the table a model should map to; ``name`` is the physical
    child. ``strategy`` and ``key_columns`` come from the parent's
    ``pg_get_partkeydef`` text, and ``bound`` from the child's own
    ``relpartbound`` expression. ``row_count`` and ``size_bytes`` are the
    child's own storage facts, when the query was asked for them.

    ``extra`` carries catalog facts this common shape has no slot for, such as
    the parent OID or the child's ``relkind``.
    """

    table: str
    name: str
    schema: Optional[str] = None
    strategy: PostgresPartitionStrategy = PostgresPartitionStrategy.UNKNOWN
    key_columns: List[str] = field(default_factory=list)
    key_expression: Optional[str] = None
    parent: Optional[str] = None
    bound: Optional[PostgresPartitionBound] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    comment: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


class PostgresRoutineKind(str, Enum):
    """What ``pg_proc.prokind`` says a routine is."""

    FUNCTION = "function"
    PROCEDURE = "procedure"
    AGGREGATE = "aggregate"
    WINDOW = "window"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PostgresRoutineInfo:
    """One function, procedure, aggregate or window function.

    ``identity_arguments`` is the argument list that identifies the routine
    among its overloads, as PostgreSQL itself spells it. ``result_type`` is
    the ``pg_get_function_result`` text; for a procedure it is empty because
    procedures return no value. ``volatility`` is the catalog's word
    (``immutable``/``stable``/``volatile``), not a boolean.
    """

    name: str
    schema: Optional[str] = None
    kind: PostgresRoutineKind = PostgresRoutineKind.FUNCTION
    identity_arguments: Optional[str] = None
    result_type: Optional[str] = None
    language: Optional[str] = None
    volatility: Optional[str] = None
    security_definer: bool = False
    comment: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


class PostgresTypeKind(str, Enum):
    """The user-visible kinds of PostgreSQL type this family reports.

    ``pg_type.typtype`` also has base (``b``) and pseudo (``p``) codes, but
    the introspection family is about the kinds a schema defines itself:
    enums, domains, composites, ranges and multiranges. A system base type
    belongs to ``pg_catalog`` and is not the schema's object.
    """

    ENUM = "enum"
    DOMAIN = "domain"
    COMPOSITE = "composite"
    RANGE = "range"
    MULTIRANGE = "multirange"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PostgresTypeInfo:
    """One enum, domain, composite, range or multirange type.

    ``labels`` is the enum's ordered value list. ``fields`` is the composite's
    ordered attribute list -- for a table's row type these are the table's
    columns, which is why ``extra['relation_kind']`` also travels: it tells a
    real ``CREATE TYPE ... AS (...)`` (``typrelid`` points at a ``'c'``
    relation) apart from a table row type (``'r'``/``'p'``/...).

    ``base_type`` is the domain's underlying type or the range's subtype.
    ``domain_not_null``/``domain_default`` are ``None`` unless the row really
    is a domain; ``None`` here means "not a domain fact" rather than an
    unknown one.
    """

    name: str
    schema: Optional[str] = None
    kind: PostgresTypeKind = PostgresTypeKind.UNKNOWN
    labels: List[str] = field(default_factory=list)
    fields: List[str] = field(default_factory=list)
    base_type: Optional[str] = None
    domain_not_null: Optional[bool] = None
    domain_default: Optional[str] = None
    comment: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PostgresOperatorInfo:
    """One row of ``pg_operator``.

    ``operator_kind`` is the catalog's ``oprkind`` word: ``binary``, ``prefix``
    or ``postfix``. ``left_type``/``right_type`` are ``None`` for a position
    the operator does not have -- a prefix operator has no left operand, and
    ``format_type(0, NULL)`` would be a different type rather than "absent".
    """

    name: str
    schema: Optional[str] = None
    operator_kind: Optional[str] = None
    left_type: Optional[str] = None
    right_type: Optional[str] = None
    result_type: Optional[str] = None
    function_name: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PostgresOperatorClassInfo:
    """One row of ``pg_opclass``: the operator class an index method uses."""

    name: str
    schema: Optional[str] = None
    method: Optional[str] = None
    input_type: Optional[str] = None
    key_type: Optional[str] = None
    is_default: bool = False
    family: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PostgresOperatorFamilyInfo:
    """One row of ``pg_opfamily``: a family of operator classes."""

    name: str
    schema: Optional[str] = None
    method: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PostgresColumnDataProfile(ColumnDataProfile):
    """A column profile carrying the extra facts ``pg_stats`` supplies.

    Named fields rather than ``extra`` entries: these are PostgreSQL's own
    statistics, and a consumer that knows it asked PostgreSQL should be able to
    read them off the object instead of guessing at dictionary keys.

    ``correlation`` is the physical/logical ordering agreement the planner
    uses -- 1.0 means a scan reads the table in index order, near 0 means it
    does not. It is not a quality measure and not a hint. ``most_common_vals``
    and ``most_common_freqs`` are the kept value list and its shares, in
    lockstep; both are also what :meth:`sample_column_values` returns, and they
    are kept here because the profile is where a caller looks to decide whether
    asking for them is worth it.

    ``min_value``/``max_value`` are the true extremes: the parser merges the
    histogram's endpoints with the most-common values, since ANALYZE's
    histogram excludes the values it kept as MCVs.

    They arrive as JSON scalars, not as the driver's own objects. ``pg_stats``
    is declared over ``anyarray``, a pseudo-type PostgreSQL refuses to
    subscript or aggregate, so the array is read through ``to_jsonb``: a number
    stays a number, while a date, a timestamp or a UUID reads as its ISO text.
    Everything else on the profile is still native.
    """

    #: Agreement between physical row order and the column's ordering.
    correlation: Optional[float] = None
    #: The most common values ANALYZE kept, in the column's own types.
    most_common_vals: List[Any] = field(default_factory=list)
    #: Their shares of the rows, in lockstep with ``most_common_vals``.
    most_common_freqs: List[float] = field(default_factory=list)
