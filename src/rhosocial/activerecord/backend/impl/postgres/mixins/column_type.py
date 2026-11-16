# src/rhosocial/activerecord/backend/impl/postgres/mixins/column_type.py
"""PostgreSQL's answer to "which column class does this Python type mean here".

The column half of the column-type protocol. A column class says what a value
can **do** on this server; how it is **stored** is the ``DataType`` layer's
separate decision and never read from here. Keeping the two apart is why this
table answers ``dict`` with a JSON column class while the DDL side reaches for
``jsonb`` (``.claude/plan/2026-10-08/column-suggestion-protocol.md`` §0.2) --
the two are answers to different questions, and neither infers the other.

Every answer below is written out rather than inherited from a core-side
table, because a backend that owns its answer should be readable without a
second file. The twenty-four entries are the protocol's closed list, all of
them answered: ``datetime.timedelta`` names ``IntervalColumn``, and the six
complex families -- geometry, vector, bit string, range, multirange and MAC
address -- name the core classes, which promise only the surface every column
has. The DDL half of the six families is answered separately, in
``data_type_suggestions.py``: this backend spells ``GEOMETRY`` / ``VARBIT`` /
``MACADDR`` for three of them and refuses the three whose parameters a bare
annotation does not carry. A column class and a storage entry answer
separately, and neither one's answer implies the other's.

**What the answers are measured against.** The live probes in
``.claude/plan/2026-10-08/suggested-mappings.md`` §8 (≈90 server instances
across ten backends), the per-family detail in ``suggested-pairing-array.md``,
``-json``, ``-datetime``, ``-string-enum``, ``-uuid``, ``-numeric``, and this
backend's own ``.claude/plan/2026-10-08/secondary-gaps-investigation.md``
(附录 C mapping, 附录 D gap inventory, 附录 F catalog round-trip over eleven
servers). 附录 F is the discipline this table follows rather than a source of
answers: its single finding F1 (``bit varying(n)`` parsed as ``PostgresBitType``
instead of ``PostgresVarBitType``) was invisible to render/parse round-trips
and only appeared once the **server's own catalog words** were fed in -- four of
thirty-six words were wrong, and the declared spelling was wrong the same way,
so both sides of the comparison agreed with each other and both were wrong. Fixed
in ``04ec5e5``. An answer written from documentation rather than from a server
is exactly the shape of that bug, which is why the array and JSON rows below
quote the probe tables instead of the manual.

**The two numeric deviations are gone, and the reason is a protocol fact.**
This table used to answer ``float`` with ``FloatColumn`` and ``decimal.Decimal``
with ``DecimalColumn`` where the framework answered the generic
``NumericColumn``. The rebuild removed those two classes -- the numeric family
is deliberately one class (``NumericColumn``), because the operations are the
same whatever the value was declared as, and the difference between
``numeric(18,4)`` and ``double precision`` belongs to the DDL layer's
``DataType``. So the deviation closed for both entries without an edit here:
they now answer ``NumericColumn``, which is what
``.claude/plan/2026-10-08/secondary-gaps-investigation.md`` 附录 C called the
"偏泛" answer, and what the family has to say now that there is only one class
to say it with.
"""

import datetime
import decimal
import enum
import uuid
from typing import Any, Dict, Optional, Type

from rhosocial.activerecord.backend.adapters import (
    BitString,
    Geometry,
    MacAddress,
    Multirange,
    Range,
    Vector,
)

from rhosocial.activerecord.backend.dialect.mixins import ColumnTypeMixin
from rhosocial.activerecord.backend.expression.column_types import (
    ArrayColumn,
    BinaryColumn,
    BitStringColumn,
    BooleanColumn,
    ColumnBase,
    GeometryColumn,
    IntegerColumn,
    IntervalColumn,
    JSONColumn,
    MacAddressColumn,
    MultirangeColumn,
    NumericColumn,
    RangeColumn,
    StringColumn,
    TimestampColumn,
    UUIDColumn,
    VectorColumn,
)

#: All twenty-four protocol entries, in the protocol's reading order. Commented
#: per group because the reasons are the content: the answer is a claim about
#: this server, and a claim without its evidence is decoration.
POSTGRES_COLUMN_TYPES: Dict[Any, Optional[Type[ColumnBase]]] = {
    # -- Scalars ---------------------------------------------------------
    # Native `boolean`, and the three-valued logic that comes with it, so
    # `is_true()` / `is_false()` render as `IS TRUE` / `IS FALSE` rather
    # than `= 1`. That spelling is not a stylistic choice: PostgreSQL rejects
    # `boolean = integer` outright (suggested-mappings §8.1, `bool` row),
    # which is why `BooleanColumn` exists rather than a numeric column.
    bool: BooleanColumn,
    int: IntegerColumn,
    # `NumericColumn`, as for the entries above and below it. PostgreSQL really
    # does have distinct `numeric` / `real` / `double precision` families
    # (suggested-mappings §8.1, `float` row), but that is a *storage* fact the
    # `DataType` layer spells; the operation surface is the numeric one, and it
    # is one class since the rebuild removed `FloatColumn` / `RealColumn` /
    # `DoubleColumn`.
    float: NumericColumn,
    decimal.Decimal: NumericColumn,
    str: StringColumn,
    # `bytea`. Binary comparison, length, substring and hex are all
    # available (suggested-mappings §8.1, `bytes` row, where PostgreSQL is
    # in the unqualified set).
    bytes: BinaryColumn,
    bytearray: BinaryColumn,
    # -- Temporal --------------------------------------------------------
    # `date` / `time` / `datetime` are three entries and one answer, and the
    # answer is the baseline all ten backends agree on. A tz-aware `datetime`
    # lands on the same class and is where this backend is strongest:
    # `AT TIME ZONE` is native and named zones work (only the *read-back*
    # follows the session time zone -- §8.1 `datetime tz` row).
    datetime.date: TimestampColumn,
    datetime.time: TimestampColumn,
    datetime.datetime: TimestampColumn,
    # PostgreSQL has a real `interval` type -- §1 spells it `INTERVAL` where
    # eight backends have no answer at all -- and it is not a number:
    # `justify_hours`, `EXTRACT(... FROM interval)` and the field qualifiers
    # operate on it, while `sqrt(interval '1 day')` is refused outright. So
    # `IntervalColumn` names the value and offers no arithmetic; the numeric
    # operations stay with numbers. The storage is spelled by the `DataType`
    # layer (`format_data_type_interval`).
    #
    # The backend's own interval operations (`justify_hours`, field extraction)
    # belong to a PostgreSQL-named subclass; the core class states only the
    # surface every column has.
    datetime.timedelta: IntervalColumn,
    # -- Value families --------------------------------------------------
    uuid.UUID: UUIDColumn,
    # `dict` → the JSON column class, on both `json` and `jsonb`. §12's
    # PostgreSQL row adds `jsonb` because `json` has **no equality operator,
    # no `?` and no `@>`** (protocol §0.2; §8.1 `dict` row) -- but that is a
    # DDL-layer choice made at the `DataType` layer (`JsonBType` /
    # `format_data_type_jsonb` on this backend), not something a column class
    # can express. The column class is the operation surface, and it is the
    # same either way: `json_path` / `json_value`, has-key, array length,
    # validity, and on `jsonb` the containment operators on top
    # (`PostgresJSONBEnhancedMixin`).
    dict: JSONColumn,
    # Native arrays -- the strongest `ArrayColumn` backend of the ten, and the
    # one place where this table's answer is a real *difference* rather than
    # a shared baseline. Seven backends route `list` through a JSON document
    # (§12); `suggested-pairing-array.md` §A measures PostgreSQL 9.6 / 12 /
    # 18 with every row of the guarantee set green: `INT[]` / `TEXT[]` DDL,
    # a Python list bound directly and read back as a list, `array_length`,
    # 1-based `x[1]`, containment `x @> ARRAY[v]`, concatenation `x || v`,
    # the quantified form `v = ANY(x)`, whole-column equality, and `unnest`
    # -- against a seven-backend field where the same ops are variously
    # absent, silently wrong, or need emulation. 附录 C calls this the
    # contrast case: same Python type, different storage, different answer.
    #
    # `set` / `frozenset` / `tuple` answer `ArrayColumn` too, for the same
    # reason the baseline puts them together: they are sequences to the
    # server, and order is the one thing a set does not promise.
    list: ArrayColumn,
    tuple: ArrayColumn,
    set: ArrayColumn,
    frozenset: ArrayColumn,
    # `enum.Enum` → `StringColumn`, which is the baseline and is also what
    # the value semantics say: the labels compare as strings, so equality,
    # `IN` and `ORDER BY` render as they do for text (§8.1 `enum` row).
    #
    # PostgreSQL does have enums, but as a *named* type created once by
    # `CREATE TYPE ... AS ENUM` and referred to by name -- which is a DDL
    # fact, carried by `PostgresEnumColumnType` on the `DataType` side
    # (secondary-gaps §2 item 5). Spelling the label set into the column
    # class would put storage into the operation layer, which is the split
    # this protocol exists to keep.
    enum.Enum: StringColumn,
    # -- Complex families ------------------------------------------------
    # The six families now have core column classes. Those classes promise
    # only the surface every column has -- alias, null tests, casting and
    # collation -- because no distance, containment, overlap, bitwise or
    # similarity operation is spelled the same way on every backend. The
    # PostgreSQL operations live on backend-named subclasses (PostGIS is an
    # extension, not the core server), which is where the richer spelling
    # belongs. The storage stays answered in `data_type_suggestions.py`.
    Geometry: GeometryColumn,
    Vector: VectorColumn,
    BitString: BitStringColumn,
    Range: RangeColumn,
    Multirange: MultirangeColumn,
    MacAddress: MacAddressColumn,
}


class PostgresColumnTypeMixin(ColumnTypeMixin):
    """The full framework-modelled-Python-type → column-class table for PostgreSQL.

    Composed into :class:`~rhosocial.activerecord.backend.impl.postgres.dialect.PostgresDialect`
    ahead of this backend's own mixins. Nothing else in the list can answer for
    a common Python type, so the MRO leaves the placement free.

    **Every entry is measured, and two of them moved with the rebuild.**
    ``float`` and ``decimal.Decimal`` now answer ``NumericColumn`` like every
    other backend: core had a ``FloatColumn`` and a ``DecimalColumn`` when this
    table was written, and the entry-by-entry measurements were right against
    them; the numeric family is now deliberately one class, so the two answers
    became the generic one. The change is in the class name, not in the
    operations, and it is pinned by the backend's own test file rather than
    left to drift.

    **No entry is refused.** ``datetime.timedelta`` used to answer ``None``
    because PostgreSQL's native ``interval`` had no core column class; core
    now has ``IntervalColumn``, so the value is named and the arithmetic it
    does not promise stays where it belongs. The six complex families used to
    answer ``None`` for the mirror-image reason -- no core class existed --
    and now name their core classes, which promise only the surface every
    column has. Their storage is answered here too -- ``GEOMETRY``,
    ``VARBIT`` and ``MACADDR`` -- so a bare model builds the table and a
    field resolves.

    **A backend-named class is where richer operations go.** PostGIS
    functions, range ``@>`` operators and `justify_hours` are each this
    server's own; they belong to a subclass such as
    ``PostgresGeometryColumn(GeometryColumn)`` in the extra table, not to
    the core class that every backend derives from.

    The table has no version branch because nothing in it moves with the
    version: the one thing that does (``jsonb`` becoming available in 9.4)
    changes the DDL spelling, not which column class a ``dict`` is.
    """

    def suggested_column_types(self) -> Dict[Any, Optional[Type[ColumnBase]]]:
        return dict(POSTGRES_COLUMN_TYPES)


__all__ = ["POSTGRES_COLUMN_TYPES", "PostgresColumnTypeMixin"]
