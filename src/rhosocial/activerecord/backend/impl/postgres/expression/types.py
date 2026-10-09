# src/rhosocial/activerecord/backend/impl/postgres/expression/types.py
"""PostgreSQL-specific DataType subclasses.

Naming convention
-----------------
PostgreSQL-specific types use the ``Postgres`` prefix to distinguish them
from the core types (which have no prefix).  This avoids ambiguity when both
core and backend types are used together.

Usage scope
-----------
These types are used **only** for PostgreSQL backend DDL column definitions,
introspection result parsing, and schema comparison.  They should **not**
be used by application code directly — always use the core types for
DDL definition expressions (``ColumnDefinition.data_type``).
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    EnumType,
    BigIntType,
    BlobType,
    DataType,
    IntegerType,
    SmallIntType,
    TextType,
    UUIDType,
    XmlType,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ---------------------------------------------------------------------------
# Binary data: BYTEA
# ---------------------------------------------------------------------------

class PostgresByteaType(BlobType):
    """PostgreSQL ``BYTEA`` — variable-length binary string.

    PostgreSQL's own name for the concept core calls ``BlobType``; ``BYTEA`` is
    in that concept's ``SPELLINGS`` list, so the storage is identical and only
    the written form differs.
    """

    name = "postgres_bytea"


# ---------------------------------------------------------------------------
# Serial (auto-increment) types
# ---------------------------------------------------------------------------

class PostgresSmallSerialType(SmallIntType):
    """PostgreSQL ``SMALLSERIAL`` — auto-incrementing SMALLINT (2 bytes).

    ``SMALLSERIAL`` is not a storage type of its own: PostgreSQL stores a
    ``SMALLINT`` and adds a nextval-backed ``DEFAULT`` plus a sequence. The
    auto-increment is a column default, not a property of how the bytes are
    stored, so this derives from :class:`SmallIntType` — the width *is* the
    type — rather than sitting on ``DataType`` where it would claim to be a
    concept no other backend has.

    The name stays ``postgres_smallserial`` because the DDL really does say
    ``SMALLSERIAL``, and that is what a schema diff must compare.
    """

    # A SERIAL column has no signedness. The auto-increment is a column
    # default over a sequence, not a property of how the bytes are stored, and
    # this derives from the integer concept only for its width. Declared empty
    # so it does not inherit ``unsigned`` from that concept -- which PostgreSQL
    # has no way to render anyway, so the field could only ever be a claim the
    # backend cannot honour.
    PARAMETERS = ()

    name = "postgres_smallserial"


class PostgresSerialType(IntegerType):
    """PostgreSQL ``SERIAL`` — auto-incrementing INTEGER (4 bytes).

    The auto-increment is a column default over a sequence, not a distinct
    storage type, so the concept is exactly ``INTEGER``; see
    :class:`PostgresSmallSerialType`.
    """

    # A SERIAL column has no signedness. The auto-increment is a column
    # default over a sequence, not a property of how the bytes are stored, and
    # this derives from the integer concept only for its width. Declared empty
    # so it does not inherit ``unsigned`` from that concept -- which PostgreSQL
    # has no way to render anyway, so the field could only ever be a claim the
    # backend cannot honour.
    PARAMETERS = ()

    name = "postgres_serial"


class PostgresBigSerialType(BigIntType):
    """PostgreSQL ``BIGSERIAL`` — auto-incrementing BIGINT (8 bytes).

    See :class:`PostgresSmallSerialType` for why this is the 8-byte integer
    concept rather than a type of its own.
    """

    # A SERIAL column has no signedness. The auto-increment is a column
    # default over a sequence, not a property of how the bytes are stored, and
    # this derives from the integer concept only for its width. Declared empty
    # so it does not inherit ``unsigned`` from that concept -- which PostgreSQL
    # has no way to render anyway, so the field could only ever be a claim the
    # backend cannot honour.
    PARAMETERS = ()

    name = "postgres_bigserial"


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class PostgresEnumColumnType(EnumType):
    """A PostgreSQL ENUM type used as a column's storage type.

    A PostgreSQL enum is not an inline list of values the way MySQL's is: it is
    a named type created once by ``CREATE TYPE ... AS ENUM`` and then referred
    to by name, which is what lets two columns share one set of labels.

    That difference is about **rendering**, not about what the value *is*. The
    value is an enum either way, so this derives from the core ``EnumType`` and
    ``isinstance(col.data_type, EnumType)`` is true -- which is the question a
    caller asking "is this column an enum?" is actually asking. What PostgreSQL
    writes into the DDL is the *name*; the labels live in the ``CREATE TYPE``
    that made it. Hence ``type_name``, and hence a formatter that emits the
    name rather than an inline ``ENUM('a','b')``.

    The labels are carried as well, so introspection can report them and so
    equality compares what the column can actually hold rather than only a name
    that two different enums could share.

    Creating the type itself is ``EnumTypeManager.create_type`` or
    ``PostgresCreateEnumTypeExpression``; this class is the DataType-shaped door
    into that machinery.

    Args:
        dialect: The dialect that will render the type reference.
        type_name: Name of the created enum type. Required -- a column cannot
            state a PostgreSQL enum without it. That requirement is why the
            dialect's ``suggested_data_types()["enum"]`` points *here* rather
            than claiming the generic ``EnumType`` is renderable.
        schema: Optional schema holding it.
        values: The labels, for introspection and equality.
    """

    name = "postgres_enum"
    type_name: str
    schema: Optional[str]

    def __init__(
        self,
        dialect=None,
        type_name: Optional[str] = None,
        schema: Optional[str] = None,
        values: Optional[list] = None,
    ):
        if not type_name:
            raise ValueError(
                "PostgresEnumColumnType requires a type_name: PostgreSQL refers "
                "to an enum by the name given to CREATE TYPE, so a column "
                "cannot state one without it"
            )
        super().__init__(dialect, values=values)
        self.type_name = type_name
        self.schema = schema

    # Identity is the *named type* plus its labels: two columns pointing at
    # different enums are different columns even if the label lists happen to
    # match, and two columns pointing at the same enum are the same column even
    # if introspection only recovered some of the labels.  ``values`` is already
    # a tuple (EnumType stores it that way), so a plain read is correct.
    PARAMETERS = ("type_name", "schema", "values",)


class PostgresUUIDType(UUIDType):
    """PostgreSQL ``UUID`` — universally unique identifier.

    PostgreSQL is the reference implementation for this concept: 128 random
    bits, stored in the order PostgreSQL documents. Deriving from
    :class:`UUIDType` is what lets code written against the core concept work
    on this backend unchanged, and what makes "is this a UUID column?" one
    question instead of nine.
    """

    name = "postgres_uuid"


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

class PostgresXMLType(XmlType):
    """PostgreSQL ``XML`` — an XML document.

    PostgreSQL's ``xml`` stores a document that may be validated against an XML
    Schema registered for it, and offers XPath over it — which is why it is the
    XML concept and not text that happens to hold tags. The associated schema
    is a column attribute, so it does not enter the type's identity.

    :class:`PostgresCitextType` is the interesting contrast: it *is* text, and
    deriving from :class:`TextType` says so, which this deliberately does not.
    """

    name = "postgres_xml"

# ---------------------------------------------------------------------------
# Text search
# ---------------------------------------------------------------------------

class PostgresTSVectorType(DataType):
    """PostgreSQL ``TSVECTOR`` — a text search document.

    The *result* of analysing a document with a text-search configuration: a
    normalised list of lexemes with positions and weights, which is what
    ``@@`` and the ranking functions match against. A different thing from
    :class:`TextType` — the lexemes are stemmed and indexed, the document text
    is not recoverable from it, and neither comparison nor ordering behaves as
    it does for text.

    On ``DataType`` rather than derived from anything in core because SQL:2016
    has no full-text search type; the ``tsvector`` type is PostgreSQL's own.
    """

    name = "postgres_tsvector"


class PostgresTSQueryType(DataType):
    """PostgreSQL ``TSQUERY`` — a text search query.

    The *question* side of :class:`PostgresTSVectorType`: a parsed, normalised
    expression such as ``'fat' & 'rat'`` that a ``tsvector`` is tested against.
    Kept separate from ``tsvector`` because the two are never the same value
    and never the same column.

    On ``DataType`` for the same reason as ``TSVECTOR`` — SQL:2016 has no
    full-text search types, so there is no core concept to anchor to.
    """

    name = "postgres_tsquery"


# ---------------------------------------------------------------------------
# JSON path
# ---------------------------------------------------------------------------

class PostgresJsonPathType(DataType):
    """PostgreSQL ``JSONPATH`` — an SQL/JSON path expression (PG 12+).

    A compiled path such as ``$.b.bc`` over a :class:`JsonType` or
    :class:`JsonBType` value. Not part of the JSON *storage* type: a column is
    ``jsonb``, and this is the expression evaluated against it. It stays on
    ``DataType`` rather than deriving from ``JsonType`` because it holds no JSON
    document — deriving would make ``isinstance(path, JsonType)`` true for
    something that cannot be stored in a JSON column, which is exactly the kind
    of lie the hierarchy exists to avoid.
    """

    name = "postgres_jsonpath"

# ---------------------------------------------------------------------------
# Bit string types
# ---------------------------------------------------------------------------

class PostgresBitType(DataType):
    """PostgreSQL ``BIT(n)`` — fixed-length bit string of *bits*.

    Not a boolean. ``BIT(8)`` is eight bits, and comparing it to ``true`` or
    indexing it as an array of bytes are both mistakes that only a type name
    invites — hence a class of its own rather than an ``unsigned`` flag on
    :class:`IntegerType`. The same argument keeps it away from
    :class:`BooleanType`: ``BIT(1)`` accepts only ``0`` and ``1``, ``BOOLEAN``
    accepts ``TRUE``/``FALSE``/``yes``/``on``/``t``/``f``, and the two do not
    cast to each other implicitly in PostgreSQL.

    ``n`` is the declared length in bits. PostgreSQL pads to it, so a shorter
    input is not an error — the *value* is length-``n``, whatever was written.
    ``n=None`` means the type was declared without one, which PostgreSQL
    treats as ``BIT(1)``.
On ``DataType``: no core concept covers a fixed-length bit string. It is not an
integer — ``BIT(8)`` is eight bits, and a ``BIT`` column cannot be compared to
``true`` or indexed as bytes — and it is not a boolean, since PostgreSQL does
not cast between ``BIT(1)`` and ``BOOLEAN``. So there is no concept here to
derive from, and it does not derive from the types it must not be confused
with.


    Args:
        dialect: The dialect this type is bound to. First positional parameter,
            as for every ``DataType`` — putting ``n`` first would let a
            dialect passed by mistake be silently stored as the length.
        n: Declared length in bits.
    """

    name = "postgres_bit"
    n: Optional[int] = None

    def __init__(self, dialect: Optional["SQLDialectBase"] = None,
                 n: Optional[int] = None,
                 ):
        super().__init__(dialect)
        self.n = n

    PARAMETERS = ("n",)

class PostgresVarBitType(DataType):
    """PostgreSQL ``VARBIT(n)`` — variable-length bit string of *bits*.

    The same type as :class:`PostgresBitType` with one difference: the value
    keeps the length it was given, so a ``VARBIT(8)`` holding three bits stores
    three bits rather than being padded. That is why it stays a separate class
    instead of a flag on ``PostgresBitType`` — the storage differs, not just the
    declaration — and both stay away from the integer types, since a bit string
    is not a number in any way a reader should assume.

    Args:
        dialect: The dialect this type is bound to. First positional parameter,
            as for every ``DataType``.
        n: Declared maximum length in bits, or ``None`` for no maximum.
    """

    name = "postgres_varbit"
    n: Optional[int] = None

    def __init__(self, dialect: Optional["SQLDialectBase"] = None,
                 n: Optional[int] = None,
                 ):
        super().__init__(dialect)
        self.n = n

    PARAMETERS = ("n",)

# ---------------------------------------------------------------------------
# Network address types
# ---------------------------------------------------------------------------

class PostgresInetType(DataType):
    """PostgreSQL ``INET`` — an IPv4 or IPv6 *host* address.

    Holds an address and optionally the network it belongs to
    (``192.168.1.5/24``), and ``inet`` containment is bit-prefix containment,
    so a bare address is treated as its own ``/32`` or ``/128``. Distinct from
    :class:`PostgresCidrType` in that the host bits are kept.

    On ``DataType`` because SQL:2016 has no network address type. Core's type
    documentation once listed ``InetType`` as a planned concept; it was never
    implemented, and adding it to core is a separate decision (see the
    datatype-hierarchy plan's out-of-scope list) rather than something to settle
    by inheritance here.
    """

    name = "postgres_inet"


class PostgresCidrType(DataType):
    """PostgreSQL ``CIDR`` — an IPv4 or IPv6 *network*.

    The same address-and-prefix as :class:`PostgresInetType`, except the host
    bits are normalised to zero on input, so the value is always a network
    boundary and ``cidr`` arithmetic means the same thing everywhere. That
    difference in stored contents is why these are two types and not one with
    a flag.

    On ``DataType``: SQL:2016 has no network address type.
    """

    name = "postgres_cidr"


class PostgresMacAddrType(DataType):
    """PostgreSQL ``MACADDR`` — a MAC address, EUI-48.

    Six bytes. Distinct from :class:`PostgresMacAddr8Type` because the stored
    length differs, which is what a schema diff has to notice.

    On ``DataType``: SQL:2016 has no MAC address type. See
    :class:`PostgresInetType` on why these are not core concepts yet.
    """

    name = "postgres_macaddr"


class PostgresMacAddr8Type(DataType):
    """PostgreSQL ``MACADDR8`` — a MAC address, EUI-64 (PG 10+).

    Eight bytes, covering the 64-bit space. Separate from ``MACADDR`` for the
    stored length alone.

    On ``DataType``: SQL:2016 has no MAC address type.
    """

    name = "postgres_macaddr8"

# ---------------------------------------------------------------------------
# Geometric types
# ---------------------------------------------------------------------------

class PostgresPointType(DataType):
    """PostgreSQL ``POINT`` — a geometric point ``(x, y)``.

    One of nine geometric primitives in PostgreSQL's built-in ``contrib``-era
    geometric type system (``point``/``line``/``lseg``/``box``/``path``/
    ``polygon``/``circle``). Each is its own type because each carries different
    coordinates and supports different operators; there is no common "geometry"
    value among them.

    On ``DataType`` because SQL:2016 has no geometric types at all. PostGIS's
    :class:`PostgresGeometryType` and :class:`PostgresGeographyType` are a
    different system again (they carry an SRID and a type tag) and are not
    interchangeable with these.

    Nine types across five backends is the largest candidate for a new core
    concept, but adding one is a design decision of its own — see the
    datatype-hierarchy plan's out-of-scope list.
    """

    name = "postgres_point"


class PostgresLineType(DataType):
    """PostgreSQL ``LINE`` — an infinite line ``Ax + By + C = 0``.

    Not a segment: it is unbounded in both directions and has no endpoints, so
    it is neither :class:`PostgresLineSegmentType` nor
    :class:`PostgresPathType`.

    On ``DataType``: SQL:2016 has no geometric types.
    """

    name = "postgres_line"


class PostgresLineSegmentType(DataType):
    """PostgreSQL ``LSEG`` — a line segment between two points.

    Bounded, with two endpoints, which is what separates it from
    :class:`PostgresLineType`.

    On ``DataType``: SQL:2016 has no geometric types.
    """

    name = "postgres_line_segment"


class PostgresBoxType(DataType):
    """PostgreSQL ``BOX`` — an axis-aligned rectangle.

    Stored as two opposite corners, which is why it is not a general
    quadrilateral the way :class:`PostgresPolygonType` is.

    On ``DataType``: SQL:2016 has no geometric types.
    """

    name = "postgres_box"


class PostgresPathType(DataType):
    """PostgreSQL ``PATH`` — an open or closed sequence of points.

    The same structure whether it is open or closed — that flag lives *in* the
    value, not in the type — which is why it is one class rather than two.

    On ``DataType``: SQL:2016 has no geometric types.
    """

    name = "postgres_path"


class PostgresPolygonType(DataType):
    """PostgreSQL ``POLYGON`` — a closed path, with an interior.

    Always closed and always enclosing an area, unlike :class:`PostgresPathType`
    which may be either. The distinction is enforced by the type, not by
    convention.

    On ``DataType``: SQL:2016 has no geometric types.
    """

    name = "postgres_polygon"


class PostgresCircleType(DataType):
    """PostgreSQL ``CIRCLE`` — a circle as centre plus radius.

    On ``DataType``: SQL:2016 has no geometric types.
    """

    name = "postgres_circle"

# ---------------------------------------------------------------------------
# Monetary type
# ---------------------------------------------------------------------------

class PostgresMoneyType(DataType):
    """PostgreSQL ``MONEY`` — a fixed-scale currency amount.

    Deliberately **not** a subclass of :class:`DecimalType`. The two are not
    two spellings of one concept; PostgreSQL's own manual separates them, and
    the differences are not cosmetic:

    ====================  ==========================  ==========================
                         ``MONEY``                   ``NUMERIC``
    ====================  ==========================  ==========================
    Storage               8 bytes, fixed              2 bytes per 4 decimal
                                                    digits, plus 3–8 overhead
    Range                 about 17 integer digits     up to 131072
    Scale                 set by the ``lc_monetary``  declared: ``NUMERIC(p,s)``
                         GUC — **not in the schema**
    Output format         **locale-dependent**       locale-independent
    ``a / b``             ``double precision``        exact ``numeric``
    ====================  ==========================  ==========================

    Two of these are reasons on their own:

    * The **scale is not in the schema**. ``ALTER DATABASE ... SET
      lc_monetary`` silently changes what every ``money`` column stores and
      prints, with nothing in ``information_schema`` recording it. A schema
      snapshot therefore cannot tell you what a ``money`` column holds.
    * The **output is locale-dependent**, which the manual warns about
      explicitly ("the output of this data type is locale-sensitive"). It
      follows that ``schema/differ.py``'s fallback to comparing upper-cased raw
      type strings can report a false difference on a ``money`` column across
      two environments with different ``lc_monetary`` settings — the type name
      is stable, but anything read back out of one is not.

    The **division result type** is the decisive one for this framework: a
    type that changes the result type of an operation on it is not the same
    concept as one that does not. ``money / money`` yields a ``double
    precision``, losing exactness in a way ``numeric / numeric`` does not.

    Upstream agrees: the PostgreSQL Wiki's *Don't Do This: Don't use money*
    says to use ``numeric`` with an explicit scale instead. This class exists
    because ``money`` columns exist — a dump may contain them — not because it
    is a good choice.

    See ``docs/.../datatype-hierarchy.md`` §D6/D7 for why this stays on
    ``DataType`` rather than joining ``DecimalType``.
    """

    name = "postgres_money"

# ---------------------------------------------------------------------------
# Range types
# ---------------------------------------------------------------------------

class PostgresInt4RangeType(DataType):
    """PostgreSQL ``INT4RANGE`` — range of ``integer``.

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.
    """

    name = "postgres_int4range"

class PostgresInt8RangeType(DataType):
    """PostgreSQL ``INT8RANGE`` — range of ``bigint``.

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.
    """

    name = "postgres_int8range"

class PostgresNumRangeType(DataType):
    """PostgreSQL ``NUMRANGE`` — range of ``numeric``.

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.
    """

    name = "postgres_numrange"

class PostgresTsRangeType(DataType):
    """PostgreSQL ``TSRANGE`` — range of ``timestamp`` without time zone.

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.
    """

    name = "postgres_tsrange"

class PostgresTsTzRangeType(DataType):
    """PostgreSQL ``TSTZRANGE`` — range of ``timestamp with time zone``.

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.
    """

    name = "postgres_tstzrange"

class PostgresDateRangeType(DataType):
    """PostgreSQL ``DATERANGE`` — range of ``date``.

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.
    """

    name = "postgres_daterange"

# ---------------------------------------------------------------------------
# Multirange types (PG 14+)
# ---------------------------------------------------------------------------

class PostgresInt4MultirangeType(DataType):
    """PostgreSQL ``INT4MULTIRANGE`` — multirange of ``integer`` (PG 14+).

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.

Separate from the corresponding single range rather than a flag on it: a
multirange holds a *set of non-overlapping ranges*, may be empty, and is
normalised to the coarsest such decomposition. That is a different value, not a
wider version of the same one, so it does not inherit from it. They share no
concept that is not a grouping node, and grouping nodes are not used here —
identity is expressed by inheritance alone.
    """

    name = "postgres_int4multirange"

class PostgresInt8MultirangeType(DataType):
    """PostgreSQL ``INT8MULTIRANGE`` — multirange of ``bigint`` (PG 14+).

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.

Separate from the corresponding single range rather than a flag on it: a
multirange holds a *set of non-overlapping ranges*, may be empty, and is
normalised to the coarsest such decomposition. That is a different value, not a
wider version of the same one, so it does not inherit from it. They share no
concept that is not a grouping node, and grouping nodes are not used here —
identity is expressed by inheritance alone.
    """

    name = "postgres_int8multirange"

class PostgresNumMultirangeType(DataType):
    """PostgreSQL ``NUMMULTIRANGE`` — multirange of ``numeric`` (PG 14+).

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.

Separate from the corresponding single range rather than a flag on it: a
multirange holds a *set of non-overlapping ranges*, may be empty, and is
normalised to the coarsest such decomposition. That is a different value, not a
wider version of the same one, so it does not inherit from it. They share no
concept that is not a grouping node, and grouping nodes are not used here —
identity is expressed by inheritance alone.
    """

    name = "postgres_nummultirange"

class PostgresTsMultirangeType(DataType):
    """PostgreSQL ``TSMULTIRANGE`` — multirange of ``timestamp`` (PG 14+).

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.

Separate from the corresponding single range rather than a flag on it: a
multirange holds a *set of non-overlapping ranges*, may be empty, and is
normalised to the coarsest such decomposition. That is a different value, not a
wider version of the same one, so it does not inherit from it. They share no
concept that is not a grouping node, and grouping nodes are not used here —
identity is expressed by inheritance alone.
    """

    name = "postgres_tsmultirange"

class PostgresTsTzMultirangeType(DataType):
    """PostgreSQL ``TSTZMULTIRANGE`` — multirange of ``timestamp with time zone`` (PG 14+).

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.

Separate from the corresponding single range rather than a flag on it: a
multirange holds a *set of non-overlapping ranges*, may be empty, and is
normalised to the coarsest such decomposition. That is a different value, not a
wider version of the same one, so it does not inherit from it. They share no
concept that is not a grouping node, and grouping nodes are not used here —
identity is expressed by inheritance alone.
    """

    name = "postgres_tstzmultirange"

class PostgresDateMultirangeType(DataType):
    """PostgreSQL ``DATEMULTIRANGE`` — multirange of ``date`` (PG 14+).

On ``DataType``: the core has no interval-of-values concept, and a range is not
a widened scalar. A range carries its own bound semantics — a lower and an
upper bound, ``[`` inclusive and ``(`` exclusive, plus the empty range — and
PostgreSQL *canonicalises* it, so ``numrange '[1,4)'`` comes back as ``[1,3]``.
Deriving from the element type's concept would make ``isinstance(r, IntegerType)``
true of something that cannot be stored in an integer column at all, which is
precisely the lie this hierarchy exists to prevent. Each element type is its own
concept rather than one class parameterised by element, because the
canonicalisation rules differ between continuous and discrete element types —
and that is behaviour, not a field.

Separate from the corresponding single range rather than a flag on it: a
multirange holds a *set of non-overlapping ranges*, may be empty, and is
normalised to the coarsest such decomposition. That is a different value, not a
wider version of the same one, so it does not inherit from it. They share no
concept that is not a grouping node, and grouping nodes are not used here —
identity is expressed by inheritance alone.
    """

    name = "postgres_datemultirange"

# ---------------------------------------------------------------------------
# Object identifier types
# ---------------------------------------------------------------------------

class PostgresOIDType(DataType):
    """PostgreSQL ``OID`` — an unsigned 32-bit object identifier.

    A row identifier into ``pg_catalog``. One of eight system-catalog types
    below, none of which appear in an application schema — they show up when
    querying the catalog itself (``SELECT ... FROM pg_class``) and as
    ``CAST``/``::`` targets.

    On ``DataType``: SQL:2016 has no object identifier type, and ``OID`` is
    specifically PostgreSQL's own. Being an unsigned 32-bit number does *not*
    make it :class:`IntegerType` — the value only means anything in relation to
    a catalog row, which is precisely what makes it a different concept.
    """

    name = "postgres_oid"


class PostgresRegClassType(DataType):
    """PostgreSQL ``REGCLASS`` — a relation, referred to by name.

    Stores an ``oid`` but prints and accepts the relation's name, and resolves
    names as they were at insert time — the behaviour that makes it pleasant to
    use and wrong to treat as an integer.

    On ``DataType``: a system-catalog type, specific to PostgreSQL.
    """

    name = "postgres_regclass"


class PostgresRegTypeType(DataType):
    """PostgreSQL ``REGTYPE`` — a type, referred to by name.

    The type-valued counterpart of :class:`PostgresRegClassType`, and for the
    same reason a separate type rather than a flag on it: what the number means
    depends on the catalog.

    On ``DataType``: a system-catalog type, specific to PostgreSQL.
    """

    name = "postgres_regtype"


class PostgresXIDType(DataType):
    """PostgreSQL ``XID`` — a 32-bit transaction identifier.

    A counter, not a timestamp, and it wraps: 32 bits of transactions, which is
    why the same database has :class:`PostgresXID8Type` alongside it. Not
    usable for ordering over time and not interchangeable with the row
    identifier :class:`PostgresOIDType`.

    On ``DataType``: a system-catalog type, specific to PostgreSQL.
    """

    name = "postgres_xid"


class PostgresXID8Type(DataType):
    """PostgreSQL ``XID8`` — a 64-bit transaction identifier (PG 13+).

    :class:`PostgresXIDType` widened to 64 bits so it does not wrap in
    practice. Separate because the stored width differs — which is exactly what
    a schema diff must see — rather than a flag, since the width is the whole
    difference.

    On ``DataType``: a system-catalog type, specific to PostgreSQL.
    """

    name = "postgres_xid8"


class PostgresCIDType(DataType):
    """PostgreSQL ``CID`` — a command identifier within a transaction.

    Selects among the commands of one statement inside one transaction, so it
    means nothing without that context and is not comparable to
    :class:`PostgresXIDType`, which identifies the transaction.

    On ``DataType``: a system-catalog type, specific to PostgreSQL.
    """

    name = "postgres_cid"


class PostgresTIDType(DataType):
    """PostgreSQL ``TID`` — a tuple identifier as ``(page, tuple)``.

    A physical row location, which changes when the row is rewritten. Unlike
    :class:`PostgresOIDType` it identifies a *version* of a row at a place on
    disk, not an object, and it is not stable across updates.

    On ``DataType``: a system-catalog type, specific to PostgreSQL.
    """

    name = "postgres_tid"


# ---------------------------------------------------------------------------
# pg_lsn type
# ---------------------------------------------------------------------------

class PostgresPgLSNType(DataType):
    """PostgreSQL ``PG_LSN`` — a WAL log sequence number.

    A position in the write-ahead log, printed ``XXXXXXXX/XXXXXXXX`` and
    comparable against the values in ``pg_current_wal_lsn()``. Ordered, and
    monotonically increasing per server — but a *log position*, not a clock
    reading and not an identifier of anything, which is why it does not derive
    from :class:`PostgresOIDType` despite both being 64-bit-ish numbers.

    On ``DataType``: a system-catalog type, specific to PostgreSQL.
    """

    name = "postgres_pg_lsn"

# ---------------------------------------------------------------------------
# Extension-provided types (minimal DataType wrappers)
# ---------------------------------------------------------------------------

class PostgresHstoreType(DataType):
    """PostgreSQL ``HSTORE`` — a key/value store (hstore extension).

    String keys and string values with operators on the pairs —
    ``a->'k'``, ``a ? 'k'``, containment — which is a mapping, not text.
    ``JsonType`` is a mapping too, and they stay apart: hstore values are
    always strings, JSON has a type system, and PostgreSQL will not implicitly
    convert between them. Deriving from ``JsonType`` would claim a column can
    hold both.

    On ``DataType``: SQL:2016 has no key/value type, and hstore is an
    extension type.
    """

    name = "postgres_hstore"


class PostgresGeometryType(DataType):
    """PostGIS ``GEOMETRY`` — a generic spatial geometry (PostGIS extension).

    Unlike the nine built-in primitives (:class:`PostgresPointType` and
    friends), one ``geometry`` column holds *any* of them, tagged internally,
    together with an SRID. Subclass this to fix a specific geometry type and its
    dimensionality for production use.

    Not a superclass of :class:`PostgresPointType` and the rest, and not a
    subclass of them: the built-in primitives carry no SRID and are not
    PostGIS geometries, so neither holds the other. A common ancestor would be
    a "spatial value" family node, which the hierarchy deliberately does not
    have — see the datatype-hierarchy plan, decision D1.

    On ``DataType``: SQL:2016 has no spatial types.
    """

    name = "postgres_geometry"


class PostgresGeographyType(DataType):
    """PostGIS ``GEOGRAPHY`` — a geodetic spatial type (PostGIS extension).

    A ``geometry`` measured on the spheroid: distances and areas come out in
    metres and square metres by default, and the value is stored as
    longitude/latitude. Distinct from :class:`PostgresGeometryType` because the
    measurement semantics differ, not only the storage, so a ``geography``
    distance and a ``geometry`` distance are not the same number.

    On ``DataType``: SQL:2016 has no spatial types.
    """

    name = "postgres_geography"

class PostgresCitextType(TextType):
    """PostgreSQL ``CITEXT`` — case-insensitive text (citext extension).

    The case-insensitivity is behaviour, not storage: ``citext`` stores exactly
    what ``text`` stores and the comparison operators are the difference. It is
    therefore the same *data* concept — character data of unbounded length —
    and derives from :class:`TextType`.

    The distinction from :class:`PostgresXMLType` is the point of this pair:
    both are "some kind of text" to a reader, only one of them is honestly a
    text column, and the hierarchy now says which.
    """

    name = "postgres_citext"


class PostgresCubeType(DataType):
    """PostgreSQL ``CUBE`` — a multi-dimensional cube (cube extension).

    A floating-point n-dimensional point that also supports containment and
    distance, so it is a geometric type rather than a number: two cubes
    compare as equal only when they cover the same region, which is not how
    floats compare.

    On ``DataType``: SQL:2016 has no geometric types, and the ``cube``
    extension's types are its own. See :class:`PostgresPointType`` on why the
    nine geometric primitives are not a core concept yet.
    """

    name = "postgres_cube"


class PostgresLtreeType(DataType):
    """PostgreSQL ``LTREE`` — a hierarchical label path (ltree extension).

    A slash-separated path of labels (``Top.Science.Astronomy``) with
    ancestors, descendants and match operators defined over it. Not text: the
    value is traversed structurally, and ``@>`` tests sub-path containment
    rather than substring containment.

    On ``DataType``: SQL:2016 has no hierarchical label path type; ``ltree`` is
    an extension type with its own operators.
    """

    name = "postgres_ltree"


class PostgresLqueryType(DataType):
    """PostgreSQL ``LQUERY`` — regular-expression match of an ltree label path.

    Named by the extension itself rather than by what it holds: a value of this
    type is a pattern such as ``*.Astronomy.*``, and casting a literal to
    ``LQUERY`` is how the library tells PostgreSQL to check it as one.

    On ``DataType`` for the same reason as :class:`PostgresLtreeType` — and it
    must stay off ``ltree`` specifically, because a ``lquery`` *matches* a path
    rather than being one; deriving from ``LTREE`` would claim it could be
    stored in the same column.
    """

    name = "postgres_lquery"


class PostgresLtxtqueryType(DataType):
    """PostgreSQL ``LTXTQUERY`` — regular-expression match of an ltree label.

    The label counterpart of :class:`PostgresLqueryType`: it matches a single
    label rather than a whole path.

    On ``DataType`` for the same reason, and kept distinct from ``LQUERY``
    because a path pattern and a label pattern are matched by different
    operators against different things.
    """

    name = "postgres_ltxtquery"


class PostgresRasterType(DataType):
    """PostgreSQL ``RASTER`` — a raster grid (PostGIS raster extension).

    Bands of cells over a world coordinate reference system, stored as
    compressed pixel data. A different model from
    :class:`PostgresGeometryType` — which is a single vector of coordinates —
    and from :class:`PostgresCubeType` — which is a continuous region — so it
    derives from none of them.

    On ``DataType``: SQL:2016 has no raster type.
    """

    name = "postgres_raster"


class PostgresVectorType(DataType):
    """pgvector ``VECTOR(n)`` — vector embedding (pgvector extension).
On ``DataType``: the core has no vector-embedding concept, and this is not a
widened scalar either. A ``vector(n)`` column holds *n* ``float4`` components
with a fixed dimension baked into the storage, and the pgvector extension
defines the format; there is nothing in the core for it to be a special case of.
Its dimension is the declaration, so ``dim`` is identity rather than an
``unsigned``-style flag — which is also why it does not derive from the half
precision variant below.


    Args:
        dialect: The dialect this type is bound to. First positional parameter,
            as for every ``DataType`` — with ``dim`` first, a caller following
            the documented convention would have their dialect silently stored
            as the dimension count.
        dim: Number of dimensions. PostgreSQL's own ``vector`` allows 2,000 at
            most; the limit is not enforced here because it is the extension's,
            not the type's.
    """

    name = "postgres_vector"
    dim: int

    def __init__(self, dialect: Optional["SQLDialectBase"] = None, dim: int = 0):
        super().__init__(dialect)
        self.dim = dim

    PARAMETERS = ("dim",)

class PostgresHalfvecType(DataType):
    """pgvector ``HALFVEC(n)`` — half-precision vector (pgvector 0.5.0+).

    Stores each component as a half-precision float, halving memory compared
    to ``VECTOR``. Requires the pgvector extension.

    Separate from :class:`PostgresVectorType` rather than a flag on it: the
    element type is part of the storage format, so the two are different types
    in every way that matters to the server, not different declarations of one.
On ``DataType``: as ``PostgresVectorType`` — no core vector concept — and
separate from it rather than a flag on it, because the element width is part of
the on-disk format. A ``halfvec`` stores each component as a half-precision
float, so the same ``dim`` produces a different number of bytes; the server
stores and compares them differently, which makes them different concepts
rather than two declarations of one.


    Args:
        dialect: The dialect this type is bound to. First positional parameter,
            as for every ``DataType``.
        dim: Number of dimensions.
    """

    name = "postgres_halfvec"
    dim: int

    def __init__(self, dialect: Optional["SQLDialectBase"] = None, dim: int = 0):
        super().__init__(dialect)
        self.dim = dim

    PARAMETERS = ("dim",)

class PostgresSparsevecType(DataType):
    """pgvector ``SPARSEVEC(n)`` — sparse vector (pgvector 0.7.0+).

    Stores only non-zero components as ``{idx:value,...}/dim``, suitable for
    high-dimensional sparse embeddings. Requires the pgvector extension.

    The storage is a list of ``(index, value)`` pairs, which is why this is not
    a flag on :class:`PostgresVectorType`: nothing about the two is shared but
    the purpose.

    Args:
        dialect: The dialect this type is bound to. First positional parameter,
            as for every ``DataType``.
        dim: Number of dimensions — the index space, not the stored count.
    """

    name = "postgres_sparsevec"
    dim: int

    def __init__(self, dialect: Optional["SQLDialectBase"] = None, dim: int = 0):
        super().__init__(dialect)
        self.dim = dim

    PARAMETERS = ("dim",)

# ---------------------------------------------------------------------------
# Array container
# ---------------------------------------------------------------------------

class PostgresArrayType(ArrayType):
    """PostgreSQL array type.

    PostgreSQL **normalises every array declaration to one dimension**: an
    ``integer[3][3]`` column introspects as ``integer[]``, with the extents held
    separately in ``pg_attribute`` and not in the type at all. That is a fact
    about what PostgreSQL stores, and it is why
    :meth:`~...types.array.ArrayType.is_element_type_equivalent` — not ``==`` —
    is the right tool when asking "do these two array columns hold the same kind
    of thing", since ``dimensions`` describes the *declaration* here and is
    discarded on the way in.

    Two consequences worth stating, because they are easy to trip over:

    * ``PostgresArrayType(IntegerType(), dimensions=2)`` renders
      ``INTEGER[][]`` but is **not** equal to ``PostgresArrayType(IntegerType())``,
      and a schema diff against an introspected column will therefore report a
      difference that PostgreSQL itself does not have. Use
      ``dimensions=1`` — the only value that round-trips — when the source is
      a live database.
    * ``PostgresArrayType(IntegerType(), dimensions=2)`` and
      ``PostgresArrayType(PostgresArrayType(IntegerType()))`` render the same
      SQL and compare unequal. Both spell a two-dimensional array; the first is
      the portable spelling and the second is the nesting the standard permits.
      ``dimensions`` is the one that matches what PostgreSQL reports.
    """

    name = "postgres_array"