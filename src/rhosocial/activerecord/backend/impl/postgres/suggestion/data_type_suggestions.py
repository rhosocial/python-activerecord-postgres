# src/rhosocial/activerecord/backend/impl/postgres/suggestion/data_type_suggestions.py
"""PostgreSQL's **suggested data types**: what a bare field is declared as here.

The DDL half of the type answer, separate from :mod:`.column_type` for the
reason that file already states: a column class says what a value can *do*,
this says what a column *is*.

Three things about this server shape the answers:

* **text is one tier, and it is unbounded.** PostgreSQL's ``TEXT`` has no size
  cap (``TOAST`` moves large values out of the row automatically), so a declared
  capacity is satisfied rather than rounded: ``TextType(length=N)``, ``JsonType``
  and ``XmlType`` all spend the same ``TEXT``/``XML`` word here.
* **a JSON document's natural home here is ``JSONB``.** The column table's own
  comment explains why the operation surface is one class either way; this is
  where the storage choice is made, and it is ``JSONB`` (indexable, and the only
  one of the two with an equality operator).
* **arrays are native.** ``dict``'s container siblings get real arrays, and the
  element width is the shared default: a Python list is ``TEXT[]`` unless the
  field declares otherwise.

Three families carry a parameter a bare annotation does not say: ``Vector``
needs a dimension, and ``Range`` / ``Multirange`` need an element type (the
element decides the spelling, ``int4range`` / ``numrange`` / ``daterange``).
A suggestion may not invent either, so it names the **text carrier** their
neutral adapters already write -- pgvector's ``[1,2,3]`` and the range literal
``[1,2)`` -- and a bare field round-trips. The field that wants the server's
native type declares it with ``UseSqlType``; the alternative, answering
``None``, would take the table away from the caller, which is not what a
suggestion is for.

Each call returns a **fresh dict** whose values are this module's read-only
templates; the derivation binds a dialect on a copy of the single entry it uses.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import uuid
from typing import Any, Dict, Optional

from rhosocial.activerecord.backend.adapters import (
    BitString,
    Geometry,
    MacAddress,
    Multirange,
    Range,
    Vector,
)
from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BlobType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    IntervalType,
    JsonBType,
    TextType,
    TimeType,
    XmlType,
    TimestampType,
    UUIDType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresBoxType,
    PostgresCIDType,
    PostgresCircleType,
    PostgresGeometryType,
    PostgresHstoreType,
    PostgresJsonPathType,
    PostgresLineSegmentType,
    PostgresLineType,
    PostgresMacAddr8Type,
    PostgresMacAddrType,
    PostgresMoneyType,
    PostgresOIDType,
    PostgresPathType,
    PostgresPointType,
    PostgresPolygonType,
    PostgresRegClassType,
    PostgresRegTypeType,
    PostgresTIDType,
    PostgresTSQueryType,
    PostgresTSVectorType,
    PostgresVarBitType,
    PostgresXID8Type,
    PostgresXIDType,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.bit_string import (
    PostgresBitString,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.geometric import (
    Box,
    Circle,
    Line,
    LineSegment,
    Path,
    Point,
    Polygon,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.hstore import (
    PostgresHstore,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.json import (
    PostgresJsonPath,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.monetary import (
    PostgresMoney,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.network_address import (
    PostgresMacaddr,
    PostgresMacaddr8,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.object_identifier import (
    CID,
    OID,
    TID,
    XID,
    XID8,
    RegClass,
    RegCollation,
    RegConfig,
    RegDictionary,
    RegNamespace,
    RegOper,
    RegOperator,
    RegProc,
    RegProcedure,
    RegRole,
    RegType,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.pg_lsn import (
    PostgresLsn,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.pgvector import (
    PostgresVector,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.postgis import (
    PostgresGeometry,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.text_search import (
    PostgresTsQuery,
    PostgresTsVector,
)
from rhosocial.activerecord.backend.impl.postgres.type_values.xml import (
    PostgresXML,
)

#: PostgreSQL's suggested declaration per framework-modelled Python type.
POSTGRES_DATA_TYPE_SUGGESTIONS: Dict[Any, Optional[Any]] = {
    bool: BooleanType(),
    int: IntegerType(),
    float: DoubleType(),
    decimal.Decimal: DecimalType(),
    str: TextType(),
    bytes: BlobType(),
    bytearray: BlobType(),
    datetime.date: DateType(),
    datetime.time: TimeType(),
    datetime.datetime: TimestampType(precision=3),
    datetime.timedelta: IntervalType(),
    uuid.UUID: UUIDType(),
    dict: JsonBType(),
    list: ArrayType(element_type=VarCharType(length=255)),
    tuple: ArrayType(element_type=VarCharType(length=255)),
    set: ArrayType(element_type=VarCharType(length=255)),
    frozenset: ArrayType(element_type=VarCharType(length=255)),
    enum.Enum: VarCharType(length=255),
    # -- Complex families ------------------------------------------------
    # GEOMETRY untyped: point/line/polygon are the *shape* a value carries, and
    # the column decides the shape and SRID it accepts.
    Geometry: PostgresGeometryType(),
    # No dimension to declare here; the neutral adapter's text is the carrier.
    Vector: TextType(),
    BitString: PostgresVarBitType(),
    # The element type is the column's decision; the literal text round-trips.
    Range: TextType(),
    Multirange: TextType(),
    MacAddress: PostgresMacAddrType(),
}


#: Declarations for PostgreSQL's **own** value classes -- the types the
#: framework does not model. A field annotated with one of these is a Python
#: type no portable reader knows, so the backend has to say what it stores.
#: Entries whose native spelling needs a parameter (a vector's dimension, a
#: range's subtype) name the text carrier their adapters write; the rest name
#: the server's own type.
POSTGRES_EXTRA_DATA_TYPE_SUGGESTIONS: Dict[Any, Optional[Any]] = {
    # Bit string, text search, jsonpath and the PostGIS wrapper.
    PostgresBitString: PostgresVarBitType(),
    PostgresGeometry: PostgresGeometryType(),
    PostgresTsVector: PostgresTSVectorType(),
    PostgresTsQuery: PostgresTSQueryType(),
    PostgresJsonPath: PostgresJsonPathType(),
    # The server's own geometric family.
    Point: PostgresPointType(),
    Line: PostgresLineType(),
    LineSegment: PostgresLineSegmentType(),
    Box: PostgresBoxType(),
    Path: PostgresPathType(),
    Polygon: PostgresPolygonType(),
    Circle: PostgresCircleType(),
    # Network, money, hstore, xml and LSN.
    PostgresMacaddr: PostgresMacAddrType(),
    PostgresMacaddr8: PostgresMacAddr8Type(),
    PostgresMoney: PostgresMoneyType(),
    PostgresHstore: PostgresHstoreType(),
    PostgresXML: XmlType(),
    PostgresLsn: TextType(),
    # Object identifiers: the wrapped ones have their own declaration; the
    # registry names are OID aliases and are carried as text.
    OID: PostgresOIDType(),
    RegClass: PostgresRegClassType(),
    RegType: PostgresRegTypeType(),
    RegProc: TextType(),
    RegProcedure: TextType(),
    RegOper: TextType(),
    RegOperator: TextType(),
    RegConfig: TextType(),
    RegDictionary: TextType(),
    RegNamespace: TextType(),
    RegRole: TextType(),
    RegCollation: TextType(),
    XID: PostgresXIDType(),
    XID8: PostgresXID8Type(),
    CID: PostgresCIDType(),
    TID: PostgresTIDType(),
    # Parameter-carrying wrapper: the text its adapter writes.
    PostgresVector: TextType(),
    # `PostgresRange` / `PostgresMultirange` are deliberately absent: their
    # adapters are registered only in the CUSTOM/BOTH range modes, so a default
    # backend has no adapter for those value classes. A suggestion without an
    # adapter builds a column nothing can write.
}


class PostgresDataTypeSuggestionsMixin:
    """PostgreSQL's answer to "what declaration does a bare field get here"."""

    def suggested_data_types(self) -> Dict[Any, Optional[Any]]:
        return dict(POSTGRES_DATA_TYPE_SUGGESTIONS)

    def suggested_extra_data_types(self) -> Dict[Any, Optional[Any]]:
        return dict(POSTGRES_EXTRA_DATA_TYPE_SUGGESTIONS)


__all__ = [
    "POSTGRES_DATA_TYPE_SUGGESTIONS",
    "POSTGRES_EXTRA_DATA_TYPE_SUGGESTIONS",
    "PostgresDataTypeSuggestionsMixin",
]
