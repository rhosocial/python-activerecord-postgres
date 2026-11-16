# src/rhosocial/activerecord/backend/impl/postgres/mixins/data_type_suggestions.py
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
    TimestampType,
    UUIDType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresGeometryType,
    PostgresMacAddrType,
    PostgresVarBitType,
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


class PostgresDataTypeSuggestionsMixin:
    """PostgreSQL's answer to "what declaration does a bare field get here"."""

    def suggested_data_types(self) -> Dict[Any, Optional[Any]]:
        return dict(POSTGRES_DATA_TYPE_SUGGESTIONS)


__all__ = ["POSTGRES_DATA_TYPE_SUGGESTIONS", "PostgresDataTypeSuggestionsMixin"]
