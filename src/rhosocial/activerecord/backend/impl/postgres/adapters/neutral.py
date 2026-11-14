# src/rhosocial/activerecord/backend/impl/postgres/adapters/neutral.py
"""PostgreSQL value adapters for the core neutral types.

The adapters in this module speak the driver-independent types from
``rhosocial.activerecord.backend.adapters`` only: :class:`Geometry`,
:class:`Vector`, :class:`BitString`, :class:`Range`, :class:`Multirange`, and
:class:`MacAddress`.  They are deliberately separate from the backend-native
adapters already exported by this package (``PostgresPostGISAdapter``,
``PostgresVectorAdapter``, ``PostgresBitStringAdapter``, ``PostgresRangeAdapter``,
``PostgresMacaddrAdapter``), whose Python surface is the PostgreSQL wrapper
types (``PostgresGeometry``, ``PostgresVector`` ...).  Keeping both lets an
existing application keep its wrapper types while a new application can opt
into the neutral ones.

Wire formats:

- geometry / geography: plain WKT, or ``SRID=n;WKT`` (EWKT) when the geometry
  carries an SRID.  Both types accept that text form, so one adapter serves
  both.  EWKB hex output is not decoded: select ``ST_AsEWKT``/``ST_AsText``
  when reading.
- pgvector ``vector``: ``[v1,v2,...]``.
- ``bit`` / ``varbit``: the binary digits accepted by the bit input function.
  ``options={"literal": True}`` produces the SQL literal form ``B'1010'``,
  which is an expression rather than a bind parameter.
- range / multirange: the PostgreSQL range literal produced by
  ``to_postgres_literal()`` (bare numeric bounds, double-quoted text/temporal
  bounds).
- ``macaddr`` / ``macaddr8``: the canonical lowercase colon-separated form.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Type

from rhosocial.activerecord.backend.adapters import (
    BitString,
    BitStringAdapterBase,
    Geometry,
    GeometryAdapterBase,
    MacAddress,
    MacAddressAdapterBase,
    Multirange,
    Range,
    RangeAdapterBase,
    Vector,
    VectorAdapterBase,
)

from ..type_values.bit_string import PostgresBitString
from ..type_values.network_address import PostgresMacaddr, PostgresMacaddr8
from ..type_values.pgvector import PostgresVector
from ..type_values.postgis import PostgresGeometry
from ..type_values.range import PostgresMultirange, PostgresRange

_HEX_TEXT_RE = re.compile(r"^(?:0[xX])?[0-9A-Fa-f]+$")
_INT_TEXT_RE = re.compile(r"^[+-]?\d+$")
_NUMBER_TEXT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_NON_FINITE_NAMES = {
    "nan": float("nan"),
    "infinity": float("inf"),
    "+infinity": float("inf"),
    "inf": float("inf"),
    "-infinity": float("-inf"),
    "-inf": float("-inf"),
}

#: SQL range type names mapped onto the neutral range element labels.
_RANGE_TYPE_LABELS = {
    "int4range": "int",
    "int8range": "int",
    "numrange": "decimal",
    "tsrange": "datetime",
    "tstzrange": "datetime",
    "daterange": "date",
    "timerange": "time",
    "timetzrange": "time",
}
#: Labels the neutral range decoder knows how to turn into Python values.
_RANGE_ELEMENT_LABELS = frozenset(("int", "float", "decimal", "datetime", "date", "time", "str"))
_ELEMENT_TYPE_ALIASES = {
    "integer": "int",
    "bigint": "int",
    "smallint": "int",
    "numeric": "decimal",
    "number": "decimal",
    "double": "float",
    "double precision": "float",
    "real": "float",
    "timestamp": "datetime",
    "timestamptz": "datetime",
    "timestamp with time zone": "datetime",
    "timestamp without time zone": "datetime",
    "date": "date",
    "time": "time",
    "timetz": "time",
    "text": "str",
    "varchar": "str",
    "character varying": "str",
}


# --- Geometry --------------------------------------------------------------


def _looks_like_ewkb_hex(text: str) -> bool:
    """Return whether ``text`` is PostGIS's default hex EWKB output."""
    return bool(_HEX_TEXT_RE.match(text)) and len(text) >= 10


class PostgresNeutralGeometryAdapter(GeometryAdapterBase):
    """Encode neutral geometries as WKT/EWKT for ``geometry``/``geography``.

    The wire format is text because both PostGIS types accept EWKT text and
    psycopg binds text parameters without a driver-specific wrapper.  On read,
    WKT, EWKT and the backend wrapper :class:`PostgresGeometry` are accepted.
    Hex EWKB output is rejected with an explicit message: requesting
    ``ST_AsEWKT`` or ``ST_AsText`` is the caller's responsibility, and silently
    guessing at the binary encoding would be worse than failing.

    ``options``:

    - ``srid``: overrides the geometry's own SRID in the emitted EWKT prefix.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(Geometry, str)

    def _encode(self, geometry: Geometry, target_type: Type, options: Dict[str, Any]) -> str:
        if target_type is not str:
            raise TypeError("PostgreSQL geometry/geography binds as text; target_type must be str")

        srid = options.get("srid", geometry.srid)
        if srid is not None:
            srid = int(srid)
            return "SRID={};{}".format(srid, geometry.to_wkt())
        return geometry.to_wkt()

    def _decode(self, value: Any, options: Dict[str, Any]) -> Geometry:
        if isinstance(value, Geometry) and type(value) is not Geometry:
            return value
        if isinstance(value, PostgresGeometry):
            return Geometry.from_wkt(value.wkt, srid=value.srid)
        if isinstance(value, str):
            text = value.strip()
            if _looks_like_ewkb_hex(text):
                raise ValueError(
                    "PostGIS returned hex EWKB, which this adapter does not decode; "
                    "select ST_AsEWKT or ST_AsText instead"
                )
            return Geometry.from_wkt(text)
        raise TypeError(
            "PostgreSQL geometry/geography must decode from WKT/EWKT text or "
            "PostgresGeometry, got {}".format(type(value).__name__)
        )


# --- Vector ----------------------------------------------------------------


def _format_vector_component(value: Any) -> str:
    """Render one component in the pgvector text form."""
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    return repr(value)


def _parse_vector_text(text: str) -> Vector:
    """Parse the pgvector ``[v1,v2,...]`` text form into a neutral vector."""
    stripped = text.strip()
    if not stripped.startswith("[") or not stripped.endswith("]"):
        raise ValueError("pgvector text must look like [v1,v2,...]")
    inner = stripped[1:-1].strip()
    if not inner:
        raise ValueError("pgvector text must contain at least one component")

    values: List[float] = []
    allow_non_finite = False
    for part in inner.split(","):
        token = part.strip()
        if not token:
            raise ValueError("pgvector text contains an empty component")
        lower = token.lower()
        if lower in _NON_FINITE_NAMES:
            values.append(_NON_FINITE_NAMES[lower])
            allow_non_finite = True
            continue
        try:
            number = float(token)
        except ValueError:
            raise ValueError("invalid pgvector component: {!r}".format(token)) from None
        if not math.isfinite(number):
            allow_non_finite = True
        values.append(number)

    return Vector(tuple(values), allow_non_finite=allow_non_finite)


class PostgresNeutralVectorAdapter(VectorAdapterBase):
    """Encode neutral vectors as pgvector text.

    The text form ``[v1,v2,...]`` is accepted by the ``vector`` type's input
    function, so it is bindable as a plain string parameter.
    ``halfvec``/``sparsevec`` are not served: they have different wire formats
    and are still gaps in the backend's coverage.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(Vector, str)

    def _encode(self, vector: Vector, target_type: Type, options: Optional[Dict[str, Any]]) -> str:
        if target_type is not str:
            raise TypeError("pgvector binds as text; target_type must be str")
        return "[{}]".format(",".join(_format_vector_component(v) for v in vector.values))

    def _decode(self, value: Any, options: Optional[Dict[str, Any]]) -> Vector:
        if isinstance(value, Vector):
            return value
        if isinstance(value, PostgresVector):
            return Vector(tuple(value.values))
        if isinstance(value, str):
            return _parse_vector_text(value)
        if isinstance(value, (list, tuple)):
            return Vector.from_json_value(list(value))
        raise TypeError(
            "pgvector must decode from text, a sequence, or PostgresVector, got "
            "{}".format(type(value).__name__)
        )


# --- Bit string ------------------------------------------------------------


def _declared_bit_length(options: Optional[Dict[str, Any]]) -> Optional[int]:
    """Read the column's declared bit length from the adapter options."""
    if not options:
        return None
    for key in ("bit_length", "length"):
        raw = options.get(key)
        if raw is None:
            continue
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ValueError("{} must be int, got {}".format(key, type(raw).__name__))
        if raw < 1:
            raise ValueError("{} must be at least 1, got {}".format(key, raw))
        return raw
    return None


def _parse_bit_text(text: str) -> str:
    """Return the binary digits of a bit string value."""
    stripped = text.strip()
    if len(stripped) >= 3 and stripped[0] in ("B", "b") and stripped[1] == "'":
        if not stripped.endswith("'"):
            raise ValueError("unterminated B'...' bit string")
        stripped = stripped[2:-1]
    if not stripped:
        raise ValueError("bit string text must not be empty")
    if any(char not in "01" for char in stripped):
        raise ValueError("bit string text may only contain '0' and '1'")
    return stripped


class PostgresNeutralBitStringAdapter(BitStringAdapterBase):
    """Encode neutral bit strings for PostgreSQL ``bit``/``varbit``.

    The default encoding is the bare binary digits (``"1010"``), which the
    bit input function accepts for a text-bound parameter.  A literal
    ``B'1010'`` is SQL syntax, not a bind parameter, so it is only produced
    when ``options={"literal": True}`` is requested explicitly.

    Reading recovers the declared length from ``options["bit_length"]`` (or
    ``options["length"]``) when the column is fixed-width; otherwise the
    length is taken from the returned text.  A shorter text is right-padded
    with zeros to the declared length, matching ``bit(n)`` casting rules.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(BitString, str)

    def _encode(self, bits: BitString, target_type: Type, options: Optional[Dict[str, Any]]) -> str:
        if target_type is not str:
            raise TypeError("PostgreSQL bit/varbit binds as text; target_type must be str")
        text = bits.to_bits()
        if options and options.get("literal"):
            return "B'{}'".format(text)
        return text

    def _decode(self, value: Any, options: Optional[Dict[str, Any]]) -> BitString:
        if isinstance(value, BitString):
            return value
        declared = _declared_bit_length(options)

        if isinstance(value, PostgresBitString):
            text = value.bits
            if declared is None and value.length is not None:
                declared = value.length
        elif isinstance(value, str):
            text = _parse_bit_text(value)
        elif isinstance(value, (bytes, bytearray)):
            raw = bytes(value)
            if not raw:
                raise ValueError("bit string bytes must not be empty")
            return BitString.from_bytes(raw, declared)
        elif isinstance(value, int) and not isinstance(value, bool):
            length = declared
            if length is None:
                length = max(1, value.bit_length())
            return BitString.from_int(value, length)
        else:
            raise TypeError(
                "PostgreSQL bit/varbit must decode from text, bytes, int, or "
                "PostgresBitString, got {}".format(type(value).__name__)
            )

        if declared is not None:
            if len(text) > declared:
                raise ValueError(
                    "bit string text has {} bits, more than the declared {}".format(
                        len(text), declared
                    )
                )
            text = text + "0" * (declared - len(text))
        return BitString.from_bits(text)


# --- Range / multirange ----------------------------------------------------


def _element_label(options: Optional[Dict[str, Any]]) -> Optional[str]:
    """Resolve the neutral range element label from adapter options."""
    if not options:
        return None
    for key in ("element_type", "range_type", "subtype"):
        raw = options.get(key)
        if raw is None:
            continue
        if not isinstance(raw, str):
            raise ValueError("{} must be str, got {}".format(key, type(raw).__name__))
        name = raw.strip().lower()
        if name in _RANGE_TYPE_LABELS:
            return _RANGE_TYPE_LABELS[name]
        label = _ELEMENT_TYPE_ALIASES.get(name, name)
        if label not in _RANGE_ELEMENT_LABELS:
            raise ValueError(
                "unsupported range element type hint {!r}; known labels are {}".format(
                    raw, ", ".join(sorted(_RANGE_ELEMENT_LABELS))
                )
            )
        return label
    return None


def _split_range_parts(text: str, separator: str = ",") -> List[str]:
    """Split a range/multirange body on top-level separators.

    Brackets nest and quoted bounds shield commas, backslashes and brackets.
    PostgreSQL quotes a bound that contains whitespace, a bracket, a comma,
    a double quote or a backslash, and escapes ``"``/``\\`` with a backslash.
    Doubled quotes are also accepted, as some callers spell them that way.
    """
    parts: List[str] = []
    buffer: List[str] = []
    in_quote = False
    depth = 0
    index = 0
    while index < len(text):
        char = text[index]
        if in_quote:
            if char == "\\" and index + 1 < len(text):
                buffer.append(char)
                buffer.append(text[index + 1])
                index += 2
                continue
            if char == '"':
                if index + 1 < len(text) and text[index + 1] == '"':
                    buffer.append('""')
                    index += 2
                    continue
                in_quote = False
            buffer.append(char)
            index += 1
            continue

        if char == '"':
            in_quote = True
            buffer.append(char)
        elif char in "([{":
            depth += 1
            buffer.append(char)
        elif char in ")]}":
            if depth == 0:
                raise ValueError("unbalanced closing bracket in range literal")
            depth -= 1
            buffer.append(char)
        elif char == separator and depth == 0:
            parts.append("".join(buffer))
            buffer = []
        else:
            buffer.append(char)
        index += 1

    if in_quote:
        raise ValueError("unterminated quoted range bound")
    if depth != 0:
        raise ValueError("unbalanced brackets in range literal")
    parts.append("".join(buffer))
    return parts


def _unquote_bound(text: str) -> Tuple[str, bool]:
    """Return the raw bound text and whether it was quoted."""
    text = text.strip()
    if not text.startswith('"'):
        return text, False
    if len(text) < 2 or not text.endswith('"'):
        raise ValueError("unterminated quoted range bound")

    characters: List[str] = []
    index = 1
    end = len(text) - 1
    while index < end:
        char = text[index]
        if char == "\\":
            if index + 1 >= end:
                raise ValueError("dangling escape in quoted range bound")
            characters.append(text[index + 1])
            index += 2
            continue
        if char == '"':
            if index + 1 < end and text[index + 1] == '"':
                characters.append('"')
                index += 2
                continue
            raise ValueError("unescaped quote inside a quoted range bound")
        characters.append(char)
        index += 1
    return "".join(characters), True


def _parse_element(text: str, label: Optional[str]) -> Any:
    """Convert one bound token to its Python value."""
    if label == "datetime":
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            raise ValueError("invalid datetime range bound: {!r}".format(text)) from None
    if label == "date":
        try:
            return date.fromisoformat(text)
        except ValueError:
            raise ValueError("invalid date range bound: {!r}".format(text)) from None
    if label == "time":
        try:
            return time.fromisoformat(text)
        except ValueError:
            raise ValueError("invalid time range bound: {!r}".format(text)) from None
    if label == "int":
        if not _INT_TEXT_RE.match(text):
            raise ValueError("invalid integer range bound: {!r}".format(text))
        return int(text)
    if label == "decimal":
        try:
            return Decimal(text)
        except (InvalidOperation, ValueError):
            raise ValueError("invalid decimal range bound: {!r}".format(text)) from None
    if label == "float":
        try:
            return float(text)
        except ValueError:
            raise ValueError("invalid float range bound: {!r}".format(text)) from None
    if label == "str":
        return text

    # No label: infer the narrowest lossless type from the token.
    lower = text.lower()
    if lower in _NON_FINITE_NAMES:
        return _NON_FINITE_NAMES[lower]
    if _INT_TEXT_RE.match(text):
        return int(text)
    if _NUMBER_TEXT_RE.match(text):
        try:
            return Decimal(text)
        except InvalidOperation:
            return text
    return text


def _parse_range_literal(text: str, options: Optional[Dict[str, Any]]) -> Range:
    """Parse one PostgreSQL range literal into a neutral range."""
    stripped = text.strip()
    if not stripped:
        raise ValueError("range literal is empty")
    if stripped.lower() == "empty":
        return Range(None, None, "[)", True)
    if len(stripped) < 2:
        raise ValueError("range literal is too short: {!r}".format(stripped))

    lower_bracket = stripped[0]
    upper_bracket = stripped[-1]
    if lower_bracket not in "[(":
        raise ValueError("invalid lower bound bracket: {!r}".format(lower_bracket))
    if upper_bracket not in "])":
        raise ValueError("invalid upper bound bracket: {!r}".format(upper_bracket))

    body = stripped[1:-1]
    parts = _split_range_parts(body)
    if len(parts) != 2:
        raise ValueError(
            "range literal must have exactly one bound separator, got {}".format(len(parts))
        )

    label = _element_label(options)
    lower_text, _ = _unquote_bound(parts[0])
    upper_text, _ = _unquote_bound(parts[1])

    lower = _parse_element(lower_text, label) if lower_text else None
    upper = _parse_element(upper_text, label) if upper_text else None

    # PostgreSQL canonical output uses '(' for an infinite bound: normalize
    # so an input such as '[,5]' does not produce a non-canonical neutral value.
    if lower is None:
        lower_bracket = "("
    if upper is None:
        upper_bracket = ")"

    return Range(lower, upper, lower_bracket + upper_bracket, False)


def _parse_multirange_literal(text: str, options: Optional[Dict[str, Any]]) -> Multirange:
    """Parse one PostgreSQL multirange literal into a neutral multirange."""
    stripped = text.strip()
    if not stripped.startswith("{") or not stripped.endswith("}"):
        raise ValueError("multirange literal must be enclosed in braces: {!r}".format(stripped))
    body = stripped[1:-1]
    if not body.strip():
        return Multirange(())
    pieces = [piece for piece in _split_range_parts(body) if piece.strip()]
    return Multirange(tuple(_parse_range_literal(piece, options) for piece in pieces))


def _neutral_range_from_parts(
    lower: Any, upper: Any, lower_inc: bool, upper_inc: bool, empty: bool
) -> Range:
    """Build a neutral range from a backend range's parts."""
    if empty:
        return Range(None, None, "[)", True)
    lower_bracket = "[" if (lower is not None and lower_inc) else "("
    upper_bracket = "]" if (upper is not None and upper_inc) else ")"
    return Range(lower, upper, lower_bracket + upper_bracket, False)


def _is_range_like(value: Any) -> bool:
    """Return whether ``value`` looks like a driver/backend range object."""
    return all(hasattr(value, name) for name in ("lower", "upper", "bounds"))


def _decode_range_like(value: Any) -> Range:
    """Convert a psycopg-like range object into a neutral range."""
    empty = bool(getattr(value, "isempty", False) or getattr(value, "empty", False))
    bounds = getattr(value, "bounds", "[)") or "[)"
    return _neutral_range_from_parts(
        getattr(value, "lower", None),
        getattr(value, "upper", None),
        bounds[0] == "[",
        len(bounds) > 1 and bounds[1] == "]",
        empty,
    )


class PostgresNeutralRangeAdapter(RangeAdapterBase):
    """Encode and decode neutral ``Range``/``Multirange`` values.

    Writes delegate wholly to the neutral literal renderers
    (``to_postgres_literal``), which now produce PostgreSQL's native range
    syntax: numeric bounds are bare and text/temporal bounds are double-quoted
    with ``"``/``\\`` escaped.  Reads accept the PostgreSQL literal text, the
    backend wrappers ``PostgresRange`` / ``PostgresMultirange``, psycopg's
    range objects, and sequences of psycopg-like ranges.

    Text parsing cannot recover an element type by itself.  Unquoted numbers
    become ``int``/``Decimal`` and quoted bounds stay ``str`` unless the caller
    passes ``options={"element_type": "date", ...}`` or
    ``options={"range_type": "daterange"}``.  Without that hint, a quoted
    ``"2024-01-01"`` remains a string; this is a documented limitation of
    parsing PostgreSQL's text output, not a silent conversion.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(Range, str)
        self.register_type(Multirange, str)

    def _encode(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> str:
        if target_type is not str:
            raise TypeError("PostgreSQL range/multirange binds as text; target_type must be str")
        return value.to_postgres_literal()

    def _decode(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]]) -> Any:
        if isinstance(value, (Range, Multirange)):
            return value
        if isinstance(value, PostgresRange):
            return _neutral_range_from_parts(
                value.lower, value.upper, value.lower_inc, value.upper_inc, value.is_empty
            )
        if isinstance(value, PostgresMultirange):
            return Multirange(
                tuple(
                    _neutral_range_from_parts(
                        item.lower,
                        item.upper,
                        item.lower_inc,
                        item.upper_inc,
                        item.is_empty,
                    )
                    for item in value.ranges
                )
            )
        if _is_range_like(value):
            return _decode_range_like(value)
        if isinstance(value, (list, tuple)):
            if not value:
                return Multirange(())
            if all(_is_range_like(item) for item in value):
                return Multirange(tuple(_decode_range_like(item) for item in value))
            raise TypeError("a sequence of ranges must contain range objects")
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("{"):
                return _parse_multirange_literal(text, options)
            return _parse_range_literal(text, options)
        raise TypeError(
            "PostgreSQL range/multirange must decode from text, a backend/driver "
            "range, or a sequence of ranges, got {}".format(type(value).__name__)
        )


# --- MAC address -----------------------------------------------------------


def _canonical_mac(mac: MacAddress) -> str:
    return ":".join("{:02x}".format(octet) for octet in mac.octets)


class PostgresNeutralMacAddressAdapter(MacAddressAdapterBase):
    """Encode neutral MAC addresses as canonical ``macaddr``/``macaddr8`` text.

    PostgreSQL accepts the lowercase colon-separated form for both types.
    ``options={"eui64": True}`` (or ``{"version": 8}``) expands a 6-octet
    address into the EUI-64 form (inserting ``ff:fe`` after the third octet)
    before writing it to a ``macaddr8`` column; without that option the
    canonical 6-octet text is emitted and the server performs its own
    ``macaddr8`` conversion.
    """

    def __init__(self) -> None:
        super().__init__()
        self.register_type(MacAddress, str)

    def _encode(self, mac: MacAddress, target_type: Type, options: Optional[Dict[str, Any]]) -> str:
        if target_type is not str:
            raise TypeError("PostgreSQL macaddr binds as text; target_type must be str")
        options = options or {}
        eui64 = options.get("eui64", False)
        if "version" in options:
            eui64 = eui64 or int(options["version"]) >= 8
        if eui64 and len(mac.octets) == 6:
            octets = mac.octets[:3] + b"\xff\xfe" + mac.octets[3:]
            return ":".join("{:02x}".format(octet) for octet in octets)
        return _canonical_mac(mac)

    def _decode(self, value: Any, options: Optional[Dict[str, Any]]) -> MacAddress:
        if isinstance(value, MacAddress):
            return value
        if isinstance(value, (PostgresMacaddr, PostgresMacaddr8)):
            return MacAddress.parse(str(value))
        if isinstance(value, (str, bytes, bytearray)):
            return MacAddress.parse(value)
        raise TypeError(
            "PostgreSQL macaddr must decode from text, bytes, or a backend MAC "
            "wrapper, got {}".format(type(value).__name__)
        )


__all__ = [
    "PostgresNeutralBitStringAdapter",
    "PostgresNeutralGeometryAdapter",
    "PostgresNeutralMacAddressAdapter",
    "PostgresNeutralRangeAdapter",
    "PostgresNeutralVectorAdapter",
]
