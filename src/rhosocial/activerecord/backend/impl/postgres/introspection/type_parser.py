# src/rhosocial/activerecord/backend/impl/postgres/introspection/type_parser.py
"""PostgreSQL's type-spelling parser (read direction, introspection-only).

Backend-local helper: parsing a server-reported type spelling back into a
``DataType`` is not an expression-construction concern and is no longer a
dialect-protocol member (the dialect's ``parse_type`` was removed when core
delegated ``DataType.parse_data_type_str`` to ``CustomType``). It lives under
PostgreSQL introspection and is used only when reading back a column's declared
type; it is NOT a dialect protocol member.
"""
from typing import Optional

from ....expression.types import (
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    CustomType,
    DataType,
    DateType,
    DateTimeType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntervalType,
    JsonBType,
    JsonType,
    RealType,
    SmallIntType,
    TextType,
    TimeType,
    TimeTzType,
    TimestampType,
    TimestampTzType,
    UUIDType,
    VarCharType,
    XmlType,
)
from .._scan import digit_runs, read_digits, starts_with_word
from ..expression.types import PostgresArrayType


def _first_integer_in_parentheses(text: str) -> Optional[int]:
    """Read the number of the first ``(digits)`` group in ``text``."""
    index = 0
    while True:
        index = text.find("(", index)
        if index < 0:
            return None
        digits, end = read_digits(text, index + 1)
        if digits and end < len(text) and text[end] == ")":
            return int(digits)
        index += 1


def _interval_fields(upper: str) -> Optional[str]:
    """Return the fields spelling that follows ``INTERVAL``, if any.

    The fields are the rest of the line after the whitespace that follows
    the keyword, upper-cased and stripped, which is what
    ``INTERVAL\\s+(.*)`` read.
    """
    marker = "INTERVAL"
    start = upper.find(marker)
    if start < 0:
        return None
    index = start + len(marker)
    if index >= len(upper) or not upper[index].isspace():
        return None
    while index < len(upper) and upper[index].isspace():
        index += 1
    line_end = upper.find("\n", index)
    fields = upper[index:] if line_end < 0 else upper[index:line_end]
    return fields.strip()


def _strip_array_bracket_suffix(text: str) -> Optional[str]:
    """Drop one trailing ``[digits]`` group from ``text``."""
    open_index = text.rfind("[", 0, len(text) - 1)
    if open_index < 0:
        return None
    inner = text[open_index + 1 : -1]
    if not all(char.isdecimal() for char in inner):
        return None
    return text[:open_index]


def _strip_array_keyword_suffix(upper: str) -> Optional[str]:
    """Drop a trailing `` ARRAY`` spelling, with its optional ``[count]``.

    The spelling is ``\\s+ARRAY(?:\\s*\\[\\s*\\d+\\s*\\])?\\s*$``: at least one
    whitespace character before ``ARRAY``, and a bracketed count that is
    optional but, when present, needs at least one digit.
    Returns the base spelling, or ``None`` when the suffix is not there.
    """
    core = upper[: len(upper.rstrip())]
    if core.endswith("]"):
        open_index = core.rfind("[", 0, len(core) - 1)
        if open_index < 0:
            return None
        inner = core[open_index + 1 : -1]
        start = 0
        while start < len(inner) and inner[start].isspace():
            start += 1
        digits, digits_end = read_digits(inner, start)
        if not digits or inner[digits_end:].strip():
            return None
        core = core[:open_index]
    trimmed = core.rstrip()
    if not trimmed.endswith("ARRAY"):
        return None
    index = len(trimmed) - len("ARRAY")
    if index == 0 or not trimmed[index - 1].isspace():
        return None
    while index > 0 and trimmed[index - 1].isspace():
        index -= 1
    return upper[:index]


def _starts_with_bit_varying(upper: str) -> bool:
    """Return whether ``upper`` begins with ``BIT`` then ``VARYING``."""
    if starts_with_word(upper, ("BIT",)) is None:
        return False
    index = len("BIT")
    if index >= len(upper) or not upper[index].isspace():
        return False
    while index < len(upper) and upper[index].isspace():
        index += 1
    return starts_with_word(upper[index:], ("VARYING",)) is not None


class PostgresTypeParser:
    """Parse a raw PostgreSQL type spelling into a ``DataType``, bound to a dialect."""

    _PG_INTEGER_WORDS = (
        "SMALLINT",
        "INT2",
        "INT",
        "INTEGER",
        "INT4",
        "BIGINT",
        "INT8",
    )
    _PG_SERIAL_WORDS = (
        "SMALLSERIAL",
        "SERIAL2",
        "SERIAL",
        "SERIAL4",
        "BIGSERIAL",
        "SERIAL8",
    )
    _PG_FLOAT_WORDS = (
        "REAL",
        "FLOAT4",
        "FLOAT",
        "DOUBLE",
    )
    _PG_DECIMAL_WORDS = (
        "DECIMAL",
        "NUMERIC",
    )
    _PG_STRING_WORDS = (
        "CHARACTER",
        "CHAR",
        "VARCHAR",
        "TEXT",
        "CITEXT",
    )
    _PG_BINARY_WORDS = (
        "BYTEA",
        "BLOB",
    )
    _PG_DATE_WORDS = (
        "DATETIME",
        "DATE",
        "TIMESTAMPTZ",
        "TIMESTAMP",
        "TIMETZ",
        "TIME",
        "INTERVAL",
    )
    _PG_JSON_WORDS = (
        "JSON",
        "JSONB",
        "JSONPATH",
    )
    _PG_UUID_WORDS = ("UUID",)
    _PG_NET_WORDS = (
        "INET",
        "CIDR",
        "MACADDR",
        "MACADDR8",
    )
    _PG_GEOM_WORDS = (
        "POINT",
        "LINE",
        "LSEG",
        "BOX",
        "PATH",
        "POLYGON",
        "CIRCLE",
    )
    _PG_BIT_WORDS = (
        "BIT",
        "VARBIT",
    )
    _PG_RANGE_WORDS = (
        "INT4RANGE",
        "INT8RANGE",
        "NUMRANGE",
        "TSRANGE",
        "TSTZRANGE",
        "DATERANGE",
    )
    _PG_MULTIRANGE_WORDS = (
        "INT4MULTIRANGE",
        "INT8MULTIRANGE",
        "NUMMULTIRANGE",
        "TSMULTIRANGE",
        "TSTZMULTIRANGE",
        "DATEMULTIRANGE",
    )
    _PG_OID_WORDS = (
        "OID",
        "REGCLASS",
        "REGTYPE",
        "XID",
        "XID8",
        "CID",
        "TID",
    )
    _PG_TS_WORDS = (
        "TSVECTOR",
        "TSQUERY",
    )
    _PG_MISC_WORDS = (
        "MONEY",
        "XML",
        "PG_LSN",
        "HSTORE",
        "GEOMETRY",
        "GEOGRAPHY",
        "CUBE",
        "LTREE",
        "LQUERY",
        "LTXTQUERY",
        "RASTER",
    )
    _PG_VECTOR_WORDS = (
        "VECTOR",
        "HALFVEC",
        "SPARSEVEC",
    )

    def __init__(self, dialect):
        self.dialect = dialect

    def parse(self, raw: str) -> DataType:
        dialect = self.dialect
        stripped = raw.strip()
        upper = stripped.upper()

        # ---- Array detection (must run before any type-specific match) ----

        # Check for ARRAY keyword: e.g. "INTEGER ARRAY" or "INTEGER ARRAY[3]"
        base_raw = _strip_array_keyword_suffix(upper)
        if base_raw is not None:
            element_type = self.parse(stripped[: len(base_raw)])
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type=element_type, dimensions=1, dialect=dialect)

        # Check for array bracket suffix: e.g. "INTEGER[]", "INTEGER[][]"
        remaining = stripped
        while remaining.endswith("[]"):
            remaining = remaining[:-2]
        while remaining.endswith("]"):
            bracket_base = _strip_array_bracket_suffix(remaining)
            if bracket_base is None:
                break
            remaining = bracket_base

        if remaining != stripped:
            element_type = self.parse(remaining)
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type=element_type, dimensions=1, dialect=dialect)

        # ---- Standard type dispatch ----

        # Serial family
        if starts_with_word(upper, self._PG_SERIAL_WORDS) is not None:
            from ..expression.types import (
                PostgresBigSerialType,
                PostgresSerialType,
                PostgresSmallSerialType,
            )
            if upper.startswith("BIGSERIAL") or upper.startswith("SERIAL8"):
                return PostgresBigSerialType(dialect)
            if upper.startswith("SMALLSERIAL") or upper.startswith("SERIAL2"):
                return PostgresSmallSerialType(dialect)
            return PostgresSerialType(dialect)

        # Integer family
        if starts_with_word(upper, self._PG_INTEGER_WORDS) is not None:
            if upper.startswith("SMALLINT") or upper.startswith("INT2"):
                return SmallIntType(dialect)
            if upper.startswith("BIGINT") or upper.startswith("INT8"):
                return BigIntType(dialect)
            return IntegerType(dialect)

        # Float family
        if starts_with_word(upper, self._PG_FLOAT_WORDS) is not None:
            if upper.startswith("DOUBLE"):
                return DoubleType(dialect=dialect)
            if upper.startswith("REAL") or upper.startswith("FLOAT4"):
                return RealType(dialect=dialect)
            nums = digit_runs(stripped)
            precision = int(nums[0]) if nums else None
            return FloatType(dialect=dialect, precision=precision)

        # Decimal family
        if starts_with_word(upper, self._PG_DECIMAL_WORDS) is not None:
            nums = digit_runs(stripped)
            if len(nums) >= 2:
                return DecimalType(dialect=dialect, precision=int(nums[0]), scale=int(nums[1]))
            if len(nums) == 1:
                return DecimalType(dialect=dialect, precision=int(nums[0]))
            return DecimalType(dialect=dialect)

        # String family
        if starts_with_word(upper, self._PG_STRING_WORDS) is not None:
            length = _first_integer_in_parentheses(stripped)
            if upper.startswith("CHARACTER VARYING"):
                return VarCharType(dialect=dialect, length=length,
                                   spelling="character varying")
            if upper.startswith("VARCHAR"):
                return VarCharType(dialect=dialect, length=length)
            if upper.startswith("CHARACTER"):
                return CharType(dialect=dialect, length=length,
                                spelling="character")
            if upper.startswith("CHAR"):
                return CharType(dialect=dialect, length=length)
            # citext: case-insensitive text, a contrib extension with its own
            # type. A TextType subclass rather than a spelling, because the
            # comparison semantics differ -- a citext column compares equal
            # case-insensitively, which text does not, so the differ must see
            # the difference.
            if upper.startswith("CITEXT"):
                from ..expression.types import PostgresCitextType
                return PostgresCitextType(dialect)
            return TextType(dialect=dialect)

        # Binary
        if starts_with_word(upper, self._PG_BINARY_WORDS) is not None:
            return BlobType(dialect=dialect)

        # Date/time
        if starts_with_word(upper, self._PG_DATE_WORDS) is not None:
            if upper.startswith("DATETIME"):
                return DateTimeType(dialect=dialect)
            if upper.startswith("DATE"):
                return DateType(dialect=dialect)
            if upper.startswith("TIMESTAMP"):
                nums = digit_runs(stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMESTAMPTZ"):
                    return TimestampTzType(dialect=dialect, precision=precision)
                return TimestampType(dialect=dialect, precision=precision)
            if upper.startswith("TIME"):
                nums = digit_runs(stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMETZ"):
                    return TimeTzType(dialect=dialect, precision=precision)
                return TimeType(dialect=dialect, precision=precision)
            if upper.startswith("INTERVAL"):
                fields = _interval_fields(upper)
                return IntervalType(dialect=dialect, fields=fields)

        # JSON
        if starts_with_word(upper, self._PG_JSON_WORDS) is not None:
            if upper.startswith("JSONB"):
                return JsonBType(dialect=dialect)
            if upper.startswith("JSONPATH"):
                from ..expression.types import PostgresJsonPathType
                return PostgresJsonPathType(dialect=dialect)
            return JsonType(dialect=dialect)

        # UUID
        if starts_with_word(upper, self._PG_UUID_WORDS) is not None:
            return UUIDType(dialect=dialect)

        # Boolean
        if upper.startswith("BOOLEAN") or upper.startswith("BOOL"):
            return BooleanType(dialect=dialect)

        # Bit string
        if starts_with_word(upper, self._PG_BIT_WORDS) is not None:
            nums = digit_runs(stripped)
            n = int(nums[0]) if nums else None
            # The variable-length family is tested first: live ``format_type``
            # emits ``bit varying(n)`` for a varbit column, and that string
            # starts with ``BIT`` exactly as the fixed-length word does.  A
            # leading ``startswith("BIT")`` test sent the catalog spelling to
            # ``PostgresBitType``; ``VARBIT`` and ``BIT VARYING`` are the two
            # words for the one storage and must answer the same class.
            if upper.startswith("VARBIT") or _starts_with_bit_varying(upper):
                from ..expression.types import PostgresVarBitType
                return PostgresVarBitType(n=n, dialect=dialect)
            from ..expression.types import PostgresBitType
            return PostgresBitType(n=n, dialect=dialect)

        # Network address
        if starts_with_word(upper, self._PG_NET_WORDS) is not None:
            from ..expression.types import (
                PostgresCidrType,
                PostgresInetType,
                PostgresMacAddr8Type,
                PostgresMacAddrType,
            )
            if upper.startswith("CIDR"):
                return PostgresCidrType(dialect)
            if upper.startswith("MACADDR8"):
                return PostgresMacAddr8Type(dialect)
            if upper.startswith("MACADDR"):
                return PostgresMacAddrType(dialect)
            return PostgresInetType(dialect)

        # Geometric
        if starts_with_word(upper, self._PG_GEOM_WORDS) is not None:
            from ..expression.types import (
                PostgresBoxType,
                PostgresCircleType,
                PostgresLineSegmentType,
                PostgresLineType,
                PostgresPathType,
                PostgresPointType,
                PostgresPolygonType,
            )
            geom_map = {
                "POINT": PostgresPointType,
                "LINE": PostgresLineType,
                "LSEG": PostgresLineSegmentType,
                "BOX": PostgresBoxType,
                "PATH": PostgresPathType,
                "POLYGON": PostgresPolygonType,
                "CIRCLE": PostgresCircleType,
            }
            for name, cls in geom_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresPointType(dialect)

        # Range types
        if starts_with_word(upper, self._PG_RANGE_WORDS) is not None:
            from ..expression.types import (
                PostgresDateRangeType,
                PostgresInt4RangeType,
                PostgresInt8RangeType,
                PostgresNumRangeType,
                PostgresTsRangeType,
                PostgresTsTzRangeType,
            )
            range_map = {
                "INT4RANGE": PostgresInt4RangeType,
                "INT8RANGE": PostgresInt8RangeType,
                "NUMRANGE": PostgresNumRangeType,
                "TSRANGE": PostgresTsRangeType,
                "TSTZRANGE": PostgresTsTzRangeType,
                "DATERANGE": PostgresDateRangeType,
            }
            for name, cls in range_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresInt4RangeType(dialect)

        # Multirange types
        if starts_with_word(upper, self._PG_MULTIRANGE_WORDS) is not None:
            from ..expression.types import (
                PostgresDateMultirangeType,
                PostgresInt4MultirangeType,
                PostgresInt8MultirangeType,
                PostgresNumMultirangeType,
                PostgresTsMultirangeType,
                PostgresTsTzMultirangeType,
            )
            mr_map = {
                "INT4MULTIRANGE": PostgresInt4MultirangeType,
                "INT8MULTIRANGE": PostgresInt8MultirangeType,
                "NUMMULTIRANGE": PostgresNumMultirangeType,
                "TSMULTIRANGE": PostgresTsMultirangeType,
                "TSTZMULTIRANGE": PostgresTsTzMultirangeType,
                "DATEMULTIRANGE": PostgresDateMultirangeType,
            }
            for name, cls in mr_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresInt4MultirangeType(dialect)

        # OID types
        if starts_with_word(upper, self._PG_OID_WORDS) is not None:
            from ..expression.types import (
                PostgresCIDType,
                PostgresOIDType,
                PostgresRegClassType,
                PostgresRegTypeType,
                PostgresTIDType,
                PostgresXID8Type,
                PostgresXIDType,
            )
            oid_map = {
                "OID": PostgresOIDType,
                "REGCLASS": PostgresRegClassType,
                "REGTYPE": PostgresRegTypeType,
                "XID8": PostgresXID8Type,
                "XID": PostgresXIDType,
                "CID": PostgresCIDType,
                "TID": PostgresTIDType,
            }
            for name, cls in oid_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresOIDType(dialect)

        # Text search
        if starts_with_word(upper, self._PG_TS_WORDS) is not None:
            from ..expression.types import (
                PostgresTSQueryType,
                PostgresTSVectorType,
            )
            if upper.startswith("TSVECTOR"):
                return PostgresTSVectorType(dialect)
            return PostgresTSQueryType(dialect)

        # Miscellaneous
        if starts_with_word(upper, self._PG_MISC_WORDS) is not None:
            from ..expression.types import (
                PostgresGeographyType,
                PostgresGeometryType,
                PostgresHstoreType,
                PostgresMoneyType,
                PostgresPgLSNType,
            )
            if upper.startswith("MONEY"):
                return PostgresMoneyType(dialect)
            if upper.startswith("XML"):
                return XmlType(dialect=dialect)
            if upper.startswith("PG_LSN"):
                return PostgresPgLSNType(dialect)
            if upper.startswith("HSTORE"):
                return PostgresHstoreType(dialect)
            if upper.startswith("GEOGRAPHY"):
                return PostgresGeographyType(dialect)
            if upper.startswith("GEOMETRY"):
                return PostgresGeometryType(dialect)
            # Extension types with one word each: cube and the ltree family
            # are contrib extensions, RASTER is PostGIS -- the same home as
            # HSTORE and GEOMETRY already above them, and ungated for the
            # same reason they are: the word can only have come from a
            # server where the extension exists, because a column of the
            # type requires it.
            if upper.startswith("CUBE"):
                from ..expression.types import PostgresCubeType
                return PostgresCubeType(dialect)
            # LTXTQUERY is checked before LQUERY although the words cannot
            # collide (they differ at the second letter), because the pair
            # is where a prefix mistake would go if one were ever added.
            if upper.startswith("LTXTQUERY"):
                from ..expression.types import PostgresLtxtqueryType
                return PostgresLtxtqueryType(dialect)
            if upper.startswith("LQUERY"):
                from ..expression.types import PostgresLqueryType
                return PostgresLqueryType(dialect)
            if upper.startswith("LTREE"):
                from ..expression.types import PostgresLtreeType
                return PostgresLtreeType(dialect)
            if upper.startswith("RASTER"):
                from ..expression.types import PostgresRasterType
                return PostgresRasterType(dialect)

        # Vector types (pgvector)
        vector_kind = starts_with_word(upper, self._PG_VECTOR_WORDS)
        if vector_kind is not None:
            if (
                hasattr(dialect, "_extensions")
                and "vector" in dialect._extensions
                and not dialect._extensions["vector"].installed
            ):
                raise RuntimeError(
                    "Cannot use pgvector types: the 'vector' (pgvector) extension "
                    "is not installed. Run: CREATE EXTENSION IF NOT EXISTS vector;"
                )
            nums = digit_runs(stripped)
            dim = int(nums[0]) if nums else 0
            if vector_kind == "HALFVEC":
                from ..expression.types import PostgresHalfvecType
                return PostgresHalfvecType(dim=dim, dialect=dialect)
            if vector_kind == "SPARSEVEC":
                from ..expression.types import PostgresSparsevecType
                return PostgresSparsevecType(dim=dim, dialect=dialect)
            from ..expression.types import PostgresVectorType
            return PostgresVectorType(dim=dim, dialect=dialect)

        # Fallback
        return CustomType(dialect=dialect, raw=stripped)


__all__ = ["PostgresTypeParser"]
