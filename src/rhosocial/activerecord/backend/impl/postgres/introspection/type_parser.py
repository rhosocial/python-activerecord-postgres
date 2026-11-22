# src/rhosocial/activerecord/backend/impl/postgres/introspection/type_parser.py
"""PostgreSQL's type-spelling parser (read direction, introspection-only).

Backend-local helper: parsing a server-reported type spelling back into a
``DataType`` is not an expression-construction concern and is no longer a
dialect-protocol member (the dialect's ``parse_type`` was removed when core
delegated ``DataType.parse_data_type_str`` to ``CustomType``). It lives under
PostgreSQL introspection and is used only when reading back a column's declared
type; it is NOT a dialect protocol member.
"""
import re

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
from ..expression.types import PostgresArrayType


class PostgresTypeParser:
    """Parse a raw PostgreSQL type spelling into a ``DataType``, bound to a dialect."""

    _PG_INTEGER_TYPES = re.compile(
        r"^(?:SMALLINT|INT2|INT|INTEGER|INT4|BIGINT|INT8)\b",
        re.IGNORECASE,
    )
    _PG_SERIAL_TYPES = re.compile(
        r"^(?:SMALLSERIAL|SERIAL2|SERIAL|SERIAL4|BIGSERIAL|SERIAL8)\b",
        re.IGNORECASE,
    )
    _PG_FLOAT_TYPES = re.compile(
        r"^(?:REAL|FLOAT4|FLOAT|DOUBLE)\b",
        re.IGNORECASE,
    )
    _PG_DECIMAL_TYPES = re.compile(
        r"^(?:DECIMAL|NUMERIC)\b",
        re.IGNORECASE,
    )
    _PG_STRING_TYPES = re.compile(
        r"^(?:CHARACTER|CHAR|VARCHAR|TEXT|CITEXT)\b",
        re.IGNORECASE,
    )
    _PG_BINARY_TYPES = re.compile(
        r"^(?:BYTEA|BLOB)\b",
        re.IGNORECASE,
    )
    _PG_DATE_TYPES = re.compile(
        r"^(?:DATETIME|DATE|TIMESTAMPTZ|TIMESTAMP|TIMETZ|TIME|INTERVAL)\b",
        re.IGNORECASE,
    )
    _PG_JSON_TYPES = re.compile(
        r"^(?:JSON|JSONB|JSONPATH)\b",
        re.IGNORECASE,
    )
    _PG_UUID_TYPES = re.compile(
        r"^(?:UUID)\b",
        re.IGNORECASE,
    )
    _PG_NET_TYPES = re.compile(
        r"^(?:INET|CIDR|MACADDR|MACADDR8)\b",
        re.IGNORECASE,
    )
    _PG_GEOM_TYPES = re.compile(
        r"^(?:POINT|LINE|LSEG|BOX|PATH|POLYGON|CIRCLE)\b",
        re.IGNORECASE,
    )
    _PG_BIT_TYPES = re.compile(
        r"^(?:BIT|VARBIT)\b",
        re.IGNORECASE,
    )
    _PG_RANGE_TYPES = re.compile(
        r"^(?:INT4RANGE|INT8RANGE|NUMRANGE|TSRANGE|TSTZRANGE|DATERANGE)\b",
        re.IGNORECASE,
    )
    _PG_MULTIRANGE_TYPES = re.compile(
        r"^(?:INT4MULTIRANGE|INT8MULTIRANGE|NUMMULTIRANGE|"
        r"TSMULTIRANGE|TSTZMULTIRANGE|DATEMULTIRANGE)\b",
        re.IGNORECASE,
    )
    _PG_OID_TYPES = re.compile(
        r"^(?:OID|REGCLASS|REGTYPE|XID|XID8|CID|TID)\b",
        re.IGNORECASE,
    )
    _PG_TS_TYPES = re.compile(
        r"^(?:TSVECTOR|TSQUERY)\b",
        re.IGNORECASE,
    )
    _PG_MISC_TYPES = re.compile(
        r"^(?:MONEY|XML|PG_LSN|HSTORE|GEOMETRY|GEOGRAPHY|CUBE|LTREE|LQUERY|"
        r"LTXTQUERY|RASTER)\b",
        re.IGNORECASE,
    )
    _PG_ARRAY_SUFFIX = re.compile(r"(\[(\d*)\])+\s*$")
    _PG_ARRAY_KEYWORD = re.compile(r"\s+ARRAY(?:\s*\[\s*(\d+)\s*\])?\s*$", re.IGNORECASE)

    def __init__(self, dialect):
        self.dialect = dialect

    def parse(self, raw: str) -> DataType:
        dialect = self.dialect
        stripped = raw.strip()
        upper = stripped.upper()

        # ---- Array detection (must run before any type-specific match) ----

        # Check for ARRAY keyword: e.g. "INTEGER ARRAY" or "INTEGER ARRAY[3]"
        kw_match = self._PG_ARRAY_KEYWORD.search(upper)
        if kw_match:
            base_raw = stripped[:kw_match.start()]
            element_type = self.parse(base_raw)
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type=element_type, dimensions=1, dialect=dialect)

        # Check for array bracket suffix: e.g. "INTEGER[]", "INTEGER[][]"
        remaining = stripped
        while remaining.endswith("[]"):
            remaining = remaining[:-2]
        while remaining.endswith("]"):
            m = self._PG_ARRAY_SUFFIX.search(remaining)
            if m:
                remaining = remaining[:m.start()]
            else:
                break

        if remaining != stripped:
            element_type = self.parse(remaining)
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type=element_type, dimensions=1, dialect=dialect)

        # ---- Standard type dispatch ----

        # Serial family
        if self._PG_SERIAL_TYPES.match(upper):
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
        if self._PG_INTEGER_TYPES.match(upper):
            if upper.startswith("SMALLINT") or upper.startswith("INT2"):
                return SmallIntType(dialect)
            if upper.startswith("BIGINT") or upper.startswith("INT8"):
                return BigIntType(dialect)
            return IntegerType(dialect)

        # Float family
        if self._PG_FLOAT_TYPES.match(upper):
            if upper.startswith("DOUBLE"):
                return DoubleType(dialect=dialect)
            if upper.startswith("REAL") or upper.startswith("FLOAT4"):
                return RealType(dialect=dialect)
            nums = re.findall(r"\d+", stripped)
            precision = int(nums[0]) if nums else None
            return FloatType(dialect=dialect, precision=precision)

        # Decimal family
        if self._PG_DECIMAL_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                return DecimalType(dialect=dialect, precision=int(nums[0]), scale=int(nums[1]))
            if len(nums) == 1:
                return DecimalType(dialect=dialect, precision=int(nums[0]))
            return DecimalType(dialect=dialect)

        # String family
        if self._PG_STRING_TYPES.match(upper):
            length_match = re.search(r"\((\d+)\)", stripped)
            length = int(length_match.group(1)) if length_match else None
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
        if self._PG_BINARY_TYPES.match(upper):
            return BlobType(dialect=dialect)

        # Date/time
        if self._PG_DATE_TYPES.match(upper):
            if upper.startswith("DATETIME"):
                return DateTimeType(dialect=dialect)
            if upper.startswith("DATE"):
                return DateType(dialect=dialect)
            if upper.startswith("TIMESTAMP"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMESTAMPTZ"):
                    return TimestampTzType(dialect=dialect, precision=precision)
                return TimestampType(dialect=dialect, precision=precision)
            if upper.startswith("TIME"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMETZ"):
                    return TimeTzType(dialect=dialect, precision=precision)
                return TimeType(dialect=dialect, precision=precision)
            if upper.startswith("INTERVAL"):
                fields_match = re.search(r"INTERVAL\s+(.*)", upper)
                fields = fields_match.group(1).strip() if fields_match else None
                return IntervalType(dialect=dialect, fields=fields)

        # JSON
        if self._PG_JSON_TYPES.match(upper):
            if upper.startswith("JSONB"):
                return JsonBType(dialect=dialect)
            if upper.startswith("JSONPATH"):
                from ..expression.types import PostgresJsonPathType
                return PostgresJsonPathType(dialect=dialect)
            return JsonType(dialect=dialect)

        # UUID
        if self._PG_UUID_TYPES.match(upper):
            return UUIDType(dialect=dialect)

        # Boolean
        if upper.startswith("BOOLEAN") or upper.startswith("BOOL"):
            return BooleanType(dialect=dialect)

        # Bit string
        if self._PG_BIT_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            n = int(nums[0]) if nums else None
            # The variable-length family is tested first: live ``format_type``
            # emits ``bit varying(n)`` for a varbit column, and that string
            # starts with ``BIT`` exactly as the fixed-length word does.  A
            # leading ``startswith("BIT")`` test sent the catalog spelling to
            # ``PostgresBitType``; ``VARBIT`` and ``BIT VARYING`` are the two
            # words for the one storage and must answer the same class.
            if upper.startswith("VARBIT") or re.match(r"BIT\s+VARYING\b", upper):
                from ..expression.types import PostgresVarBitType
                return PostgresVarBitType(n=n, dialect=dialect)
            from ..expression.types import PostgresBitType
            return PostgresBitType(n=n, dialect=dialect)

        # Network address
        if self._PG_NET_TYPES.match(upper):
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
        if self._PG_GEOM_TYPES.match(upper):
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
        if self._PG_RANGE_TYPES.match(upper):
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
        if self._PG_MULTIRANGE_TYPES.match(upper):
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
        if self._PG_OID_TYPES.match(upper):
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
        if self._PG_TS_TYPES.match(upper):
            from ..expression.types import (
                PostgresTSQueryType,
                PostgresTSVectorType,
            )
            if upper.startswith("TSVECTOR"):
                return PostgresTSVectorType(dialect)
            return PostgresTSQueryType(dialect)

        # Miscellaneous
        if self._PG_MISC_TYPES.match(upper):
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
        vector_match = re.match(r"^(VECTOR|HALFVEC|SPARSEVEC)\b", upper)
        if vector_match:
            if (
                hasattr(dialect, "_extensions")
                and "vector" in dialect._extensions
                and not dialect._extensions["vector"].installed
            ):
                raise RuntimeError(
                    "Cannot use pgvector types: the 'vector' (pgvector) extension "
                    "is not installed. Run: CREATE EXTENSION IF NOT EXISTS vector;"
                )
            nums = re.findall(r"\d+", stripped)
            dim = int(nums[0]) if nums else 0
            vector_kind = vector_match.group(1)
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
