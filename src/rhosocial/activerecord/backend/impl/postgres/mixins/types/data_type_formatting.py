# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/data_type_formatting.py
"""PostgreSQL DataType formatting and parsing mixin."""

from __future__ import annotations

import re
from typing import Tuple, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.mixins.ddl_type import DDLTypeMixin
from rhosocial.activerecord.backend.dialect.protocols import DDLTypeSupport
from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    DataType,
    DateType,
    DateTimeType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntType,
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
    TinyIntType,
    VarCharType,
)
from ...expression.types import (
    PostgresArrayType,
    PostgresBigSerialType,
    PostgresBitType,
    PostgresBoxType,
    PostgresByteaType,
    PostgresCIDType,
    PostgresCircleType,
    PostgresDateMultirangeType,
    PostgresDateRangeType,
    PostgresGeographyType,
    PostgresGeometryType,
    PostgresHstoreType,
    PostgresInetType,
    PostgresInt4MultirangeType,
    PostgresInt4RangeType,
    PostgresInt8MultirangeType,
    PostgresInt8RangeType,
    PostgresJsonPathType,
    PostgresLineSegmentType,
    PostgresLineType,
    PostgresMacAddr8Type,
    PostgresMacAddrType,
    PostgresMoneyType,
    PostgresNumMultirangeType,
    PostgresNumRangeType,
    PostgresOIDType,
    PostgresPathType,
    PostgresPgLSNType,
    PostgresPointType,
    PostgresPolygonType,
    PostgresRegClassType,
    PostgresRegTypeType,
    PostgresSerialType,
    PostgresSmallSerialType,
    PostgresTIDType,
    PostgresTSQueryType,
    PostgresTSVectorType,
    PostgresTsMultirangeType,
    PostgresTsRangeType,
    PostgresTsTzMultirangeType,
    PostgresTsTzRangeType,
    PostgresUUIDType,
    PostgresVarBitType,
    PostgresVectorType,
    PostgresXID8Type,
    PostgresXIDType,
    PostgresXMLType,
    PostgresCidrType,
    PostgresCharacterVaryingType,
    PostgresCitextType,
    PostgresCubeType,
    PostgresLtreeType,
    PostgresRasterType,
)

if TYPE_CHECKING:
    pass


class PostgresTypeFormatSupportMixin(DDLTypeMixin, DDLTypeSupport):
    """PostgreSQL DataType formatting and parsing.

    Implements ``DDLTypeSupport`` so the dialect can render ``DataType``
    expressions to SQL strings and parse raw SQL type strings back into
    ``DataType`` instances.
    """

    # ------------------------------------------------------------------
    # DDLTypeSupport — formatting
    # ------------------------------------------------------------------

    # --- PostgreSQL-specific formatters ---

    @DDLTypeMixin.handles(PostgresByteaType)
    def format_data_type_bytea(self, data_type) -> Tuple[str, tuple]:
        return "BYTEA", ()

    @DDLTypeMixin.handles(PostgresSmallSerialType)
    def format_data_type_small_serial(self, data_type) -> Tuple[str, tuple]:
        return "SMALLSERIAL", ()

    @DDLTypeMixin.handles(PostgresSerialType)
    def format_data_type_serial(self, data_type) -> Tuple[str, tuple]:
        return "SERIAL", ()

    @DDLTypeMixin.handles(PostgresBigSerialType)
    def format_data_type_big_serial(self, data_type) -> Tuple[str, tuple]:
        return "BIGSERIAL", ()

    @DDLTypeMixin.handles(PostgresUUIDType)
    def format_data_type_uuid(self, data_type) -> Tuple[str, tuple]:
        return "UUID", ()

    @DDLTypeMixin.handles(PostgresXMLType)
    def format_data_type_xml(self, data_type) -> Tuple[str, tuple]:
        return "XML", ()

    @DDLTypeMixin.handles(PostgresCharacterVaryingType)
    def format_data_type_character_varying(self, data_type: PostgresCharacterVaryingType) -> Tuple[str, tuple]:
        if data_type.length is not None:
            return f"CHARACTER VARYING({data_type.length})", ()
        return "CHARACTER VARYING", ()

    @DDLTypeMixin.handles(PostgresTSVectorType)
    def format_data_type_ts_vector(self, data_type) -> Tuple[str, tuple]:
        return "TSVECTOR", ()

    @DDLTypeMixin.handles(PostgresTSQueryType)
    def format_data_type_ts_query(self, data_type) -> Tuple[str, tuple]:
        return "TSQUERY", ()

    @DDLTypeMixin.handles(PostgresJsonPathType)
    def format_data_type_json_path(self, data_type) -> Tuple[str, tuple]:
        return "JSONPATH", ()

    @DDLTypeMixin.handles(PostgresBitType)
    def format_data_type_bit(self, data_type) -> Tuple[str, tuple]:
        if data_type.n is not None:
            return f"BIT({data_type.n})", ()
        return "BIT", ()

    @DDLTypeMixin.handles(PostgresVarBitType)
    def format_data_type_var_bit(self, data_type) -> Tuple[str, tuple]:
        if data_type.n is not None:
            return f"VARBIT({data_type.n})", ()
        return "VARBIT", ()

    @DDLTypeMixin.handles(PostgresInetType)
    def format_data_type_inet(self, data_type) -> Tuple[str, tuple]:
        return "INET", ()

    @DDLTypeMixin.handles(PostgresCidrType)
    def format_data_type_cidr(self, data_type) -> Tuple[str, tuple]:
        return "CIDR", ()

    @DDLTypeMixin.handles(PostgresMacAddrType)
    def format_data_type_mac_addr(self, data_type) -> Tuple[str, tuple]:
        return "MACADDR", ()

    @DDLTypeMixin.handles(PostgresMacAddr8Type)
    def format_data_type_mac_addr8(self, data_type) -> Tuple[str, tuple]:
        return "MACADDR8", ()

    @DDLTypeMixin.handles(PostgresPointType)
    def format_data_type_point(self, data_type) -> Tuple[str, tuple]:
        return "POINT", ()

    @DDLTypeMixin.handles(PostgresLineType)
    def format_data_type_line(self, data_type) -> Tuple[str, tuple]:
        return "LINE", ()

    @DDLTypeMixin.handles(PostgresLineSegmentType)
    def format_data_type_line_segment(self, data_type) -> Tuple[str, tuple]:
        return "LSEG", ()

    @DDLTypeMixin.handles(PostgresBoxType)
    def format_data_type_box(self, data_type) -> Tuple[str, tuple]:
        return "BOX", ()

    @DDLTypeMixin.handles(PostgresPathType)
    def format_data_type_path(self, data_type) -> Tuple[str, tuple]:
        return "PATH", ()

    @DDLTypeMixin.handles(PostgresPolygonType)
    def format_data_type_polygon(self, data_type) -> Tuple[str, tuple]:
        return "POLYGON", ()

    @DDLTypeMixin.handles(PostgresCircleType)
    def format_data_type_circle(self, data_type) -> Tuple[str, tuple]:
        return "CIRCLE", ()

    @DDLTypeMixin.handles(PostgresMoneyType)
    def format_data_type_money(self, data_type) -> Tuple[str, tuple]:
        return "MONEY", ()

    @DDLTypeMixin.handles(PostgresInt4RangeType)
    def format_data_type_int4_range(self, data_type) -> Tuple[str, tuple]:
        return "INT4RANGE", ()

    @DDLTypeMixin.handles(PostgresInt8RangeType)
    def format_data_type_int8_range(self, data_type) -> Tuple[str, tuple]:
        return "INT8RANGE", ()

    @DDLTypeMixin.handles(PostgresNumRangeType)
    def format_data_type_num_range(self, data_type) -> Tuple[str, tuple]:
        return "NUMRANGE", ()

    @DDLTypeMixin.handles(PostgresTsRangeType)
    def format_data_type_ts_range(self, data_type) -> Tuple[str, tuple]:
        return "TSRANGE", ()

    @DDLTypeMixin.handles(PostgresTsTzRangeType)
    def format_data_type_ts_tz_range(self, data_type) -> Tuple[str, tuple]:
        return "TSTZRANGE", ()

    @DDLTypeMixin.handles(PostgresDateRangeType)
    def format_data_type_date_range(self, data_type) -> Tuple[str, tuple]:
        return "DATERANGE", ()

    @DDLTypeMixin.handles(PostgresInt4MultirangeType)
    def format_data_type_int4_multirange(self, data_type) -> Tuple[str, tuple]:
        return "INT4MULTIRANGE", ()

    @DDLTypeMixin.handles(PostgresInt8MultirangeType)
    def format_data_type_int8_multirange(self, data_type) -> Tuple[str, tuple]:
        return "INT8MULTIRANGE", ()

    @DDLTypeMixin.handles(PostgresNumMultirangeType)
    def format_data_type_num_multirange(self, data_type) -> Tuple[str, tuple]:
        return "NUMMULTIRANGE", ()

    @DDLTypeMixin.handles(PostgresTsMultirangeType)
    def format_data_type_ts_multirange(self, data_type) -> Tuple[str, tuple]:
        return "TSMULTIRANGE", ()

    @DDLTypeMixin.handles(PostgresTsTzMultirangeType)
    def format_data_type_ts_tz_multirange(self, data_type) -> Tuple[str, tuple]:
        return "TSTZMULTIRANGE", ()

    @DDLTypeMixin.handles(PostgresDateMultirangeType)
    def format_data_type_date_multirange(self, data_type) -> Tuple[str, tuple]:
        return "DATEMULTIRANGE", ()

    @DDLTypeMixin.handles(PostgresOIDType)
    def format_data_type_oid(self, data_type) -> Tuple[str, tuple]:
        return "OID", ()

    @DDLTypeMixin.handles(PostgresRegClassType)
    def format_data_type_reg_class(self, data_type) -> Tuple[str, tuple]:
        return "REGCLASS", ()

    @DDLTypeMixin.handles(PostgresRegTypeType)
    def format_data_type_reg_type(self, data_type) -> Tuple[str, tuple]:
        return "REGTYPE", ()

    @DDLTypeMixin.handles(PostgresXIDType)
    def format_data_type_xid(self, data_type) -> Tuple[str, tuple]:
        return "XID", ()

    @DDLTypeMixin.handles(PostgresXID8Type)
    def format_data_type_xid8(self, data_type) -> Tuple[str, tuple]:
        return "XID8", ()

    @DDLTypeMixin.handles(PostgresCIDType)
    def format_data_type_cid(self, data_type) -> Tuple[str, tuple]:
        return "CID", ()

    @DDLTypeMixin.handles(PostgresTIDType)
    def format_data_type_tid(self, data_type) -> Tuple[str, tuple]:
        return "TID", ()

    @DDLTypeMixin.handles(PostgresPgLSNType)
    def format_data_type_pg_lsn(self, data_type) -> Tuple[str, tuple]:
        return "PG_LSN", ()

    @DDLTypeMixin.handles(PostgresHstoreType)
    def format_data_type_hstore(self, data_type) -> Tuple[str, tuple]:
        return "HSTORE", ()

    @DDLTypeMixin.handles(PostgresGeometryType)
    def format_data_type_geometry(self, data_type) -> Tuple[str, tuple]:
        return "GEOMETRY", ()

    @DDLTypeMixin.handles(PostgresGeographyType)
    def format_data_type_geography(self, data_type) -> Tuple[str, tuple]:
        return "GEOGRAPHY", ()

    @DDLTypeMixin.handles(PostgresVectorType)
    def format_data_type_vector(self, data_type) -> Tuple[str, tuple]:
        return f"VECTOR({data_type.dim})", ()

    @DDLTypeMixin.handles(PostgresCitextType)
    def format_data_type_citext(self, data_type) -> Tuple[str, tuple]:
        return "CITEXT", ()

    @DDLTypeMixin.handles(PostgresCubeType)
    def format_data_type_cube(self, data_type) -> Tuple[str, tuple]:
        return "CUBE", ()

    @DDLTypeMixin.handles(PostgresLtreeType)
    def format_data_type_ltree(self, data_type) -> Tuple[str, tuple]:
        return "LTREE", ()

    @DDLTypeMixin.handles(PostgresRasterType)
    def format_data_type_raster(self, data_type) -> Tuple[str, tuple]:
        return "RASTER", ()

    @DDLTypeMixin.handles(ArrayType)
    def format_data_type_array(self, data_type: ArrayType) -> Tuple[str, tuple]:
        elem = data_type.element_type
        elem_cls = type(elem)
        if elem_cls not in self._type_formatters:
            raise TypeError(
                f"ArrayType element type {elem_cls.__name__!r} is not supported "
                f"by {type(self).__name__}. Use a PostgreSQL-specific DataType "
                f"as the element type (e.g. PostgresIntegerType)."
            )
        element_sql, _ = self.format_data_type(elem)
        return element_sql + "[]" * data_type.dimensions, ()

    # --- Core formatters (PostgreSQL specialized) ---

    @DDLTypeMixin.handles(TinyIntType)
    def format_data_type_tiny_int(self, data_type: TinyIntType) -> Tuple[str, tuple]:
        return "SMALLINT", ()

    @DDLTypeMixin.handles(SmallIntType)
    def format_data_type_small_int(self, data_type: SmallIntType) -> Tuple[str, tuple]:
        return "SMALLINT", ()

    @DDLTypeMixin.handles(IntType)
    def format_data_type_int(self, data_type: IntType) -> Tuple[str, tuple]:
        return "INTEGER", ()

    @DDLTypeMixin.handles(IntegerType)
    def format_data_type_integer(self, data_type: IntegerType) -> Tuple[str, tuple]:
        return "INTEGER", ()

    @DDLTypeMixin.handles(BigIntType)
    def format_data_type_big_int(self, data_type: BigIntType) -> Tuple[str, tuple]:
        return "BIGINT", ()

    @DDLTypeMixin.handles(FloatType)
    def format_data_type_float(self, data_type: FloatType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"FLOAT({data_type.precision})", ()
        return "REAL", ()

    @DDLTypeMixin.handles(RealType)
    def format_data_type_real(self, data_type: RealType) -> Tuple[str, tuple]:
        return "REAL", ()

    @DDLTypeMixin.handles(DoubleType)
    def format_data_type_double(self, data_type: DoubleType) -> Tuple[str, tuple]:
        return "DOUBLE PRECISION", ()

    @DDLTypeMixin.handles(DecimalType)
    def format_data_type_decimal(self, data_type: DecimalType) -> Tuple[str, tuple]:
        if data_type.precision is not None and data_type.scale is not None:
            return f"DECIMAL({data_type.precision},{data_type.scale})", ()
        if data_type.precision is not None:
            return f"DECIMAL({data_type.precision})", ()
        return "DECIMAL", ()

    @DDLTypeMixin.handles(CharType)
    def format_data_type_char(self, data_type: CharType) -> Tuple[str, tuple]:
        if data_type.length is not None:
            return f"CHAR({data_type.length})", ()
        return "CHAR", ()

    @DDLTypeMixin.handles(VarCharType)
    def format_data_type_var_char(self, data_type: VarCharType) -> Tuple[str, tuple]:
        if data_type.length is not None:
            return f"VARCHAR({data_type.length})", ()
        return "VARCHAR", ()

    @DDLTypeMixin.handles(TextType)
    def format_data_type_text(self, data_type: TextType) -> Tuple[str, tuple]:
        return "TEXT", ()

    @DDLTypeMixin.handles(BooleanType)
    def format_data_type_boolean(self, data_type: BooleanType) -> Tuple[str, tuple]:
        return "BOOLEAN", ()

    @DDLTypeMixin.handles(BlobType)
    def format_data_type_blob(self, data_type: BlobType) -> Tuple[str, tuple]:
        return "BYTEA", ()

    @DDLTypeMixin.handles(DateType)
    def format_data_type_date(self, data_type: DateType) -> Tuple[str, tuple]:
        return "DATE", ()

    @DDLTypeMixin.handles(TimeType)
    def format_data_type_time(self, data_type: TimeType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIME({data_type.precision})", ()
        return "TIME", ()

    @DDLTypeMixin.handles(TimeTzType)
    def format_data_type_time_tz(self, data_type: TimeTzType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIME({data_type.precision}) WITH TIME ZONE", ()
        return "TIME WITH TIME ZONE", ()

    @DDLTypeMixin.handles(DateTimeType)
    def format_data_type_date_time(self, data_type: DateTimeType) -> Tuple[str, tuple]:
        return "TIMESTAMP", ()

    @DDLTypeMixin.handles(TimestampType)
    def format_data_type_timestamp(self, data_type: TimestampType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIMESTAMP({data_type.precision})", ()
        return "TIMESTAMP", ()

    @DDLTypeMixin.handles(TimestampTzType)
    def format_data_type_timestamp_tz(self, data_type: TimestampTzType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIMESTAMP({data_type.precision}) WITH TIME ZONE", ()
        return "TIMESTAMP WITH TIME ZONE", ()

    @DDLTypeMixin.handles(IntervalType)
    def format_data_type_interval(self, data_type: IntervalType) -> Tuple[str, tuple]:
        if data_type.fields is not None:
            return f"INTERVAL {data_type.fields}", ()
        return "INTERVAL", ()

    @DDLTypeMixin.handles(JsonType)
    def format_data_type_json(self, data_type: JsonType) -> Tuple[str, tuple]:
        return "JSON", ()

    @DDLTypeMixin.handles(JsonBType)
    def format_data_type_json_b(self, data_type: JsonBType) -> Tuple[str, tuple]:
        return "JSONB", ()

    # ------------------------------------------------------------------
    # DDLTypeSupport — parsing
    # ------------------------------------------------------------------

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
        r"^(?:CHAR|VARCHAR|TEXT)\b",
        re.IGNORECASE,
    )
    _PG_BINARY_TYPES = re.compile(
        r"^(?:BYTEA|BLOB)\b",
        re.IGNORECASE,
    )
    _PG_DATE_TYPES = re.compile(
        r"^(?:DATE|DATETIME|TIMESTAMP|TIME|INTERVAL)\b",
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
        r"^(?:MONEY|XML|PG_LSN|HSTORE|GEOMETRY|GEOGRAPHY)\b",
        re.IGNORECASE,
    )
    _PG_ARRAY_SUFFIX = re.compile(r"(\[(\d*)\])+\s*$")
    _PG_ARRAY_KEYWORD = re.compile(r"\s+ARRAY(?:\s*\[\s*(\d+)\s*\])?\s*$", re.IGNORECASE)

    def parse_type(self, raw: str) -> DataType:
        stripped = raw.strip()
        upper = stripped.upper()

        # ---- Array detection (must run before any type-specific match) ----

        # Check for ARRAY keyword: e.g. "INTEGER ARRAY" or "INTEGER ARRAY[3]"
        kw_match = self._PG_ARRAY_KEYWORD.search(upper)
        if kw_match:
            base_raw = stripped[:kw_match.start()]
            element_type = self.parse_type(base_raw)
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type, dimensions=1)

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
            element_type = self.parse_type(remaining)
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type, dimensions=1)

        # ---- Standard type dispatch ----

        # Serial family
        if self._PG_SERIAL_TYPES.match(upper):
            from ..expression.types import (
                PostgresBigSerialType,
                PostgresSerialType,
                PostgresSmallSerialType,
            )
            if upper.startswith("BIGSERIAL") or upper.startswith("SERIAL8"):
                return PostgresBigSerialType()
            if upper.startswith("SMALLSERIAL") or upper.startswith("SERIAL2"):
                return PostgresSmallSerialType()
            return PostgresSerialType()

        # Integer family
        if self._PG_INTEGER_TYPES.match(upper):
            if upper.startswith("SMALLINT") or upper.startswith("INT2"):
                return SmallIntType()
            if upper.startswith("BIGINT") or upper.startswith("INT8"):
                return BigIntType()
            return IntegerType()

        # Float family
        if self._PG_FLOAT_TYPES.match(upper):
            if upper.startswith("DOUBLE"):
                return DoubleType()
            if upper.startswith("REAL") or upper.startswith("FLOAT4"):
                return RealType()
            nums = re.findall(r"\d+", stripped)
            precision = int(nums[0]) if nums else None
            return FloatType(precision)

        # Decimal family
        if self._PG_DECIMAL_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                return DecimalType(int(nums[0]), int(nums[1]))
            if len(nums) == 1:
                return DecimalType(int(nums[0]))
            return DecimalType()

        # String family
        if self._PG_STRING_TYPES.match(upper):
            length_match = re.search(r"\((\d+)\)", stripped)
            length = int(length_match.group(1)) if length_match else None
            if upper.startswith("VARCHAR") or upper.startswith("CHARACTER VARYING"):
                return VarCharType(length)
            if upper.startswith("CHAR") or upper.startswith("CHARACTER"):
                return CharType(length)
            return TextType()

        # Binary
        if self._PG_BINARY_TYPES.match(upper):
            from ..expression.types import PostgresByteaType
            return PostgresByteaType()

        # Date/time
        if self._PG_DATE_TYPES.match(upper):
            if upper.startswith("DATE"):
                return DateType()
            if upper.startswith("DATETIME"):
                return DateTimeType()
            if upper.startswith("TIMESTAMP"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMESTAMPTZ"):
                    return TimestampTzType(precision)
                return TimestampType(precision)
            if upper.startswith("TIME"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMETZ"):
                    return TimeTzType(precision)
                return TimeType(precision)
            if upper.startswith("INTERVAL"):
                fields_match = re.search(r"INTERVAL\s+(.*)", upper)
                fields = fields_match.group(1).strip() if fields_match else None
                return IntervalType(fields)

        # JSON
        if self._PG_JSON_TYPES.match(upper):
            if upper.startswith("JSONB"):
                return JsonBType()
            if upper.startswith("JSONPATH"):
                from ..expression.types import PostgresJsonPathType
                return PostgresJsonPathType()
            return JsonType()

        # UUID
        if self._PG_UUID_TYPES.match(upper):
            from ..expression.types import PostgresUUIDType
            return PostgresUUIDType()

        # Boolean
        if upper.startswith("BOOLEAN") or upper.startswith("BOOL"):
            return BooleanType()

        # Bit string
        if self._PG_BIT_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            n = int(nums[0]) if nums else None
            if upper.startswith("BIT"):
                from ..expression.types import PostgresBitType
                return PostgresBitType(n)
            from ..expression.types import PostgresVarBitType
            return PostgresVarBitType(n)

        # Network address
        if self._PG_NET_TYPES.match(upper):
            from ..expression.types import (
                PostgresCidrType,
                PostgresInetType,
                PostgresMacAddr8Type,
                PostgresMacAddrType,
            )
            if upper.startswith("CIDR"):
                return PostgresCidrType()
            if upper.startswith("MACADDR8"):
                return PostgresMacAddr8Type()
            if upper.startswith("MACADDR"):
                return PostgresMacAddrType()
            return PostgresInetType()

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
            return PostgresPointType()

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
            return PostgresInt4RangeType()

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
            return PostgresInt4MultirangeType()

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
            return PostgresOIDType()

        # Text search
        if self._PG_TS_TYPES.match(upper):
            from ..expression.types import (
                PostgresTSQueryType,
                PostgresTSVectorType,
            )
            if upper.startswith("TSVECTOR"):
                return PostgresTSVectorType()
            return PostgresTSQueryType()

        # Miscellaneous
        if self._PG_MISC_TYPES.match(upper):
            from ..expression.types import (
                PostgresGeographyType,
                PostgresGeometryType,
                PostgresHstoreType,
                PostgresMoneyType,
                PostgresPgLSNType,
                PostgresXMLType,
            )
            if upper.startswith("MONEY"):
                return PostgresMoneyType()
            if upper.startswith("XML"):
                return PostgresXMLType()
            if upper.startswith("PG_LSN"):
                return PostgresPgLSNType()
            if upper.startswith("HSTORE"):
                return PostgresHstoreType()
            if upper.startswith("GEOGRAPHY"):
                return PostgresGeographyType()
            if upper.startswith("GEOMETRY"):
                return PostgresGeometryType()

        # Vector type (pgvector)
        if upper.startswith("VECTOR"):
            nums = re.findall(r"\d+", stripped)
            dim = int(nums[0]) if nums else 0
            from ..expression.types import PostgresVectorType
            return PostgresVectorType(dim)

        # Fallback
        from rhosocial.activerecord.backend.expression.types import CustomType
        return CustomType(stripped)