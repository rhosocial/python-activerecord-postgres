# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/data_type_formatting.py
"""PostgreSQL DataType formatting and parsing mixin."""

from __future__ import annotations

import re
from typing import Tuple, TYPE_CHECKING

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
    PostgresHalfvecType,
    PostgresSparsevecType,
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

    def format_data_type_postgres_bytea(self, data_type) -> Tuple[str, tuple]:
        return "BYTEA", ()

    def format_data_type_postgres_smallserial(self, data_type) -> Tuple[str, tuple]:
        return "SMALLSERIAL", ()

    def format_data_type_postgres_serial(self, data_type) -> Tuple[str, tuple]:
        return "SERIAL", ()

    def format_data_type_postgres_bigserial(self, data_type) -> Tuple[str, tuple]:
        return "BIGSERIAL", ()

    def format_data_type_postgres_uuid(self, data_type) -> Tuple[str, tuple]:
        return "UUID", ()

    def format_data_type_postgres_xml(self, data_type) -> Tuple[str, tuple]:
        return "XML", ()

    def format_data_type_postgres_character_varying(self, data_type: PostgresCharacterVaryingType) -> Tuple[str, tuple]:
        if data_type.length is not None:
            return f"CHARACTER VARYING({data_type.length})", ()
        return "CHARACTER VARYING", ()

    def format_data_type_postgres_tsvector(self, data_type) -> Tuple[str, tuple]:
        return "TSVECTOR", ()

    def format_data_type_postgres_tsquery(self, data_type) -> Tuple[str, tuple]:
        return "TSQUERY", ()

    def format_data_type_postgres_jsonpath(self, data_type) -> Tuple[str, tuple]:
        return "JSONPATH", ()

    def format_data_type_postgres_bit(self, data_type) -> Tuple[str, tuple]:
        if data_type.n is not None:
            return f"BIT({data_type.n})", ()
        return "BIT", ()

    def format_data_type_postgres_varbit(self, data_type) -> Tuple[str, tuple]:
        if data_type.n is not None:
            return f"VARBIT({data_type.n})", ()
        return "VARBIT", ()

    def format_data_type_postgres_inet(self, data_type) -> Tuple[str, tuple]:
        return "INET", ()

    def format_data_type_postgres_cidr(self, data_type) -> Tuple[str, tuple]:
        return "CIDR", ()

    def format_data_type_postgres_macaddr(self, data_type) -> Tuple[str, tuple]:
        return "MACADDR", ()

    def format_data_type_postgres_macaddr8(self, data_type) -> Tuple[str, tuple]:
        return "MACADDR8", ()

    def format_data_type_postgres_point(self, data_type) -> Tuple[str, tuple]:
        return "POINT", ()

    def format_data_type_postgres_line(self, data_type) -> Tuple[str, tuple]:
        return "LINE", ()

    def format_data_type_postgres_line_segment(self, data_type) -> Tuple[str, tuple]:
        return "LSEG", ()

    def format_data_type_postgres_box(self, data_type) -> Tuple[str, tuple]:
        return "BOX", ()

    def format_data_type_postgres_path(self, data_type) -> Tuple[str, tuple]:
        return "PATH", ()

    def format_data_type_postgres_polygon(self, data_type) -> Tuple[str, tuple]:
        return "POLYGON", ()

    def format_data_type_postgres_circle(self, data_type) -> Tuple[str, tuple]:
        return "CIRCLE", ()

    def format_data_type_postgres_money(self, data_type) -> Tuple[str, tuple]:
        return "MONEY", ()

    def format_data_type_postgres_int4range(self, data_type) -> Tuple[str, tuple]:
        return "INT4RANGE", ()

    def format_data_type_postgres_int8range(self, data_type) -> Tuple[str, tuple]:
        return "INT8RANGE", ()

    def format_data_type_postgres_numrange(self, data_type) -> Tuple[str, tuple]:
        return "NUMRANGE", ()

    def format_data_type_postgres_tsrange(self, data_type) -> Tuple[str, tuple]:
        return "TSRANGE", ()

    def format_data_type_postgres_tstzrange(self, data_type) -> Tuple[str, tuple]:
        return "TSTZRANGE", ()

    def format_data_type_postgres_daterange(self, data_type) -> Tuple[str, tuple]:
        return "DATERANGE", ()

    def format_data_type_postgres_int4multirange(self, data_type) -> Tuple[str, tuple]:
        return "INT4MULTIRANGE", ()

    def format_data_type_postgres_int8multirange(self, data_type) -> Tuple[str, tuple]:
        return "INT8MULTIRANGE", ()

    def format_data_type_postgres_nummultirange(self, data_type) -> Tuple[str, tuple]:
        return "NUMMULTIRANGE", ()

    def format_data_type_postgres_tsmultirange(self, data_type) -> Tuple[str, tuple]:
        return "TSMULTIRANGE", ()

    def format_data_type_postgres_tstzmultirange(self, data_type) -> Tuple[str, tuple]:
        return "TSTZMULTIRANGE", ()

    def format_data_type_postgres_datemultirange(self, data_type) -> Tuple[str, tuple]:
        return "DATEMULTIRANGE", ()

    def format_data_type_postgres_oid(self, data_type) -> Tuple[str, tuple]:
        return "OID", ()

    def format_data_type_postgres_regclass(self, data_type) -> Tuple[str, tuple]:
        return "REGCLASS", ()

    def format_data_type_postgres_regtype(self, data_type) -> Tuple[str, tuple]:
        return "REGTYPE", ()

    def format_data_type_postgres_xid(self, data_type) -> Tuple[str, tuple]:
        return "XID", ()

    def format_data_type_postgres_xid8(self, data_type) -> Tuple[str, tuple]:
        return "XID8", ()

    def format_data_type_postgres_cid(self, data_type) -> Tuple[str, tuple]:
        return "CID", ()

    def format_data_type_postgres_tid(self, data_type) -> Tuple[str, tuple]:
        return "TID", ()

    def format_data_type_postgres_pg_lsn(self, data_type) -> Tuple[str, tuple]:
        return "PG_LSN", ()

    def format_data_type_postgres_hstore(self, data_type) -> Tuple[str, tuple]:
        return "HSTORE", ()

    def format_data_type_postgres_geometry(self, data_type) -> Tuple[str, tuple]:
        return "GEOMETRY", ()

    def format_data_type_postgres_geography(self, data_type) -> Tuple[str, tuple]:
        return "GEOGRAPHY", ()

    def format_data_type_postgres_vector(self, data_type) -> Tuple[str, tuple]:
        return f"VECTOR({data_type.dim})", ()

    def format_data_type_postgres_halfvec(self, data_type) -> Tuple[str, tuple]:
        return f"HALFVEC({data_type.dim})", ()

    def format_data_type_postgres_sparsevec(self, data_type) -> Tuple[str, tuple]:
        return f"SPARSEVEC({data_type.dim})", ()

    def format_data_type_postgres_citext(self, data_type) -> Tuple[str, tuple]:
        return "CITEXT", ()

    def format_data_type_postgres_cube(self, data_type) -> Tuple[str, tuple]:
        return "CUBE", ()

    def format_data_type_postgres_ltree(self, data_type) -> Tuple[str, tuple]:
        return "LTREE", ()

    def format_data_type_postgres_raster(self, data_type) -> Tuple[str, tuple]:
        return "RASTER", ()

    def format_data_type_postgres_array(self, data_type: ArrayType) -> Tuple[str, tuple]:
        element_sql, _ = self.format_data_type(data_type.element_type)
        return element_sql + "[]" * data_type.dimensions, ()

    # --- Core formatters (PostgreSQL specialized) ---

    def format_data_type_tinyint(self, data_type: TinyIntType) -> Tuple[str, tuple]:
        return "SMALLINT", ()

    def format_data_type_smallint(self, data_type: SmallIntType) -> Tuple[str, tuple]:
        return "SMALLINT", ()

    def format_data_type_int(self, data_type: IntType) -> Tuple[str, tuple]:
        return "INTEGER", ()

    def format_data_type_integer(self, data_type: IntegerType) -> Tuple[str, tuple]:
        return "INTEGER", ()

    def format_data_type_bigint(self, data_type: BigIntType) -> Tuple[str, tuple]:
        return "BIGINT", ()

    def format_data_type_float(self, data_type: FloatType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"FLOAT({data_type.precision})", ()
        return "REAL", ()

    def format_data_type_real(self, data_type: RealType) -> Tuple[str, tuple]:
        return "REAL", ()

    def format_data_type_double(self, data_type: DoubleType) -> Tuple[str, tuple]:
        return "DOUBLE PRECISION", ()

    def format_data_type_decimal(self, data_type: DecimalType) -> Tuple[str, tuple]:
        if data_type.precision is not None and data_type.scale is not None:
            return f"DECIMAL({data_type.precision},{data_type.scale})", ()
        if data_type.precision is not None:
            return f"DECIMAL({data_type.precision})", ()
        return "DECIMAL", ()

    def format_data_type_char(self, data_type: CharType) -> Tuple[str, tuple]:
        if data_type.length is not None:
            return f"CHAR({data_type.length})", ()
        return "CHAR", ()

    def format_data_type_varchar(self, data_type: VarCharType) -> Tuple[str, tuple]:
        if data_type.length is not None:
            return f"VARCHAR({data_type.length})", ()
        return "VARCHAR", ()

    def format_data_type_text(self, data_type: TextType) -> Tuple[str, tuple]:
        return "TEXT", ()

    def format_data_type_boolean(self, data_type: BooleanType) -> Tuple[str, tuple]:
        return "BOOLEAN", ()

    def format_data_type_blob(self, data_type: BlobType) -> Tuple[str, tuple]:
        return "BYTEA", ()

    def format_data_type_date(self, data_type: DateType) -> Tuple[str, tuple]:
        return "DATE", ()

    def format_data_type_time(self, data_type: TimeType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIME({data_type.precision})", ()
        return "TIME", ()

    def format_data_type_timetz(self, data_type: TimeTzType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIME({data_type.precision}) WITH TIME ZONE", ()
        return "TIME WITH TIME ZONE", ()

    def format_data_type_datetime(self, data_type: DateTimeType) -> Tuple[str, tuple]:
        return "TIMESTAMP", ()

    def format_data_type_timestamp(self, data_type: TimestampType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIMESTAMP({data_type.precision})", ()
        return "TIMESTAMP", ()

    def format_data_type_timestamptz(self, data_type: TimestampTzType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            return f"TIMESTAMP({data_type.precision}) WITH TIME ZONE", ()
        return "TIMESTAMP WITH TIME ZONE", ()

    def format_data_type_interval(self, data_type: IntervalType) -> Tuple[str, tuple]:
        if data_type.fields is not None:
            return f"INTERVAL {data_type.fields}", ()
        return "INTERVAL", ()

    def format_data_type_json(self, data_type: JsonType) -> Tuple[str, tuple]:
        return "JSON", ()

    def format_data_type_jsonb(self, data_type: JsonBType) -> Tuple[str, tuple]:
        return "JSONB", ()

    # ------------------------------------------------------------------
    # Supported types advertisement
    # ------------------------------------------------------------------

    def supports_data_types(self) -> dict:
        """Mapping ``{<name>: concrete type class}`` of every type this
        dialect renders.

        The formatters a dialect defines **are** the set of types it
        supports (naming-convention dispatch has no registry), so this
        derives the mapping from the ``format_data_type_<name>`` methods.
        """
        result = {}
        for member_name in dir(type(self)):
            match = re.match(r"^format_data_type_([a-z][a-z0-9_]*)$", member_name)
            if not match:
                continue
            name = match.group(1)
            cls = self._type_class_for(name)
            if cls is not None:
                result[name] = cls
        return result

    def format_data_type_uuid(self, data_type) -> Tuple[str, tuple]:
        return "UUID", ()

    def format_data_type_custom(self, data_type) -> Tuple[str, tuple]:
        return data_type.raw, ()

    def format_data_type_array(self, data_type: ArrayType) -> Tuple[str, tuple]:
        element_sql, _ = self.format_data_type(data_type.element_type)
        return element_sql + "[]" * data_type.dimensions, ()

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
        r"^(?:CHARACTER|CHAR|VARCHAR|TEXT)\b",
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
            return PostgresArrayType(element_type, dimensions=1, dialect=self)

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
            return PostgresArrayType(element_type, dimensions=1, dialect=self)

        # ---- Standard type dispatch ----

        # Serial family
        if self._PG_SERIAL_TYPES.match(upper):
            from ...expression.types import (
                PostgresBigSerialType,
                PostgresSerialType,
                PostgresSmallSerialType,
            )
            if upper.startswith("BIGSERIAL") or upper.startswith("SERIAL8"):
                return PostgresBigSerialType(self)
            if upper.startswith("SMALLSERIAL") or upper.startswith("SERIAL2"):
                return PostgresSmallSerialType(self)
            return PostgresSerialType(self)

        # Integer family
        if self._PG_INTEGER_TYPES.match(upper):
            if upper.startswith("SMALLINT") or upper.startswith("INT2"):
                return SmallIntType(self)
            if upper.startswith("BIGINT") or upper.startswith("INT8"):
                return BigIntType(self)
            return IntegerType(self)

        # Float family
        if self._PG_FLOAT_TYPES.match(upper):
            if upper.startswith("DOUBLE"):
                return DoubleType(self)
            if upper.startswith("REAL") or upper.startswith("FLOAT4"):
                return RealType(self)
            nums = re.findall(r"\d+", stripped)
            precision = int(nums[0]) if nums else None
            return FloatType(precision, self)

        # Decimal family
        if self._PG_DECIMAL_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                return DecimalType(int(nums[0]), int(nums[1]), self)
            if len(nums) == 1:
                return DecimalType(int(nums[0]), self)
            return DecimalType(self)

        # String family
        if self._PG_STRING_TYPES.match(upper):
            length_match = re.search(r"\((\d+)\)", stripped)
            length = int(length_match.group(1)) if length_match else None
            if upper.startswith("VARCHAR") or upper.startswith("CHARACTER VARYING"):
                return VarCharType(length, self)
            if upper.startswith("CHAR") or upper.startswith("CHARACTER"):
                return CharType(length, self)
            return TextType(self)

        # Binary
        if self._PG_BINARY_TYPES.match(upper):
            from ...expression.types import PostgresByteaType
            return PostgresByteaType(self)

        # Date/time
        if self._PG_DATE_TYPES.match(upper):
            if upper.startswith("DATETIME"):
                return DateTimeType(self)
            if upper.startswith("DATE"):
                return DateType(self)
            if upper.startswith("TIMESTAMP"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMESTAMPTZ"):
                    return TimestampTzType(precision, self)
                return TimestampType(precision, self)
            if upper.startswith("TIME"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMETZ"):
                    return TimeTzType(precision, self)
                return TimeType(precision, self)
            if upper.startswith("INTERVAL"):
                fields_match = re.search(r"INTERVAL\s+(.*)", upper)
                fields = fields_match.group(1).strip() if fields_match else None
                return IntervalType(fields, self)

        # JSON
        if self._PG_JSON_TYPES.match(upper):
            if upper.startswith("JSONB"):
                return JsonBType(self)
            if upper.startswith("JSONPATH"):
                from ...expression.types import PostgresJsonPathType
                return PostgresJsonPathType(self)
            return JsonType(self)

        # UUID
        if self._PG_UUID_TYPES.match(upper):
            from ...expression.types import PostgresUUIDType
            return PostgresUUIDType(self)

        # Boolean
        if upper.startswith("BOOLEAN") or upper.startswith("BOOL"):
            return BooleanType(self)

        # Bit string
        if self._PG_BIT_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            n = int(nums[0]) if nums else None
            if upper.startswith("BIT"):
                from ...expression.types import PostgresBitType
                return PostgresBitType(n, self)
            from ...expression.types import PostgresVarBitType
            return PostgresVarBitType(n, self)

        # Network address
        if self._PG_NET_TYPES.match(upper):
            from ...expression.types import (
                PostgresCidrType,
                PostgresInetType,
                PostgresMacAddr8Type,
                PostgresMacAddrType,
            )
            if upper.startswith("CIDR"):
                return PostgresCidrType(self)
            if upper.startswith("MACADDR8"):
                return PostgresMacAddr8Type(self)
            if upper.startswith("MACADDR"):
                return PostgresMacAddrType(self)
            return PostgresInetType(self)

        # Geometric
        if self._PG_GEOM_TYPES.match(upper):
            from ...expression.types import (
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
            return PostgresPointType(self)

        # Range types
        if self._PG_RANGE_TYPES.match(upper):
            from ...expression.types import (
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
            return PostgresInt4RangeType(self)

        # Multirange types
        if self._PG_MULTIRANGE_TYPES.match(upper):
            from ...expression.types import (
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
            return PostgresInt4MultirangeType(self)

        # OID types
        if self._PG_OID_TYPES.match(upper):
            from ...expression.types import (
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
            return PostgresOIDType(self)

        # Text search
        if self._PG_TS_TYPES.match(upper):
            from ...expression.types import (
                PostgresTSQueryType,
                PostgresTSVectorType,
            )
            if upper.startswith("TSVECTOR"):
                return PostgresTSVectorType(self)
            return PostgresTSQueryType(self)

        # Miscellaneous
        if self._PG_MISC_TYPES.match(upper):
            from ...expression.types import (
                PostgresGeographyType,
                PostgresGeometryType,
                PostgresHstoreType,
                PostgresMoneyType,
                PostgresPgLSNType,
                PostgresXMLType,
            )
            if upper.startswith("MONEY"):
                return PostgresMoneyType(self)
            if upper.startswith("XML"):
                return PostgresXMLType(self)
            if upper.startswith("PG_LSN"):
                return PostgresPgLSNType(self)
            if upper.startswith("HSTORE"):
                return PostgresHstoreType(self)
            if upper.startswith("GEOGRAPHY"):
                return PostgresGeographyType(self)
            if upper.startswith("GEOMETRY"):
                return PostgresGeometryType(self)

        # Vector types (pgvector)
        vector_match = re.match(r"^(VECTOR|HALFVEC|SPARSEVEC)\b", upper)
        if vector_match:
            if (
                hasattr(self, "_extensions")
                and "vector" in self._extensions
                and not self._extensions["vector"].installed
            ):
                raise RuntimeError(
                    "Cannot use pgvector types: the 'vector' (pgvector) extension "
                    "is not installed. Run: CREATE EXTENSION IF NOT EXISTS vector;"
                )
            nums = re.findall(r"\d+", stripped)
            dim = int(nums[0]) if nums else 0
            vector_kind = vector_match.group(1)
            if vector_kind == "HALFVEC":
                from ...expression.types import PostgresHalfvecType
                return PostgresHalfvecType(dim, self)
            if vector_kind == "SPARSEVEC":
                from ...expression.types import PostgresSparsevecType
                return PostgresSparsevecType(dim, self)
            from ...expression.types import PostgresVectorType
            return PostgresVectorType(dim, self)

        # Fallback
        from rhosocial.activerecord.backend.expression.types import CustomType
        return CustomType(stripped, self)