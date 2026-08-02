# tests/rhosocial/activerecord_postgres_test/feature/backend/types/test_data_type_formatting.py
"""Pure-function tests for PostgreSQL DataType formatting and parsing.

These tests exercise ``PostgresTypeFormatSupportMixin`` (the
``PostgresDialect.format_data_type`` and ``PostgresDialect.parse_type``
paths) without requiring a live PostgreSQL server.
"""

import pytest

from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    CustomType,
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
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresArrayType,
    PostgresBigSerialType,
    PostgresBitType,
    PostgresBoxType,
    PostgresByteaType,
    PostgresCIDType,
    PostgresCharacterVaryingType,
    PostgresCidrType,
    PostgresCircleType,
    PostgresCitextType,
    PostgresCubeType,
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
    PostgresLtreeType,
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
    PostgresRasterType,
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
)


@pytest.fixture
def dialect():
    return PostgresDialect(version=(15, 0, 0))


# ---------------------------------------------------------------------------
# format_data_type — PostgreSQL-specific types
# ---------------------------------------------------------------------------

FORMAT_CASES = [
    (PostgresByteaType(), "BYTEA"),
    (PostgresSmallSerialType(), "SMALLSERIAL"),
    (PostgresSerialType(), "SERIAL"),
    (PostgresBigSerialType(), "BIGSERIAL"),
    (PostgresUUIDType(), "UUID"),
    (PostgresXMLType(), "XML"),
    (PostgresTSVectorType(), "TSVECTOR"),
    (PostgresTSQueryType(), "TSQUERY"),
    (PostgresJsonPathType(), "JSONPATH"),
    (PostgresBitType(), "BIT"),
    (PostgresBitType(n=8), "BIT(8)"),
    (PostgresVarBitType(), "VARBIT"),
    (PostgresVarBitType(n=16), "VARBIT(16)"),
    (PostgresInetType(), "INET"),
    (PostgresCidrType(), "CIDR"),
    (PostgresMacAddrType(), "MACADDR"),
    (PostgresMacAddr8Type(), "MACADDR8"),
    (PostgresPointType(), "POINT"),
    (PostgresLineType(), "LINE"),
    (PostgresLineSegmentType(), "LSEG"),
    (PostgresBoxType(), "BOX"),
    (PostgresPathType(), "PATH"),
    (PostgresPolygonType(), "POLYGON"),
    (PostgresCircleType(), "CIRCLE"),
    (PostgresMoneyType(), "MONEY"),
    (PostgresInt4RangeType(), "INT4RANGE"),
    (PostgresInt8RangeType(), "INT8RANGE"),
    (PostgresNumRangeType(), "NUMRANGE"),
    (PostgresTsRangeType(), "TSRANGE"),
    (PostgresTsTzRangeType(), "TSTZRANGE"),
    (PostgresDateRangeType(), "DATERANGE"),
    (PostgresInt4MultirangeType(), "INT4MULTIRANGE"),
    (PostgresInt8MultirangeType(), "INT8MULTIRANGE"),
    (PostgresNumMultirangeType(), "NUMMULTIRANGE"),
    (PostgresTsMultirangeType(), "TSMULTIRANGE"),
    (PostgresTsTzMultirangeType(), "TSTZMULTIRANGE"),
    (PostgresDateMultirangeType(), "DATEMULTIRANGE"),
    (PostgresOIDType(), "OID"),
    (PostgresRegClassType(), "REGCLASS"),
    (PostgresRegTypeType(), "REGTYPE"),
    (PostgresXIDType(), "XID"),
    (PostgresXID8Type(), "XID8"),
    (PostgresCIDType(), "CID"),
    (PostgresTIDType(), "TID"),
    (PostgresPgLSNType(), "PG_LSN"),
    (PostgresHstoreType(), "HSTORE"),
    (PostgresGeometryType(), "GEOMETRY"),
    (PostgresGeographyType(), "GEOGRAPHY"),
    (PostgresVectorType(dim=384), "VECTOR(384)"),
    (PostgresCitextType(), "CITEXT"),
    (PostgresCubeType(), "CUBE"),
    (PostgresLtreeType(), "LTREE"),
    (PostgresRasterType(), "RASTER"),
    (PostgresCharacterVaryingType(), "CHARACTER VARYING"),
    (PostgresCharacterVaryingType(length=100), "CHARACTER VARYING(100)"),
]


@pytest.mark.parametrize("data_type,expected", FORMAT_CASES)
def test_format_postgres_specific_types(dialect, data_type, expected):
    sql, params = dialect.format_data_type(data_type)
    assert sql == expected
    assert params == ()


# ---------------------------------------------------------------------------
# format_data_type — core types with PostgreSQL specialisation
# ---------------------------------------------------------------------------

CORE_FORMAT_CASES = [
    (TinyIntType(), "SMALLINT"),
    (SmallIntType(), "SMALLINT"),
    (IntType(), "INTEGER"),
    (IntegerType(), "INTEGER"),
    (BigIntType(), "BIGINT"),
    (FloatType(), "REAL"),
    (FloatType(precision=8), "FLOAT(8)"),
    (RealType(), "REAL"),
    (DoubleType(), "DOUBLE PRECISION"),
    (DecimalType(), "DECIMAL"),
    (DecimalType(precision=10), "DECIMAL(10)"),
    (DecimalType(precision=10, scale=2), "DECIMAL(10,2)"),
    (CharType(), "CHAR"),
    (CharType(length=5), "CHAR(5)"),
    (VarCharType(), "VARCHAR"),
    (VarCharType(length=50), "VARCHAR(50)"),
    (TextType(), "TEXT"),
    (BooleanType(), "BOOLEAN"),
    (BlobType(), "BYTEA"),
    (DateType(), "DATE"),
    (TimeType(), "TIME"),
    (TimeType(precision=6), "TIME(6)"),
    (TimeTzType(), "TIME WITH TIME ZONE"),
    (TimeTzType(precision=6), "TIME(6) WITH TIME ZONE"),
    (DateTimeType(), "TIMESTAMP"),
    (TimestampType(), "TIMESTAMP"),
    (TimestampType(precision=3), "TIMESTAMP(3)"),
    (TimestampTzType(), "TIMESTAMP WITH TIME ZONE"),
    (TimestampTzType(precision=3), "TIMESTAMP(3) WITH TIME ZONE"),
    (IntervalType(), "INTERVAL"),
    (IntervalType(fields="DAY TO SECOND"), "INTERVAL DAY TO SECOND"),
    (JsonType(), "JSON"),
    (JsonBType(), "JSONB"),
]


@pytest.mark.parametrize("data_type,expected", CORE_FORMAT_CASES)
def test_format_core_types(dialect, data_type, expected):
    sql, params = dialect.format_data_type(data_type)
    assert sql == expected
    assert params == ()


def test_format_array_type(dialect):
    sql, _ = dialect.format_data_type(ArrayType(IntegerType(), dimensions=1))
    assert sql == "INTEGER[]"

    sql, _ = dialect.format_data_type(ArrayType(VarCharType(10), dimensions=3))
    assert sql == "VARCHAR(10)[][][]"


def test_format_array_type_multidimensional(dialect):
    sql, _ = dialect.format_data_type(ArrayType(IntegerType(), dimensions=2))
    assert sql == "INTEGER[][]"


def test_format_array_type_with_postgres_element(dialect):
    sql, _ = dialect.format_data_type(ArrayType(PostgresUUIDType(), dimensions=1))
    assert sql == "UUID[]"


def test_format_array_type_unsupported_element_raises(dialect):
    with pytest.raises(TypeError, match="is not supported"):
        dialect.format_data_type(ArrayType(CustomType("X"), dimensions=1))


def test_format_unregistered_type_raises(dialect):
    with pytest.raises(TypeError, match="does not support"):
        dialect.format_data_type(CustomType("X"))


# ---------------------------------------------------------------------------
# parse_type — round-trip families
# ---------------------------------------------------------------------------

PARSE_CASES = [
    ("SMALLINT", SmallIntType, {}),
    ("INT2", SmallIntType, {}),
    ("INT", IntegerType, {}),
    ("INTEGER", IntegerType, {}),
    ("INT4", IntegerType, {}),
    ("BIGINT", BigIntType, {}),
    ("INT8", BigIntType, {}),
    ("SMALLSERIAL", PostgresSmallSerialType, {}),
    ("SERIAL2", PostgresSmallSerialType, {}),
    ("SERIAL", PostgresSerialType, {}),
    ("SERIAL4", PostgresSerialType, {}),
    ("BIGSERIAL", PostgresBigSerialType, {}),
    ("SERIAL8", PostgresBigSerialType, {}),
    ("REAL", RealType, {}),
    ("FLOAT4", RealType, {}),
    ("DOUBLE", DoubleType, {}),
    ("DOUBLE PRECISION", DoubleType, {}),
    ("FLOAT", FloatType, {}),
    ("FLOAT(8)", FloatType, {"precision": 8}),
    ("DECIMAL", DecimalType, {}),
    ("NUMERIC", DecimalType, {}),
    ("DECIMAL(10)", DecimalType, {"precision": 10}),
    ("DECIMAL(10,2)", DecimalType, {"precision": 10, "scale": 2}),
    ("NUMERIC(12,4)", DecimalType, {"precision": 12, "scale": 4}),
    ("CHAR", CharType, {}),
    ("CHAR(5)", CharType, {"length": 5}),
    ("CHARACTER", CharType, {}),
    ("CHARACTER(5)", CharType, {"length": 5}),
    ("VARCHAR", VarCharType, {}),
    ("VARCHAR(50)", VarCharType, {"length": 50}),
    ("CHARACTER VARYING", VarCharType, {}),
    ("CHARACTER VARYING(100)", VarCharType, {"length": 100}),
    ("TEXT", TextType, {}),
    ("BYTEA", PostgresByteaType, {}),
    ("BLOB", PostgresByteaType, {}),
    ("DATE", DateType, {}),
    ("DATETIME", DateTimeType, {}),
    ("TIMESTAMP", TimestampType, {}),
    ("TIMESTAMP(3)", TimestampType, {"precision": 3}),
    ("TIMESTAMPTZ", TimestampTzType, {}),
    ("TIMESTAMP(3) WITH TIME ZONE", TimestampTzType, {"precision": 3}),
    ("TIME", TimeType, {}),
    ("TIME(6)", TimeType, {"precision": 6}),
    ("TIMETZ", TimeTzType, {}),
    ("TIME(6) WITH TIME ZONE", TimeTzType, {"precision": 6}),
    ("INTERVAL", IntervalType, {}),
    ("INTERVAL DAY TO SECOND", IntervalType, {"fields": "DAY TO SECOND"}),
    ("JSON", JsonType, {}),
    ("JSONB", JsonBType, {}),
    ("JSONPATH", PostgresJsonPathType, {}),
    ("UUID", PostgresUUIDType, {}),
    ("BOOLEAN", BooleanType, {}),
    ("BOOL", BooleanType, {}),
    ("BIT", PostgresBitType, {}),
    ("BIT(8)", PostgresBitType, {"n": 8}),
    ("VARBIT", PostgresVarBitType, {}),
    ("VARBIT(16)", PostgresVarBitType, {"n": 16}),
    ("INET", PostgresInetType, {}),
    ("CIDR", PostgresCidrType, {}),
    ("MACADDR", PostgresMacAddrType, {}),
    ("MACADDR8", PostgresMacAddr8Type, {}),
    ("POINT", PostgresPointType, {}),
    ("LINE", PostgresLineType, {}),
    ("LSEG", PostgresLineSegmentType, {}),
    ("BOX", PostgresBoxType, {}),
    ("PATH", PostgresPathType, {}),
    ("POLYGON", PostgresPolygonType, {}),
    ("CIRCLE", PostgresCircleType, {}),
    ("INT4RANGE", PostgresInt4RangeType, {}),
    ("INT8RANGE", PostgresInt8RangeType, {}),
    ("NUMRANGE", PostgresNumRangeType, {}),
    ("TSRANGE", PostgresTsRangeType, {}),
    ("TSTZRANGE", PostgresTsTzRangeType, {}),
    ("DATERANGE", PostgresDateRangeType, {}),
    ("INT4MULTIRANGE", PostgresInt4MultirangeType, {}),
    ("INT8MULTIRANGE", PostgresInt8MultirangeType, {}),
    ("NUMMULTIRANGE", PostgresNumMultirangeType, {}),
    ("TSMULTIRANGE", PostgresTsMultirangeType, {}),
    ("TSTZMULTIRANGE", PostgresTsTzMultirangeType, {}),
    ("DATEMULTIRANGE", PostgresDateMultirangeType, {}),
    ("OID", PostgresOIDType, {}),
    ("REGCLASS", PostgresRegClassType, {}),
    ("REGTYPE", PostgresRegTypeType, {}),
    ("XID", PostgresXIDType, {}),
    ("XID8", PostgresXID8Type, {}),
    ("CID", PostgresCIDType, {}),
    ("TID", PostgresTIDType, {}),
    ("TSVECTOR", PostgresTSVectorType, {}),
    ("TSQUERY", PostgresTSQueryType, {}),
    ("MONEY", PostgresMoneyType, {}),
    ("XML", PostgresXMLType, {}),
    ("PG_LSN", PostgresPgLSNType, {}),
    ("HSTORE", PostgresHstoreType, {}),
    ("GEOGRAPHY", PostgresGeographyType, {}),
    ("GEOMETRY", PostgresGeometryType, {}),
    ("VECTOR", PostgresVectorType, {"dim": 0}),
    ("VECTOR(768)", PostgresVectorType, {"dim": 768}),
]


@pytest.mark.parametrize("raw,expected_cls,expected_attrs", PARSE_CASES)
def test_parse_type(dialect, raw, expected_cls, expected_attrs):
    result = dialect.parse_type(raw)
    assert type(result) is expected_cls
    for attr, value in expected_attrs.items():
        assert getattr(result, attr) == value


def test_parse_type_array_bracket_suffix(dialect):
    result = dialect.parse_type("INTEGER[]")
    assert type(result) is PostgresArrayType
    assert type(result.element_type) is IntegerType
    assert result.dimensions == 1


def test_parse_type_array_multidimensional_brackets(dialect):
    result = dialect.parse_type("INTEGER[][]")
    assert type(result) is PostgresArrayType
    assert type(result.element_type) is IntegerType


def test_parse_type_array_with_dimension_bracket(dialect):
    result = dialect.parse_type("VARCHAR(10)[3]")
    assert type(result) is PostgresArrayType
    assert type(result.element_type) is VarCharType
    assert result.element_type.length == 10


def test_parse_type_malformed_bracket_suffix_falls_back(dialect):
    result = dialect.parse_type("FOO[abc]")
    assert type(result) is CustomType


def test_parse_type_array_keyword(dialect):
    result = dialect.parse_type("INTEGER ARRAY")
    assert type(result) is PostgresArrayType
    assert type(result.element_type) is IntegerType


def test_parse_type_array_keyword_with_dimension(dialect):
    result = dialect.parse_type("INTEGER ARRAY[3]")
    assert type(result) is PostgresArrayType
    assert type(result.element_type) is IntegerType


def test_parse_type_array_nested_element(dialect):
    result = dialect.parse_type("UUID[]")
    assert type(result) is PostgresArrayType
    assert type(result.element_type) is PostgresUUIDType


def test_parse_type_unknown_falls_back_to_custom(dialect):
    result = dialect.parse_type("FOOBARBAZ")
    assert type(result) is CustomType
    assert result.raw == "FOOBARBAZ"


def test_parse_type_case_insensitive(dialect):
    result = dialect.parse_type("  varchar(25)  ")
    assert type(result) is VarCharType
    assert result.length == 25


# ---------------------------------------------------------------------------
# supports_data_types
# ---------------------------------------------------------------------------


def test_supports_data_types_registers_all_postgres_types(dialect):
    supported = dialect.supports_data_types()
    assert isinstance(supported, list)
    assert len(supported) > 0
    classes = {cls for cls, _ in supported}
    for expected in (
        PostgresSerialType,
        PostgresByteaType,
        PostgresUUIDType,
        PostgresVectorType,
        PostgresTsRangeType,
        ArrayType,
        JsonBType,
        IntegerType,
    ):
        assert expected in classes
