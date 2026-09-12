# tests/rhosocial/activerecord_postgres_test/feature/backend/types/test_postgres_type_protocol.py
"""Tests for the formalised core DataType protocol conformance.

Verifies:
- format family == supports family (1:1 correspondence)
- supports_data_types() includes postgres_* + core entries
- suggested_data_types(): values are DataType classes; keys disjoint from supported
- dialect_options forwarding and equality
"""

import pytest

from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntType,
    JsonBType,
    JsonType,
    RealType,
    SmallIntType,
    TextType,
    TimeType,
    TimestampType,
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
    PostgresHalfvecType,
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
    PostgresSparsevecType,
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
    return PostgresDialect(version=(16, 0, 0))


# ---------------------------------------------------------------------------
# W2: format_data_type_postgres_enum / supports_data_type_postgres_enum pair
# ---------------------------------------------------------------------------


def test_format_data_type_postgres_enum_exists(dialect):
    """format_data_type_postgres_enum method exists on the dialect."""
    assert hasattr(dialect, "format_data_type_postgres_enum")
    assert callable(getattr(dialect, "format_data_type_postgres_enum"))


def test_supports_data_type_postgres_enum_exists(dialect):
    """supports_data_type_postgres_enum method exists on the dialect."""
    assert hasattr(dialect, "supports_data_type_postgres_enum")
    assert callable(getattr(dialect, "supports_data_type_postgres_enum"))
    assert dialect.supports_data_type_postgres_enum() is True


def test_postgres_enum_expression_renders_via_format_enum_type_expression(dialect):
    """PostgresEnumType (BaseExpression) renders via format_enum_type_expression."""
    from rhosocial.activerecord.backend.impl.postgres.types.enum import PostgresEnumType

    enum_ref = PostgresEnumType(
        dialect=dialect,
        name="video_status",
        values=["pending", "processing", "ready"],
    )
    sql, params = dialect.format_enum_type_expression(enum_ref)
    assert sql == "video_status"
    assert params == ()


def test_postgres_enum_expression_with_schema_renders_via_dialect(dialect):
    from rhosocial.activerecord.backend.impl.postgres.types.enum import PostgresEnumType

    enum_ref = PostgresEnumType(
        dialect=dialect,
        name="video_status",
        values=["pending", "processing"],
        schema="app",
    )
    sql, params = dialect.format_enum_type_expression(enum_ref)
    assert sql == "app.video_status"
    assert params == ()


# ---------------------------------------------------------------------------
# W3: format family == supports family (1:1)
# ---------------------------------------------------------------------------

# Collect all format_data_type_* and supports_data_type_* names from the dialect
# and verify they match exactly.


def _get_method_suffixes(dialect, prefix):
    """Extract method name suffixes matching ``prefix_<name>``."""
    import re
    pattern = re.compile(rf"^{prefix}([a-z][a-z0-9_]*)$")
    suffixes = set()
    for member_name in dir(type(dialect)):
        match = pattern.match(member_name)
        if match:
            suffixes.add(match.group(1))
    return suffixes


def test_format_supports_family_1to1(dialect):
    """Every format_data_type_<name> must have supports_data_type_<name> and vice versa."""
    format_names = _get_method_suffixes(dialect, "format_data_type_")
    supports_names = _get_method_suffixes(dialect, "supports_data_type_")

    # Remove non-dispatch names (these are helpers, not type names)
    format_names.discard("custom")
    supports_names.discard("custom")
    # supports_data_types and format_data_type are the entry points
    format_names.discard("")
    supports_names.discard("")

    missing_supports = format_names - supports_names
    missing_format = supports_names - format_names

    assert not missing_supports, (
        f"format_data_type_ methods without corresponding supports_data_type_: "
        f"{sorted(missing_supports)}"
    )
    assert not missing_format, (
        f"supports_data_type_ methods without corresponding format_data_type_: "
        f"{sorted(missing_format)}"
    )


# ---------------------------------------------------------------------------
# W3: supports_data_types() includes postgres_* + core entries
# ---------------------------------------------------------------------------


def test_supports_data_types_includes_postgres_types(dialect):
    supported = dialect.supports_data_types()
    assert isinstance(supported, dict)
    assert len(supported) > 0

    # Verify a representative sample of postgres-specific types
    postgres_expected = {
        "postgres_bytea": PostgresByteaType,
        "postgres_serial": PostgresSerialType,
        "postgres_uuid": PostgresUUIDType,
        "postgres_vector": PostgresVectorType,
        "postgres_tsrange": PostgresTsRangeType,
        "postgres_inet": PostgresInetType,
        "postgres_point": PostgresPointType,
        "postgres_jsonpath": PostgresJsonPathType,
    }
    for name, cls in postgres_expected.items():
        assert name in supported, f"Missing postgres type: {name}"
        assert supported[name] is cls, (
            f"Type {name}: expected {cls.__name__}, got {supported[name].__name__}"
        )


def test_supports_data_types_includes_core_types(dialect):
    supported = dialect.supports_data_types()

    core_expected = {
        "integer": IntegerType,
        "bigint": BigIntType,
        "smallint": SmallIntType,
        "decimal": DecimalType,
        "varchar": VarCharType,
        "text": TextType,
        "boolean": BooleanType,
        "blob": BlobType,
        "date": None,  # DateType
        "timestamp": None,  # TimestampType
        "json": JsonType,
        "jsonb": JsonBType,
    }
    for name in core_expected:
        assert name in supported, f"Missing core type: {name}"


def test_supports_data_types_values_are_data_type_classes(dialect):
    supported = dialect.supports_data_types()
    for name, cls in supported.items():
        assert isinstance(cls, type), (
            f"supports_data_types()[{name!r}] is not a class: {cls!r}"
        )
        assert issubclass(cls, DataType), (
            f"supports_data_types()[{name!r}] = {cls.__name__} is not a DataType subclass"
        )


# ---------------------------------------------------------------------------
# W4: suggested_data_types()
# ---------------------------------------------------------------------------


def test_suggested_data_types_returns_dict(dialect):
    result = dialect.suggested_data_types()
    assert isinstance(result, dict)


def test_suggested_data_types_values_are_classes(dialect):
    result = dialect.suggested_data_types()
    for name, cls in result.items():
        assert isinstance(cls, type), (
            f"suggested_data_types()[{name!r}] is not a class: {cls!r}"
        )
        assert issubclass(cls, DataType), (
            f"suggested_data_types()[{name!r}] = {cls.__name__} is not a DataType subclass"
        )


def test_suggested_data_types_keys_disjoint_from_supported(dialect):
    supported = set(dialect.supports_data_types().keys())
    suggested = set(dialect.suggested_data_types().keys())
    overlap = supported & suggested
    assert not overlap, (
        f"suggested_data_types() keys overlap with supports_data_types(): {overlap}"
    )


# ---------------------------------------------------------------------------
# W1: dialect_options forwarding and equality
# ---------------------------------------------------------------------------


def test_bit_type_dialect_options_forwarded():
    t1 = PostgresBitType(n=8, dialect_options={"flag": True})
    t2 = PostgresBitType(n=8, dialect_options={"flag": True})
    t3 = PostgresBitType(n=8, dialect_options={"flag": False})
    t4 = PostgresBitType(n=8)

    # Same options → equal
    assert t1 == t2
    assert hash(t1) == hash(t2)

    # Different options → not equal
    assert t1 != t3
    assert t1 != t4


def test_vector_type_dialect_options_forwarded():
    t1 = PostgresVectorType(dim=384, dialect_options={"metric": "cosine"})
    t2 = PostgresVectorType(dim=384, dialect_options={"metric": "cosine"})
    t3 = PostgresVectorType(dim=384, dialect_options={"metric": "l2"})

    assert t1 == t2
    assert hash(t1) == hash(t2)
    assert t1 != t3


def test_halfvec_type_dialect_options_forwarded():
    t1 = PostgresHalfvecType(dim=128, dialect_options={"quantize": True})
    t2 = PostgresHalfvecType(dim=128, dialect_options={"quantize": True})
    t3 = PostgresHalfvecType(dim=128)

    assert t1 == t2
    assert hash(t1) == hash(t2)
    assert t1 != t3


def test_sparsevec_type_dialect_options_forwarded():
    t1 = PostgresSparsevecType(dim=16, dialect_options={"sparse": True})
    t2 = PostgresSparsevecType(dim=16, dialect_options={"sparse": True})
    t3 = PostgresSparsevecType(dim=16)

    assert t1 == t2
    assert hash(t1) == hash(t2)
    assert t1 != t3


def test_varbit_type_dialect_options_forwarded():
    t1 = PostgresVarBitType(n=16, dialect_options={"strict": True})
    t2 = PostgresVarBitType(n=16, dialect_options={"strict": True})
    t3 = PostgresVarBitType(n=16)

    assert t1 == t2
    assert hash(t1) == hash(t2)
    assert t1 != t3


# ---------------------------------------------------------------------------
# W1: _type_params() based equality replaces hand-written __eq__/__hash__
# ---------------------------------------------------------------------------


def test_bit_type_type_params():
    t1 = PostgresBitType(n=8)
    t2 = PostgresBitType(n=8)
    t3 = PostgresBitType(n=16)
    t4 = PostgresBitType()

    assert t1 == t2
    assert t1 != t3
    assert t1 != t4
    assert hash(t1) == hash(t2)
    assert hash(t1) != hash(t3)


def test_vector_type_type_params():
    t1 = PostgresVectorType(dim=384)
    t2 = PostgresVectorType(dim=384)
    t3 = PostgresVectorType(dim=768)

    assert t1 == t2
    assert t1 != t3
    assert hash(t1) == hash(t2)
    assert hash(t1) != hash(t3)


def test_vector_types_cross_class_inequality():
    """Different vector types with same dim are not equal."""
    assert PostgresVectorType(3) != PostgresHalfvecType(3)
    assert PostgresVectorType(3) != PostgresSparsevecType(3)
    assert PostgresHalfvecType(3) != PostgresSparsevecType(3)


# ---------------------------------------------------------------------------
# W5: Precision validation
# ---------------------------------------------------------------------------


def test_decimal_precision_validation(dialect):
    """PostgreSQL allows DECIMAL precision up to 1000."""
    # Valid
    sql, _ = dialect.format_data_type(DecimalType(precision=1000, scale=0))
    assert sql == "DECIMAL(1000,0)"

    # Invalid: precision 0
    with pytest.raises(ValueError, match="precision must be between 1 and 1000"):
        dialect.format_data_type(DecimalType(precision=0))

    # Invalid: precision > 1000
    with pytest.raises(ValueError, match="precision must be between 1 and 1000"):
        dialect.format_data_type(DecimalType(precision=1001))


def test_float_precision_validation(dialect):
    """PostgreSQL allows FLOAT precision up to 53."""
    # Valid
    sql, _ = dialect.format_data_type(FloatType(precision=53))
    assert sql == "FLOAT(53)"

    # Invalid: precision 0
    with pytest.raises(ValueError, match="precision must be between 1 and 53"):
        dialect.format_data_type(FloatType(precision=0))

    # Invalid: precision > 53
    with pytest.raises(ValueError, match="precision must be between 1 and 53"):
        dialect.format_data_type(FloatType(precision=54))


def test_timestamp_precision_validation(dialect):
    """PostgreSQL allows TIMESTAMP precision 0-6."""
    # Valid
    sql, _ = dialect.format_data_type(TimestampType(precision=6))
    assert sql == "TIMESTAMP(6)"

    # Invalid: precision -1
    with pytest.raises(ValueError, match="precision must be between 0 and 6"):
        dialect.format_data_type(TimestampType(precision=-1))

    # Invalid: precision 7
    with pytest.raises(ValueError, match="precision must be between 0 and 6"):
        dialect.format_data_type(TimestampType(precision=7))


def test_time_precision_validation(dialect):
    """PostgreSQL allows TIME precision 0-6."""
    # Valid
    sql, _ = dialect.format_data_type(TimeType(precision=6))
    assert sql == "TIME(6)"

    # Invalid: precision 7
    with pytest.raises(ValueError, match="precision must be between 0 and 6"):
        dialect.format_data_type(TimeType(precision=7))


# ---------------------------------------------------------------------------
# PostgresEnumType not a DataType — protocol boundary
# ---------------------------------------------------------------------------


def test_postgres_enum_type_is_not_data_type():
    """PostgresEnumType extends BaseExpression, not DataType."""
    from rhosocial.activerecord.backend.expression.bases import BaseExpression
    from rhosocial.activerecord.backend.impl.postgres.types.enum import PostgresEnumType

    assert not issubclass(PostgresEnumType, DataType)
    assert issubclass(PostgresEnumType, BaseExpression)
