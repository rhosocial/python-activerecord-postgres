# tests/rhosocial/activerecord_postgres_test/feature/backend/types/test_postgres_type_protocol.py
"""Tests for the formalised core DataType protocol conformance.

Verifies:
- format family == supports family (1:1 correspondence)
- supports_data_types() includes postgres_* + core entries
- suggested_data_types(): values are DataType classes; keys disjoint from supported
- dialect_options forwarding and equality
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import (
    UnsupportedFeatureError,
)
from rhosocial.activerecord.backend.expression.types import (
    BigIntType,
    BlobType,
    BooleanType,
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    JsonBType,
    JsonType,
    SmallIntType,
    TextType,
    TimeType,
    TimestampType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresBitType,
    PostgresByteaType,
    PostgresHalfvecType,
    PostgresInetType,
    PostgresJsonPathType,
    PostgresPointType,
    PostgresSerialType,
    PostgresSparsevecType,
    PostgresTsRangeType,
    PostgresUUIDType,
    PostgresVarBitType,
    PostgresVectorType,
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
    assert callable(dialect.format_data_type_postgres_enum)


def test_supports_data_type_postgres_enum_exists(dialect):
    """supports_data_type_postgres_enum method exists on the dialect."""
    assert hasattr(dialect, "supports_data_type_postgres_enum")
    assert callable(dialect.supports_data_type_postgres_enum)
    assert dialect.supports_data_type_postgres_enum() is True


def test_postgres_enum_expression_renders_via_format_enum_type_expression(dialect):
    """PostgresEnumType (BaseExpression) renders via format_enum_type_expression."""
    from rhosocial.activerecord.backend.impl.postgres.expression.enum_ import PostgresEnumType

    enum_ref = PostgresEnumType(
        dialect=dialect,
        name="video_status",
        values=["pending", "processing", "ready"],
    )
    sql, params = dialect.format_enum_type_expression(enum_ref)
    assert sql == '"video_status"'
    assert params == ()


def test_postgres_enum_expression_with_schema_renders_via_dialect(dialect):
    from rhosocial.activerecord.backend.impl.postgres.expression.enum_ import PostgresEnumType

    enum_ref = PostgresEnumType(
        dialect=dialect,
        name="video_status",
        values=["pending", "processing"],
        schema="app",
    )
    sql, params = dialect.format_enum_type_expression(enum_ref)
    assert sql == '"app"."video_status"'
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
# W1: type-param equality (dialect_options bag removed)
# ---------------------------------------------------------------------------


def test_type_constructor_rejects_dialect_options():
    with pytest.raises(TypeError):
        PostgresBitType(n=8, dialect_options={"flag": True})












# ---------------------------------------------------------------------------
# W1: PARAMETERS-based equality replaces hand-written __eq__/__hash__
# ---------------------------------------------------------------------------


def test_bit_type_identity():
    t1 = PostgresBitType(n=8)
    t2 = PostgresBitType(n=8)
    t3 = PostgresBitType(n=16)
    t4 = PostgresBitType()

    assert t1 == t2
    assert t1 != t3
    assert t1 != t4
    assert hash(t1) == hash(t2)
    assert hash(t1) != hash(t3)


def test_catalog_bit_varying_spelling_parses_as_varbit(dialect):
    """``bit varying(n)`` -- the live catalog word -- is the varbit class.

    Live evidence (report appendix F1, measured 2026-10-08 on PostgreSQL
    9.6/13/16/19beta4): ``pg_catalog.format_type(atttypid, atttypmod)`` for a
    ``bit varying(16)`` column is exactly ``bit varying(16)``.  Parsing used to
    answer ``PostgresBitType`` because the fixed-length prefix test ran first,
    so introspection turned every varbit column into a bit column of the same
    width.  The rendered-string sweep cannot see this: it only feeds back the
    renderer's own ``VARBIT(16)`` spelling.
    """
    varying = dialect.parse_type("bit varying(16)")
    assert varying == PostgresVarBitType(dialect, n=16)
    assert varying != PostgresBitType(dialect, n=16)

    bare = dialect.parse_type("bit varying")
    assert bare == PostgresVarBitType(dialect)
    assert bare != PostgresBitType(dialect)

    # Keep-pins for the words the renderer emits.
    assert dialect.parse_type("BIT(8)") == PostgresBitType(dialect, n=8)
    assert dialect.parse_type("VARBIT(16)") == PostgresVarBitType(dialect, n=16)


def test_vector_type_identity():
    t1 = PostgresVectorType(dim=384)
    t2 = PostgresVectorType(dim=384)
    t3 = PostgresVectorType(dim=768)

    assert t1 == t2
    assert t1 != t3
    assert hash(t1) == hash(t2)
    assert hash(t1) != hash(t3)


def test_vector_types_cross_class_inequality():
    """Different vector types with same dim are not equal."""
    assert PostgresVectorType(dim=3) != PostgresHalfvecType(dim=3)
    assert PostgresVectorType(dim=3) != PostgresSparsevecType(dim=3)
    assert PostgresHalfvecType(dim=3) != PostgresSparsevecType(dim=3)


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
# Signedness on the floating-point and exact fixed-point concepts
# ---------------------------------------------------------------------------

#: The three concepts that carry ``unsigned`` besides the four integer widths,
#: with the PostgreSQL type each renders when the flag is left alone and the
#: constructor arguments each needs to reach its precision-bearing form.
#:
#: ``DECIMAL`` and ``FLOAT`` are here with their precision/scale because those are
#: fields too, and the point of the table is that refusing ``unsigned`` does not
#: disturb them: every signed rendering below is byte-for-byte what it was.
NUMERIC_SIGNEDNESS = [
    (DecimalType, {}, "DECIMAL"),
    (DecimalType, {"precision": 10, "scale": 2}, "DECIMAL(10,2)"),
    (FloatType, {}, "REAL"),
    (FloatType, {"precision": 24}, "FLOAT(24)"),
    (DoubleType, {}, "DOUBLE PRECISION"),
    (DoubleType, {"spelling": "double precision"}, "DOUBLE PRECISION"),
]

NUMERIC_SIGNEDNESS_IDS = [
    f"{k.__name__}{''.join(sorted(kw)) or '-bare'}" for k, kw, _s
    in NUMERIC_SIGNEDNESS
]


@pytest.mark.parametrize("klass,kwargs,signed_sql", NUMERIC_SIGNEDNESS,
                         ids=NUMERIC_SIGNEDNESS_IDS)
def test_the_signed_rendering_is_unchanged(dialect, klass, kwargs, signed_sql):
    """The refusal is not paid for by moving a signed rendering."""
    assert dialect.format_data_type(klass(dialect, **kwargs)) == (signed_sql, ())
    assert dialect.format_data_type(
        klass(dialect, unsigned=False, **kwargs)) == (signed_sql, ())


@pytest.mark.parametrize("klass,kwargs,signed_sql", NUMERIC_SIGNEDNESS,
                         ids=NUMERIC_SIGNEDNESS_IDS)
def test_unsigned_is_refused_not_rendered_signed(dialect, klass, kwargs,
                                                 signed_sql):
    """The defect this answers: flipping the field used to change nothing.

    ``DecimalType(unsigned=True)``, ``FloatType(unsigned=True)`` and
    ``DoubleType(unsigned=True)`` each rendered byte-identical SQL to the signed
    declaration, so a caller got a column that accepts the negatives it declared
    it would not, and the call reported success.

    PostgreSQL's answer is a refusal. Its Table 8.2 lists ``decimal``/``numeric``,
    ``real`` and ``double precision`` with no unsigned variant; ``real`` and
    ``double precision`` are IEEE 754 binary formats whose documented ranges are
    signed on both sides; ``numeric``'s own special values include
    ``-Infinity``; and ``CREATE TABLE`` has no attribute slot after the type name
    in which ``UNSIGNED`` could go.
    https://www.postgresql.org/docs/current/datatype-numeric.html
    """
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        dialect.format_data_type(klass(dialect, unsigned=True, **kwargs))
    message = str(excinfo.value)
    assert "unsigned" in message, message
    assert excinfo.value.suggestion, "the refusal must carry a route forward"
    assert "CHECK" in excinfo.value.suggestion


@pytest.mark.parametrize("klass,kwargs,signed_sql", NUMERIC_SIGNEDNESS,
                         ids=NUMERIC_SIGNEDNESS_IDS)
def test_signedness_is_part_of_identity(dialect, klass, kwargs, signed_sql):
    """Why refusing is right: the two are different columns to the differ.

    Refusing at render time must not flatten the model to make the refusal
    easier -- the flag is in ``PARAMETERS`` and reaches ``identity()``.
    """
    assert "unsigned" in klass.PARAMETERS, klass.__name__
    signed = klass(dialect, **kwargs)
    unsigned = klass(dialect, unsigned=True, **kwargs)
    assert signed != unsigned
    assert hash(signed) != hash(unsigned)
    assert klass(dialect, **kwargs) == signed


def test_unsigned_is_type_checked(dialect):
    """It becomes a type modifier in the rendered DDL, so ``1`` is not a
    truthy ``True`` and ``"yes"`` is not a spelling of one."""
    for klass, kwargs in ((DecimalType, {}), (FloatType, {}), (DoubleType, {})):
        with pytest.raises(TypeError, match="unsigned must be a bool"):
            klass(dialect, unsigned=1, **kwargs)
        with pytest.raises(TypeError, match="unsigned must be a bool"):
            klass(dialect, unsigned="yes", **kwargs)


def test_the_scale_and_precision_checks_are_untouched_by_the_new_refusal(
        dialect):
    """A new failure path must not have swallowed the old answers.

    ``DECIMAL`` still refuses a bare scale with ``UnsupportedFeatureError`` and
    still raises ``ValueError`` for an out-of-range precision, and ``FLOAT``
    still raises ``ValueError`` for an out-of-range precision. The unsigned
    refusal is checked first, so it is what a request that got *both* wrong is
    told about -- but a signed request is unaffected.
    """
    with pytest.raises(UnsupportedFeatureError) as bare_scale:
        dialect.format_data_type(DecimalType(dialect, scale=2))
    assert "scale" in str(bare_scale.value)

    with pytest.raises(ValueError) as precision:
        dialect.format_data_type(DecimalType(dialect, precision=1001))
    assert not isinstance(precision.value, UnsupportedFeatureError)

    with pytest.raises(ValueError) as float_precision:
        dialect.format_data_type(FloatType(dialect, precision=54))
    assert not isinstance(float_precision.value, UnsupportedFeatureError)

    # Both faults at once: the flag is named, because it is checked first.
    with pytest.raises(UnsupportedFeatureError) as both:
        dialect.format_data_type(
            DecimalType(dialect, scale=2, unsigned=True))
    assert "unsigned" in str(both.value)


def test_an_unsigned_request_is_answered_the_same_way_for_every_concept(
        dialect):
    """One exception type for one refusal, so portable code can catch it.

    ``UnsupportedFeatureError`` does not subclass ``ValueError``. The project's
    rule is that a *wrong value* raises ``ValueError`` and a declaration the
    grammar cannot express at all raises ``UnsupportedFeatureError``, and every
    backend answers this identical refusal the same way -- which is what lets a
    caller write one ``except`` clause for it.
    """
    seen = 0
    for klass, kwargs, _sql in NUMERIC_SIGNEDNESS:
        try:
            dialect.format_data_type(klass(dialect, unsigned=True, **kwargs))
        except UnsupportedFeatureError:
            seen += 1
        except ValueError as exc:                     # pragma: no cover
            pytest.fail(
                f"refused an unsigned {klass.__name__} with ValueError; every "
                f"other backend raises UnsupportedFeatureError for the same "
                f"refusal, and the two do not share a base class: {exc}"
            )
    assert seen == len(NUMERIC_SIGNEDNESS)


def test_the_catalog_can_never_report_the_attribute_so_parse_type_needs_nothing(
        dialect):
    """Why ``parse_type`` is untouched here, unlike MySQL's and MariaDB's.

    On those two the catalog hands ``parse_type`` a ``COLUMN_TYPE`` that really
    can contain ``unsigned``, so it has to be read or a changed column compares
    equal to its own declaration. PostgreSQL's grammar has no such attribute, so
    the word cannot appear in a ``format_type`` result and there is nothing to
    read: inventing a branch for a string this backend never produces would be a
    guess dressed as a fix. The rendering is still canonical, which is the half
    of D8 that does apply.
    """
    assert dialect.format_data_type(DecimalType(dialect, 10, 2)) == (
        "DECIMAL(10,2)", ())
    assert "UNSIGNED" not in dialect.format_data_type(
        DecimalType(dialect, 10, 2))[0]
    # ...and what it does render, it parses back to the same value.
    for klass, kwargs, sql in NUMERIC_SIGNEDNESS:
        if klass is FloatType and not kwargs:
            # Bare FLOAT renders REAL on this backend, which is RealType's word:
            # a pre-existing spelling asymmetry, not an unsigned one.
            continue
        assert dialect.parse_type(sql) == klass(dialect, **kwargs), sql


# ---------------------------------------------------------------------------
# PostgresEnumType not a DataType — protocol boundary
# ---------------------------------------------------------------------------


def test_postgres_enum_type_is_not_data_type():
    """PostgresEnumType extends BaseExpression, not DataType."""
    from rhosocial.activerecord.backend.expression.bases import BaseExpression
    from rhosocial.activerecord.backend.impl.postgres.expression.enum_ import PostgresEnumType

    assert not issubclass(PostgresEnumType, DataType)
    assert issubclass(PostgresEnumType, BaseExpression)


# ---------------------------------------------------------------------------
# parse_type: the shared round-trip sweep over the whole declared surface


class TestParseRoundTripSweep:
    """Every rendered type parses back coherently, in one assertion each way.

    The render-parse-re-render sweep over this backend's own registry found
    six words it wrote and could not read back -- CITEXT, CUBE, LTREE,
    LQUERY, LTXTQUERY and RASTER, all extension types this dialect renders
    under its own ``format_data_type_postgres_*`` formatters -- and each
    answered ``CustomType``, so a column of one of them introspected as a
    type the backend does not model. The branches now sit where HSTORE and
    GEOMETRY already sat, ungated for the same reason: the word can only
    have come from a server where the extension exists.

    The sweep asserts the two invariants everywhere: **string stability** --
    parsing a rendering and re-rendering the answer produces the identical
    string -- and **class honesty** -- the answer is the declared
    instance, or the documented widening answer recorded below with its
    reason.
    """

    #: The documented widening answers. ``tinyint`` has no 1-byte storage
    #: here, so the concept is widened to the 2-byte one the tinyint
    #: formatter names; bare ``FLOAT`` renders ``REAL``, which is the
    #: RealType concept; the ``datetime`` concept renders ``TIMESTAMP``,
    #: which is the storage both it and the timestamp concept share; and
    #: the bytea/uuid/xml/array renderings are the backend's own classes
    #: for those storages.
    WIDENING_ANSWERS = {
        "ArrayType": "PostgresArrayType",
        "BlobType": "PostgresByteaType",
        "DateTimeType": "TimestampType",
        "TinyIntType": "SmallIntType",
        "FloatType": "RealType",
        "UUIDType": "PostgresUUIDType",
        "XmlType": "PostgresXMLType",
    }

    @pytest.fixture(scope="class")
    def registry(self):
        """The round-trip module's registry, loaded from this directory's
        parent. Executing it also registers its special constructors, which
        the sweep's ``make_instance`` consults -- the same registrations the
        full suite performs at collection time, repeated here so the sweep
        also holds when this file runs alone.
        """
        import importlib.util
        from pathlib import Path

        path = Path(__file__).resolve().parent.parent / (
            "test_expression_roundtrip_all.py"
        )
        spec = importlib.util.spec_from_file_location("postgres_rt_registry", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        registry = getattr(module, "ALL_CLASSES", None) or module.REGISTERED
        assert registry, "the round-trip module exposes no registry"
        return registry

    def test_every_rendered_type_parses_back_coherently(self, dialect, registry):
        from rhosocial.activerecord.testsuite.utils.parse_contract import (
            parse_roundtrip_failures,
        )

        failures = parse_roundtrip_failures(
            dialect,
            registry,
            widening=self.WIDENING_ANSWERS,
        )
        assert failures == [], "\n".join(failures)
