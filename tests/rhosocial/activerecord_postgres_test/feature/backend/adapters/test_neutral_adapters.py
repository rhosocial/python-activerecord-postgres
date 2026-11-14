# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/test_neutral_adapters.py
"""DB-free contract tests for the PostgreSQL neutral value adapters.

Every adapter here works on values only: no connection is opened and no server
is needed.  What is pinned is the wire form (WKT/EWKT, pgvector text, bit text,
range literals and MAC text), the read path from the wrapper types and psycopg
objects, the ``TypeConversionError`` wrapping, and the catalogue inventory.
"""

import math
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.adapters import (
    BitString,
    Geometry,
    GeometryCollection,
    LineString,
    MacAddress,
    Multirange,
    Point,
    Polygon,
    Range,
    Vector,
)
from rhosocial.activerecord.backend.errors import TypeConversionError
from rhosocial.activerecord.backend.impl.postgres.adapters.neutral import (
    PostgresNeutralBitStringAdapter,
    PostgresNeutralGeometryAdapter,
    PostgresNeutralMacAddressAdapter,
    PostgresNeutralRangeAdapter,
    PostgresNeutralVectorAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.backend import (
    AsyncPostgresBackend,
    PostgresBackend,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    PostgresBitString,
    PostgresGeometry,
    PostgresMacaddr,
    PostgresMacaddr8,
    PostgresMultirange,
    PostgresRange,
    PostgresVector,
)

_NEUTRAL_CLASSES = (
    PostgresNeutralBitStringAdapter,
    PostgresNeutralGeometryAdapter,
    PostgresNeutralMacAddressAdapter,
    PostgresNeutralRangeAdapter,
    PostgresNeutralVectorAdapter,
)


@pytest.fixture(params=[PostgresBackend, AsyncPostgresBackend])
def backend(request):
    """A constructed backend: neither branch opens a connection."""
    return request.param(database="neutral_adapter_unit")


def _class_key(adapter):
    adapter_class = type(adapter)
    return (adapter_class.__module__, adapter_class.__qualname__)


class TestInventory:
    def test_every_neutral_adapter_is_in_the_catalogue(self, backend):
        names = {type(adapter).__name__ for adapter in backend.get_type_adapters()}
        for adapter_class in _NEUTRAL_CLASSES:
            assert adapter_class.__name__ in names

    def test_neutral_classes_appear_once(self, backend):
        keys = [_class_key(adapter) for adapter in backend.get_type_adapters()]
        assert len(keys) == len(set(keys))

    def test_inventory_is_deterministic(self, backend):
        first = [_class_key(adapter) for adapter in backend.get_type_adapters()]
        second = [_class_key(adapter) for adapter in backend.get_type_adapters()]
        assert first == second
        assert first == sorted(first)


class TestGeometryAdapter:
    def test_point_round_trip_without_srid(self):
        adapter = PostgresNeutralGeometryAdapter()
        point = Point(1, 2)
        text = adapter.to_database(point, str)
        assert text == "POINT (1.0 2.0)"
        assert adapter.from_database(text, Geometry) == point

    def test_point_round_trip_with_ewkt_srid(self):
        adapter = PostgresNeutralGeometryAdapter()
        point = Point(1, 2, srid=4326)
        text = adapter.to_database(point, str)
        assert text == "SRID=4326;POINT (1.0 2.0)"
        decoded = adapter.from_database(text, Geometry)
        assert decoded == point
        assert decoded.srid == 4326

    def test_srid_option_overrides_the_geometry(self):
        adapter = PostgresNeutralGeometryAdapter()
        assert adapter.to_database(Point(1, 2), str, {"srid": 3857}).startswith("SRID=3857;")

    def test_polygon_and_collection_round_trip(self):
        adapter = PostgresNeutralGeometryAdapter()
        polygon = Polygon((((0, 0), (10, 0), (10, 10), (0, 0)),), srid=4326)
        assert adapter.from_database(adapter.to_database(polygon, str), Geometry) == polygon
        collection = GeometryCollection((Point(1, 2), LineString(((0, 0), (1, 1)))))
        assert adapter.from_database(adapter.to_database(collection, str), Geometry) == collection

    def test_decodes_the_backend_wrapper(self):
        adapter = PostgresNeutralGeometryAdapter()
        wrapper = PostgresGeometry.point(1.0, 2.0, srid=4326)
        decoded = adapter.from_database(wrapper, Geometry)
        assert decoded == Point(1.0, 2.0, srid=4326)

    def test_hex_ewkb_is_rejected_instead_of_guessed(self):
        adapter = PostgresNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError) as excinfo:
            adapter.from_database("0101000020E6100000000000000000F03F", Geometry)
        assert "EWKB" in str(excinfo.value)

    def test_wrong_target_type_is_wrapped(self):
        adapter = PostgresNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError) as excinfo:
            adapter.to_database(Point(1, 2), bytes)
        assert "Cannot convert Point to Geometry" in str(excinfo.value)

    def test_invalid_wkt_is_wrapped(self):
        adapter = PostgresNeutralGeometryAdapter()
        with pytest.raises(TypeConversionError):
            adapter.to_database("POINT (1 2", str)


class TestVectorAdapter:
    def test_text_round_trip(self):
        adapter = PostgresNeutralVectorAdapter()
        vector = Vector((1, 2.5, -3))
        assert adapter.to_database(vector, str) == "[1,2.5,-3]"
        assert adapter.from_database("[1,2.5,-3]", Vector) == vector

    def test_decodes_a_plain_sequence_and_the_wrapper(self):
        adapter = PostgresNeutralVectorAdapter()
        assert adapter.from_database([1.0, 2.0], Vector) == Vector((1.0, 2.0))
        assert adapter.from_database(PostgresVector([1.0, 2.0]), Vector) == Vector((1.0, 2.0))

    def test_non_finite_components_use_the_pgvector_spellings(self):
        adapter = PostgresNeutralVectorAdapter()
        infinite = Vector((float("inf"), float("-inf")), allow_non_finite=True)
        assert adapter.to_database(infinite, str) == "[Infinity,-Infinity]"
        assert adapter.from_database("[Infinity,-Infinity]", Vector) == infinite

        nan_vector = Vector((float("nan"),), allow_non_finite=True)
        assert adapter.to_database(nan_vector, str) == "[NaN]"
        decoded = adapter.from_database("[NaN]", Vector)
        assert len(decoded.values) == 1
        assert math.isnan(decoded.values[0])

    def test_malformed_text_is_wrapped(self):
        adapter = PostgresNeutralVectorAdapter()
        for value in ("1,2", "[]", "[1,]", "[a]"):
            with pytest.raises(TypeConversionError):
                adapter.from_database(value, Vector)

    def test_wrong_target_type_is_wrapped(self):
        adapter = PostgresNeutralVectorAdapter()
        with pytest.raises(TypeConversionError):
            adapter.to_database(Vector((1.0,)), list)


class TestBitStringAdapter:
    def test_text_round_trip(self):
        adapter = PostgresNeutralBitStringAdapter()
        bits = BitString.from_bits("1010")
        assert adapter.to_database(bits, str) == "1010"
        assert adapter.from_database("1010", BitString) == bits

    def test_literal_option_emits_sql_literal(self):
        adapter = PostgresNeutralBitStringAdapter()
        assert adapter.to_database(BitString.from_bits("1010"), str, {"literal": True}) == (
            "B'1010'"
        )

    def test_declared_length_pads_on_the_right(self):
        adapter = PostgresNeutralBitStringAdapter()
        decoded = adapter.from_database("1010", BitString, {"length": 8})
        assert decoded.to_bits() == "10100000"
        assert decoded.bit_length == 8

    def test_declared_length_rejects_longer_text(self):
        adapter = PostgresNeutralBitStringAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("10101", BitString, {"length": 4})

    def test_bytes_and_wrapper_decode(self):
        adapter = PostgresNeutralBitStringAdapter()
        assert adapter.from_database(b"\xa0", BitString, {"bit_length": 4}) == (
            BitString.from_bits("1010")
        )
        assert adapter.from_database(PostgresBitString("1010"), BitString) == (
            BitString.from_bits("1010")
        )

    def test_invalid_text_is_wrapped(self):
        adapter = PostgresNeutralBitStringAdapter()
        for value in ("102", "", "B'101'"[:-1]):
            with pytest.raises(TypeConversionError):
                adapter.from_database(value, BitString)


class TestRangeAdapter:
    def test_numeric_range_round_trip(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range(1, 10, "[)")
        assert adapter.to_database(value, str) == "[1,10)"
        assert adapter.from_database("[1,10)", Range) == value

    def test_unbounded_bounds_are_normalized(self):
        adapter = PostgresNeutralRangeAdapter()
        decoded = adapter.from_database("(,5)", Range)
        assert decoded == Range(None, 5, "()")
        assert adapter.to_database(decoded, str) == "(,5)"
        # A missing bound is forced to '('; the present upper bound keeps ']'.
        assert adapter.from_database("[,5]", Range) == Range(None, 5, "(]")

    def test_decimal_bounds_keep_their_precision(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range(Decimal("1.5"), Decimal("2.5"), "(]")
        assert adapter.to_database(value, str) == "(1.5,2.5]"
        assert adapter.from_database("(1.5,2.5]", Range) == value

    def test_quoted_text_bounds_use_postgres_quoting(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range("a b", "c", "[)")
        assert adapter.to_database(value, str) == '["a b","c")'
        assert adapter.from_database('["a b","c")', Range) == value

    def test_quote_and_backslash_escapes_round_trip(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range('a"b\\c', "z", "[)")
        text = adapter.to_database(value, str)
        assert text == '["a\\"b\\\\c","z")'
        assert adapter.from_database(text, Range) == value

    def test_date_bounds_need_an_element_type_hint(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range(date(2024, 1, 1), date(2024, 12, 31), "[)")
        text = adapter.to_database(value, str)
        assert text == '["2024-01-01","2024-12-31")'
        decoded = adapter.from_database(text, Range, {"range_type": "daterange"})
        assert decoded == value

    def test_datetime_bounds_need_an_element_type_hint(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range(datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 12, 0), "[)")
        text = adapter.to_database(value, str)
        assert adapter.from_database(text, Range, {"element_type": "datetime"}) == value

    def test_timestamp_with_timezone_bounds_need_a_hint(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range(
            datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
            "[)",
        )
        text = adapter.to_database(value, str)
        assert adapter.from_database(text, Range, {"range_type": "tstzrange"}) == value

    def test_time_bounds_need_a_hint(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range(time(9, 0), time(17, 30), "[)")
        text = adapter.to_database(value, str)
        assert adapter.from_database(text, Range, {"element_type": "time"}) == value

    def test_quoted_bounds_without_a_hint_stay_strings(self):
        adapter = PostgresNeutralRangeAdapter()
        decoded = adapter.from_database('["2024-01-01","2024-01-02")', Range)
        assert decoded.lower == "2024-01-01"
        assert isinstance(decoded.lower, str)

    def test_a_bad_hint_value_is_wrapped(self):
        adapter = PostgresNeutralRangeAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database('["not-a-date","2024-01-02")', Range, {"element_type": "date"})

    def test_an_unknown_hint_label_is_wrapped(self):
        adapter = PostgresNeutralRangeAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("[1,2)", Range, {"element_type": "uuid"})

    def test_empty_range(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Range(empty=True)
        assert adapter.to_database(value, str) == "empty"
        assert adapter.from_database("empty", Range) == value

    def test_multirange_round_trip(self):
        adapter = PostgresNeutralRangeAdapter()
        value = Multirange((Range(1, 5), Range(10, 15)))
        assert adapter.to_database(value, str) == "{[1,5),[10,15)}"
        assert adapter.from_database("{[1,5),[10,15)}", Multirange) == value
        assert adapter.from_database("{}", Multirange) == Multirange(())

    def test_decodes_the_backend_wrappers(self):
        adapter = PostgresNeutralRangeAdapter()
        wrapper = PostgresRange(1, 10, lower_inc=True, upper_inc=False)
        assert adapter.from_database(wrapper, Range) == Range(1, 10, "[)")
        multi = PostgresMultirange([wrapper, PostgresRange(20, 30)])
        assert adapter.from_database(multi, Multirange) == Multirange((Range(1, 10), Range(20, 30)))

    def test_decodes_psycopg_range_objects(self):
        psycopg_range = pytest.importorskip("psycopg.types.range")
        adapter = PostgresNeutralRangeAdapter()
        value = psycopg_range.Range(1, 5, "[)")
        assert adapter.from_database(value, Range) == Range(1, 5, "[)")
        assert adapter.from_database([value, psycopg_range.Range(10, 15, "[)")], Multirange) == (
            Multirange((Range(1, 5), Range(10, 15)))
        )

    def test_malformed_literals_are_wrapped(self):
        adapter = PostgresNeutralRangeAdapter()
        for value in ("1,2", "[1,2", "[1,2,3)", "[a,b", "{[1,2)"):
            with pytest.raises(TypeConversionError):
                adapter.from_database(value, Range)

    def test_target_type_mismatch_is_wrapped(self):
        adapter = PostgresNeutralRangeAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("[1,2)", Multirange)


class TestMacAddressAdapter:
    def test_canonical_text_round_trip(self):
        adapter = PostgresNeutralMacAddressAdapter()
        mac = MacAddress.parse("08:00:2B:01:02:03")
        assert adapter.to_database(mac, str) == "08:00:2b:01:02:03"
        assert adapter.from_database("08:00:2b:01:02:03", MacAddress) == mac

    def test_eui64_option_expands_six_octets(self):
        adapter = PostgresNeutralMacAddressAdapter()
        mac = MacAddress.parse("08:00:2b:01:02:03")
        assert adapter.to_database(mac, str, {"eui64": True}) == ("08:00:2b:ff:fe:01:02:03")
        assert adapter.to_database(mac, str, {"version": 8}) == ("08:00:2b:ff:fe:01:02:03")

    def test_decodes_bytes_and_backend_wrappers(self):
        adapter = PostgresNeutralMacAddressAdapter()
        expected = MacAddress.parse("08:00:2b:01:02:03")
        assert adapter.from_database(b"\x08\x00\x2b\x01\x02\x03", MacAddress) == expected
        assert adapter.from_database(PostgresMacaddr("08:00:2b:01:02:03"), MacAddress) == (expected)
        assert adapter.from_database(
            PostgresMacaddr8("08:00:2b:ff:fe:01:02:03"), MacAddress
        ) == MacAddress.parse("08:00:2b:ff:fe:01:02:03")

    def test_invalid_input_is_wrapped(self):
        adapter = PostgresNeutralMacAddressAdapter()
        with pytest.raises(TypeConversionError):
            adapter.from_database("08:00:2b:01:02", MacAddress)
        with pytest.raises(TypeConversionError):
            adapter.to_database(MacAddress.parse("08:00:2b:01:02:03"), int)
