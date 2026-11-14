# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_ranges.py
"""Native tests for PostgreSQL range and multirange types.

Discrete ranges are normalised by the server (``[1,3]`` becomes ``[1,4)``),
so the assertions compare canonical values rather than the input object.
Text parsing cannot recover an element type on its own: without an
``element_type``/``range_type`` hint, a quoted ``"2024-01-01"`` stays a
``str``.  That limitation is asserted explicitly, and the hinted form is
asserted next to it.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.adapters import Multirange, Range
from rhosocial.activerecord.backend.errors import TypeConversionError
from rhosocial.activerecord.backend.impl.postgres.adapters.neutral import (
    PostgresNeutralRangeAdapter,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    update_cell,
)

RANGE_TABLE = "native_ranges"
MULTIRANGE_TABLE = "native_multiranges"

RANGE = PostgresNeutralRangeAdapter()


def _require_multirange(backend):
    version = backend.get_server_version()
    if version < (14, 0, 0):
        pytest.skip(
            "multirange requires PostgreSQL 14+, server is {}".format(
                ".".join(str(part) for part in version)
            )
        )


def _read_range_columns(backend, table, row_id, columns):
    select = ", ".join("{}::text AS {}".format(name, name) for name in columns)
    return backend.fetch_one(
        "SELECT {} FROM {} WHERE id = %s".format(select, table), (row_id,)
    )


class TestRanges:
    TABLES = (RANGE_TABLE, MULTIRANGE_TABLE)

    def test_six_range_types_bind_and_read_canonically(self, native_backend):
        """S1: all six range types store and print their canonical text."""
        backend = native_backend
        backend.execute("SET TIME ZONE 'UTC'")
        values = {
            "i4": RANGE.to_database(Range(1, 3, "[]"), str),
            "i8": RANGE.to_database(Range(2**40, 2**40 + 10, "[)"), str),
            "num": RANGE.to_database(Range(Decimal("1.5"), Decimal("2.5"), "(]"), str),
            "ts": RANGE.to_database(
                Range(datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 12, 0), "[)"),
                str,
            ),
            "tstz": RANGE.to_database(
                Range(
                    datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                    "[)",
                ),
                str,
            ),
            "dt": RANGE.to_database(
                Range(date(2024, 1, 1), date(2024, 1, 2), "[)"), str
            ),
        }
        casts = {
            "i4": "int4range",
            "i8": "int8range",
            "num": "numrange",
            "ts": "tsrange",
            "tstz": "tstzrange",
            "dt": "daterange",
        }
        row_id = insert_row(backend, RANGE_TABLE, values, casts)
        row = _read_range_columns(backend, RANGE_TABLE, row_id, values)

        assert row["i4"] == "[1,4)"
        assert row["i8"] == "[1099511627776,1099511627786)"
        assert row["num"] == "(1.5,2.5]"
        assert row["ts"] == '["2024-01-01 12:00:00","2024-01-02 12:00:00")'
        assert row["tstz"] == '["2024-01-01 12:00:00+00","2024-01-02 12:00:00+00")'
        assert row["dt"] == "[2024-01-01,2024-01-02)"

        assert RANGE.from_database(row["i4"], Range, {"range_type": "int4range"}) == (
            Range(1, 4, "[)")
        )
        assert RANGE.from_database(row["i8"], Range, {"range_type": "int8range"}) == (
            Range(2**40, 2**40 + 10, "[)")
        )
        assert RANGE.from_database(row["num"], Range, {"element_type": "decimal"}) == (
            Range(Decimal("1.5"), Decimal("2.5"), "(]")
        )
        assert RANGE.from_database(row["ts"], Range, {"element_type": "datetime"}) == (
            Range(datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 12, 0), "[)")
        )
        assert RANGE.from_database(row["tstz"], Range, {"range_type": "tstzrange"}) == (
            Range(
                datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                "[)",
            )
        )
        assert RANGE.from_database(row["dt"], Range, {"range_type": "daterange"}) == (
            Range(date(2024, 1, 1), date(2024, 1, 2), "[)")
        )

    def test_without_an_element_hint_quoted_bounds_stay_strings(self, native_backend):
        """The documented limitation: text alone cannot infer date/datetime."""
        backend = native_backend
        backend.execute("SET TIME ZONE 'UTC'")
        values = {"dt": RANGE.to_database(Range(date(2024, 1, 1), date(2024, 1, 2)), str)}
        row_id = insert_row(backend, RANGE_TABLE, values, {"dt": "daterange"})
        raw = read_raw(backend, RANGE_TABLE, "dt", row_id)

        unhinted = RANGE.from_database(raw, Range)
        assert unhinted.lower == "2024-01-01"
        assert isinstance(unhinted.lower, str)
        assert isinstance(unhinted.upper, str)

        hinted = RANGE.from_database(raw, Range, {"element_type": "date"})
        assert hinted.lower == date(2024, 1, 1)
        assert hinted.upper == date(2024, 1, 2)

        by_range_type = RANGE.from_database(raw, Range, {"range_type": "daterange"})
        assert by_range_type == hinted

    def test_empty_and_unbounded_ranges_are_normalised(self, native_backend):
        """S2: empty stays empty; ``,5]`` canonicalises to ``(,6)``."""
        backend = native_backend
        backend.execute("SET TIME ZONE 'UTC'")
        empty_row = insert_row(
            backend,
            RANGE_TABLE,
            {"i4": RANGE.to_database(Range(empty=True), str)},
            {"i4": "int4range"},
        )
        assert read_raw(backend, RANGE_TABLE, "i4", empty_row) == "empty"
        empty = RANGE.from_database("empty", Range)
        assert empty == Range(empty=True)
        assert RANGE.to_database(empty, str) == "empty"

        unbounded = Range(None, 5, "(]")
        row_id = insert_row(
            backend,
            RANGE_TABLE,
            {"i4": RANGE.to_database(unbounded, str)},
            {"i4": "int4range"},
        )
        raw = read_raw(backend, RANGE_TABLE, "i4", row_id)
        assert raw == "(,6)"
        decoded = RANGE.from_database(raw, Range, {"range_type": "int4range"})
        assert decoded == Range(None, 6, "()")

        update_cell(
            backend, RANGE_TABLE, "i4", row_id,
            RANGE.to_database(decoded, str), "int4range",
        )
        assert read_raw(backend, RANGE_TABLE, "i4", row_id) == "(,6)"

    def test_six_multirange_types_round_trip(self, native_backend):
        """S1: all six multirange types keep their canonical segment list."""
        backend = native_backend
        _require_multirange(backend)
        backend.execute("SET TIME ZONE 'UTC'")
        values = {
            "i4": RANGE.to_database(Multirange((Range(1, 5), Range(10, 15))), str),
            "i8": RANGE.to_database(Multirange((Range(2**40, 2**40 + 5),)), str),
            "num": RANGE.to_database(
                Multirange((Range(Decimal("1.5"), Decimal("2.5")),)), str
            ),
            "ts": RANGE.to_database(
                Multirange(
                    (Range(datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 12, 0)),)
                ),
                str,
            ),
            "tstz": RANGE.to_database(
                Multirange(
                    (
                        Range(
                            datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                            datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                        ),
                    )
                ),
                str,
            ),
            "dt": RANGE.to_database(
                Multirange((Range(date(2024, 1, 1), date(2024, 1, 2)),)), str
            ),
        }
        casts = {
            "i4": "int4multirange",
            "i8": "int8multirange",
            "num": "nummultirange",
            "ts": "tsmultirange",
            "tstz": "tstzmultirange",
            "dt": "datemultirange",
        }
        row_id = insert_row(backend, MULTIRANGE_TABLE, values, casts)
        row = _read_range_columns(backend, MULTIRANGE_TABLE, row_id, values)

        assert row["i4"] == "{[1,5),[10,15)}"
        assert row["i8"] == "{[1099511627776,1099511627781)}"
        assert row["num"] == "{[1.5,2.5)}"
        assert row["ts"] == '{["2024-01-01 12:00:00","2024-01-02 12:00:00")}'
        assert row["tstz"] == '{["2024-01-01 12:00:00+00","2024-01-02 12:00:00+00")}'
        assert row["dt"] == "{[2024-01-01,2024-01-02)}"

        assert RANGE.from_database(row["i4"], Multirange, {"range_type": "int4range"}) == (
            Multirange((Range(1, 5), Range(10, 15)))
        )
        assert RANGE.from_database(row["num"], Multirange, {"element_type": "decimal"}) == (
            Multirange((Range(Decimal("1.5"), Decimal("2.5")),))
        )
        assert RANGE.from_database(row["dt"], Multirange, {"range_type": "daterange"}) == (
            Multirange((Range(date(2024, 1, 1), date(2024, 1, 2)),))
        )

    def test_multirange_merges_overlap_and_sorts(self, native_backend):
        """S2: unsorted/touching input becomes one sorted canonical list."""
        backend = native_backend
        _require_multirange(backend)
        value = Multirange((Range(20, 25), Range(1, 5), Range(5, 10)))
        assert RANGE.to_database(value, str) == "{[1,10),[20,25)}"

        row_id = insert_row(
            backend,
            MULTIRANGE_TABLE,
            {"i4": RANGE.to_database(value, str)},
            {"i4": "int4multirange"},
        )
        raw = read_raw(backend, MULTIRANGE_TABLE, "i4", row_id)
        assert raw == "{[1,10),[20,25)}"
        decoded = RANGE.from_database(raw, Multirange, {"range_type": "int4range"})
        assert decoded == Multirange((Range(1, 10), Range(20, 25)))

    def test_external_range_and_multirange_literals(self, native_backend):
        """S3: native SQL range literals read back as neutral values."""
        backend = native_backend
        _require_multirange(backend)
        backend.execute("SET TIME ZONE 'UTC'")
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (i4, dt) VALUES "
            "('[1,10)'::int4range, '[2024-01-01,2024-01-02)'::daterange) "
            "RETURNING id".format(RANGE_TABLE),
        )
        multi_id = insert_sql(
            backend,
            "INSERT INTO {} (i4) VALUES "
            "('{{[1,5),[10,15)}}'::int4multirange) RETURNING id".format(
                MULTIRANGE_TABLE
            ),
        )

        assert read_raw(backend, RANGE_TABLE, "i4", row_id) == "[1,10)"
        assert read_raw(backend, RANGE_TABLE, "dt", row_id) == "[2024-01-01,2024-01-02)"
        assert RANGE.from_database("[1,10)", Range, {"range_type": "int4range"}) == (
            Range(1, 10, "[)")
        )
        assert read_raw(backend, MULTIRANGE_TABLE, "i4", multi_id) == "{[1,5),[10,15)}"
        assert RANGE.from_database(
            "{[1,5),[10,15)}", Multirange, {"range_type": "int4range"}
        ) == Multirange((Range(1, 5), Range(10, 15)))

    def test_unknown_element_hint_is_wrapped(self):
        """A hint the adapter does not know is a conversion error."""
        with pytest.raises(TypeConversionError):
            RANGE.from_database("[1,2)", Range, {"element_type": "uuid"})
