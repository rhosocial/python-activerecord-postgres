# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_typed_values.py
"""The typed value nodes on PostgreSQL: bindings, tier-2 literals, live answers.

PostgreSQL spells three of the twelve value kinds its own way (the tier-2 set
the core's ``TypedValueMixin`` refuses to guess): binary, UUID and INTERVAL.
This file pins both positions of each:

* the **data position** (``inline_params = False``) must produce a placeholder
  and hand the host value to psycopg untouched -- bytes, ``uuid.UUID`` and
  ``timedelta`` are all natively bindable, so any conversion there would be
  work the driver already does;
* the **DDL position** must produce the exact literal text. ``DEFAULT``
  clauses accept no bind parameters, so this is the one place the spelling is
  observable.

The DDL position is exercised through test-local subclasses that declare
``inline_params = True`` -- the same class-level strategy flip
``DdlLiteral`` applies to ``Literal``, because whether a value inlines is the
**class's** decision, never an instance flag. The core has not shipped DDL
variants of the typed value nodes, and a dialect must not invent nodes, so the
strategy flip lives in the test that needs it.

The interval spelling is per-component signed, and that is a measured answer,
not a preference: live servers (13.23, 17.11, 18.6 and 19beta4, 2026-10-17/18)
parse ``INTERVAL '-2 days 03:04:05.000006'`` as *minus two days plus
03:04:05* -- the sign does not carry into the time part -- while
``INTERVAL '-2 days -3 hours -4 minutes -5.000006 seconds'`` reads back as
exactly the negated span. Every component therefore carries its own sign.

Live confirmation follows the core helper's discipline: a sentinel first
(deliberately invalid SQL must come back REJECTED), then a CREATE TABLE whose
DEFAULT clauses embed the rendered literals, read back through
``INSERT ... DEFAULT VALUES`` and compared as Python values.
"""

import datetime
import uuid as uuid_module

import pytest

from rhosocial.activerecord.backend.expression.execution_testing import (
    ExecutionOutcome,
    classify_execution,
)
from rhosocial.activerecord.backend.expression.values import (
    BinaryValue,
    BooleanValue,
    DateValue,
    IntervalValue,
    StringValue,
    UUIDValue,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.typed_value import (
    PostgresTypedValueMixin,
)


# ---------------------------------------------------------------------------
# DDL-position variants: the class-level inline strategy, flipped per class
# ---------------------------------------------------------------------------


class DdlBinaryValue(BinaryValue):
    """A binary value at a DDL syntax position: the class inlines."""

    inline_params: bool = True


class DdlUUIDValue(UUIDValue):
    """A UUID at a DDL syntax position: the class inlines."""

    inline_params: bool = True


class DdlIntervalValue(IntervalValue):
    """An interval at a DDL syntax position: the class inlines."""

    inline_params: bool = True


class DdlStringValue(StringValue):
    """A string at a DDL syntax position: tier-1, inlined by the base."""

    inline_params: bool = True


UUID_TEXT = "01234567-89ab-cdef-0123-456789abcdef"


# ---------------------------------------------------------------------------
# Wiring: the pair must resolve on this backend's mixin
# ---------------------------------------------------------------------------


class TestWiring:
    """The dialect composes PostgreSQL's mixin ahead of the core's."""

    def test_tier_two_methods_resolve_on_the_postgres_mixin(self):
        """MRO must find the PostgreSQL spellings, not the core refusals.

        The base list puts ``PostgresTypedValueMixin`` ahead of
        ``TypedValueMixin``; if that order were lost, the DDL position would
        fall back to the core's explicit ``NotImplementedError`` and only the
        bind path would keep working.
        """
        dialect = PostgresDialect(version=(18, 0, 0))
        for name in (
            "format_binary_value",
            "format_uuid_value",
            "format_interval_value",
        ):
            resolved = getattr(type(dialect), name)
            assert resolved is getattr(PostgresTypedValueMixin, name), name


# ---------------------------------------------------------------------------
# Offline: both positions of every kind this backend overrides
# ---------------------------------------------------------------------------


class TestDataPositionBindsTheHostValue:
    """A data position emits a placeholder and hands psycopg the host value."""

    def test_binary_binds_bytes(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert BinaryValue(dialect, b"\x01\x02").to_sql() == ("%s", (b"\x01\x02",))

    def test_uuid_binds_the_uuid_object(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        value = uuid_module.UUID(UUID_TEXT)
        assert UUIDValue(dialect, value).to_sql() == ("%s", (value,))

    def test_interval_binds_the_timedelta(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        value = datetime.timedelta(days=2, seconds=11045, microseconds=6)
        assert IntervalValue(dialect, value).to_sql() == ("%s", (value,))

    def test_tier_one_kinds_keep_the_core_binding(self):
        """NULL/boolean/date are tier-1: the core mixin binds them unchanged."""
        dialect = PostgresDialect(version=(18, 0, 0))
        assert BooleanValue(dialect, True).to_sql() == ("%s", (True,))
        assert DateValue(dialect, datetime.date(2026, 10, 17)).to_sql() == (
            "%s",
            (datetime.date(2026, 10, 17),),
        )


class TestDdlPositionLiterals:
    """A DDL position inlines the exact literal text, no parameters."""

    def test_binary_is_a_cast_hex_bytea(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert DdlBinaryValue(dialect, b"\x01\x02").to_sql() == ("'\\x0102'::bytea", ())

    def test_binary_accepts_a_bytearray(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert DdlBinaryValue(dialect, bytearray(b"\xff\x00")).to_sql() == (
            "'\\xff00'::bytea",
            (),
        )

    def test_uuid_is_the_quoted_form_with_a_cast(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert DdlUUIDValue(dialect, uuid_module.UUID(UUID_TEXT)).to_sql() == (
            f"'{UUID_TEXT}'::uuid",
            (),
        )

    @pytest.mark.parametrize(
        "value, expected",
        [
            (
                datetime.timedelta(days=2, hours=3, minutes=4, seconds=5, microseconds=6),
                "2 days 3 hours 4 minutes 5.000006 seconds",
            ),
            (
                datetime.timedelta(days=-2, hours=-3, minutes=-4, seconds=-5, microseconds=-6),
                "-2 days -3 hours -4 minutes -5.000006 seconds",
            ),
            (datetime.timedelta(0), "0 seconds"),
            (datetime.timedelta(microseconds=6), "0.000006 seconds"),
            (datetime.timedelta(hours=3), "3 hours"),
            (datetime.timedelta(days=2, microseconds=6), "2 days 0.000006 seconds"),
            (datetime.timedelta(minutes=4, seconds=5), "4 minutes 5 seconds"),
        ],
        ids=["positive-full", "negative-full", "zero", "sub-second", "hours-only",
             "days-plus-fraction", "minutes-and-seconds"],
    )
    def test_interval_spelling(self, value, expected):
        """Every component carries the sign; measured live (see module docstring)."""
        dialect = PostgresDialect(version=(18, 0, 0))
        assert DdlIntervalValue(dialect, value).to_sql() == (f"INTERVAL '{expected}'", ())

    def test_tier_one_string_still_inlines_through_the_base(self):
        """Tier-1 kinds inline via ``format_literal``; the escape is the base's."""
        dialect = PostgresDialect(version=(18, 0, 0))
        assert DdlStringValue(dialect, "it's").to_sql() == ("'it''s'", ())


# ---------------------------------------------------------------------------
# Live: the rendered literals are accepted and read back by the server
# ---------------------------------------------------------------------------


class TestLiveLiteralRoundTrip:
    """Execution confirmation on every configured scenario server.

    The defaults are rendered by the dialect's own formatters (through the
    DDL-position classes above), then executed, then read back as Python
    values. All three spellings are accepted unconditionally: the hex bytea
    literal needs PostgreSQL 9.0 or newer and every configured scenario
    satisfies that (the oldest scenario's server measured 13.23).
    """

    INTERVAL = datetime.timedelta(days=2, hours=3, minutes=4, seconds=5, microseconds=6)
    BINARY = b"\x01\x02"
    UUID = uuid_module.UUID(UUID_TEXT)

    def test_sentinel_rejected_and_control_accepted(self, postgres_backend):
        """The classifier must see both answers before any verdict is trusted."""
        assert (
            classify_execution(postgres_backend, "THIS IS NOT SQL")
            is ExecutionOutcome.REJECTED
        )
        assert (
            classify_execution(postgres_backend, "SELECT 1")
            is ExecutionOutcome.ACCEPTED
        )

    def test_column_defaults_round_trip(self, postgres_backend):
        """The three tier-2 literals as column DEFAULTs survive a read-back."""
        dialect = postgres_backend.dialect
        iv_default, _ = DdlIntervalValue(dialect, self.INTERVAL).to_sql()
        b_default, _ = DdlBinaryValue(dialect, self.BINARY).to_sql()
        u_default, _ = DdlUUIDValue(dialect, self.UUID).to_sql()

        table = "test_typed_value_defaults"
        postgres_backend.execute(f"""
            CREATE TABLE {table} (
                iv interval DEFAULT {iv_default},
                b bytea DEFAULT {b_default},
                u uuid DEFAULT {u_default}
            )
        """)
        try:
            postgres_backend.execute(f"INSERT INTO {table} DEFAULT VALUES")
            row = postgres_backend.fetch_one(
                f"SELECT iv AS iv, b AS b, u AS u FROM {table}"
            )
            assert row is not None
            assert row["iv"] == self.INTERVAL
            assert bytes(row["b"]) == self.BINARY
            assert row["u"] == self.UUID
        finally:
            postgres_backend.execute(f"DROP TABLE IF EXISTS {table}")

    def test_negative_interval_default_round_trip(self, postgres_backend):
        """The per-component minus signs read back as one negated span."""
        dialect = postgres_backend.dialect
        span = datetime.timedelta(days=-2, hours=-3, minutes=-4, seconds=-5, microseconds=-6)
        iv_default, _ = DdlIntervalValue(dialect, span).to_sql()

        table = "test_typed_value_negative_interval"
        postgres_backend.execute(f"""
            CREATE TABLE {table} (iv interval DEFAULT {iv_default})
        """)
        try:
            postgres_backend.execute(f"INSERT INTO {table} DEFAULT VALUES")
            row = postgres_backend.fetch_one(f"SELECT iv AS iv FROM {table}")
            assert row is not None
            assert row["iv"] == span
        finally:
            postgres_backend.execute(f"DROP TABLE IF EXISTS {table}")

    def test_zero_interval_default_round_trip(self, postgres_backend):
        """``INTERVAL '0 seconds'`` reads back as a zero span."""
        dialect = postgres_backend.dialect
        iv_default, _ = DdlIntervalValue(dialect, datetime.timedelta(0)).to_sql()
        assert iv_default == "INTERVAL '0 seconds'"

        table = "test_typed_value_zero_interval"
        postgres_backend.execute(f"""
            CREATE TABLE {table} (iv interval DEFAULT {iv_default})
        """)
        try:
            postgres_backend.execute(f"INSERT INTO {table} DEFAULT VALUES")
            row = postgres_backend.fetch_one(f"SELECT iv AS iv FROM {table}")
            assert row is not None
            assert row["iv"] == datetime.timedelta(0)
        finally:
            postgres_backend.execute(f"DROP TABLE IF EXISTS {table}")
