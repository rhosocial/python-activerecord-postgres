# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_literal_values.py
"""Value literals on PostgreSQL: binding and the three tier-2 spellings.

Core collapsed the twelve typed-value classes into one ``Literal`` node
carrying a value kind (baseline ``550ae740``), and made the bind-or-inline
question a per-position flag on that node: ``inline=False`` (the default, every
DQL/DML data position) emits a placeholder and hands the host value to psycopg;
``inline=True`` (the DDL syntax positions that accept no bind parameters)
writes the value as SQL text through ``format_literal`` -- the dialect's
three-level literal construction, whose second tier is exactly the types a
backend spells for itself.

PostgreSQL spells three of those types its own way, and this file pins both
positions of each:

* the **data position** must produce a placeholder and hand the host value to
  psycopg untouched -- bytes, ``uuid.UUID`` and ``timedelta`` are all natively
  bindable, so any conversion there would be work the driver already does;
* the **inline position** must produce the exact literal text, through
  ``Literal(..., inline=True)`` -- the caller-owned flag, since core removed
  the pass that used to lift a tree's literals into the DDL class.

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

from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.literal import (
    PostgresLiteralValueMixin,
)


UUID_TEXT = "01234567-89ab-cdef-0123-456789abcdef"


# ---------------------------------------------------------------------------
# Wiring: the override must resolve on this backend's mixin
# ---------------------------------------------------------------------------


class TestWiring:
    """The dialect composes PostgreSQL's literal mixin ahead of the base."""

    def test_format_literal_resolves_on_the_postgres_mixin(self):
        """MRO must find PostgreSQL's spellings, not the base's refusal.

        The base list puts ``PostgresLiteralValueMixin`` ahead of
        ``SQLDialectBase``; if that order were lost, bytes / UUID / timedelta
        would fall back to the base's explicit TypeError and only the bind
        path would keep working.
        """
        dialect = PostgresDialect(version=(18, 0, 0))
        assert (
            type(dialect).format_literal
            is PostgresLiteralValueMixin.format_literal
        )


# ---------------------------------------------------------------------------
# Offline: both positions of every kind this backend spells
# ---------------------------------------------------------------------------


class TestDataPositionBindsTheHostValue:
    """A data position emits a placeholder and hands psycopg the host value."""

    def test_binary_binds_bytes(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert Literal(dialect, b"\x01\x02").to_sql() == ("%s", (b"\x01\x02",))

    def test_uuid_binds_the_uuid_object(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        value = uuid_module.UUID(UUID_TEXT)
        assert Literal(dialect, value).to_sql() == ("%s", (value,))

    def test_interval_binds_the_timedelta(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        value = datetime.timedelta(days=2, seconds=11045, microseconds=6)
        assert Literal(dialect, value).to_sql() == ("%s", (value,))

    def test_date_binds_the_host_value(self):
        """A tier-1 kind binds through the same node, unchanged."""
        dialect = PostgresDialect(version=(18, 0, 0))
        value = datetime.date(2026, 10, 17)
        assert Literal(dialect, value).to_sql() == ("%s", (value,))


class TestInlinePositionLiterals:
    """An inline position writes the exact literal text, no parameters."""

    def test_binary_is_a_cast_hex_bytea(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert Literal(dialect, b"\x01\x02", inline=True).to_sql() == (
            "'\\x0102'::bytea",
            (),
        )

    def test_binary_accepts_a_bytearray(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert Literal(dialect, bytearray(b"\xff\x00"), inline=True).to_sql() == (
            "'\\xff00'::bytea",
            (),
        )

    def test_uuid_is_the_quoted_form_with_a_cast(self):
        dialect = PostgresDialect(version=(18, 0, 0))
        assert Literal(dialect, uuid_module.UUID(UUID_TEXT), inline=True).to_sql() == (
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
        assert Literal(dialect, value, inline=True).to_sql() == (
            f"INTERVAL '{expected}'",
            (),
        )

    def test_tier_one_string_still_inlines_through_the_base(self):
        """Tier-1 kinds inline via the base's portable forms; the escape stays its."""
        dialect = PostgresDialect(version=(18, 0, 0))
        assert Literal(dialect, "it's", inline=True).to_sql() == ("'it''s'", ())

    def test_unspelled_host_type_keeps_the_base_refusal(self):
        """A type neither the base nor PostgreSQL names fails explicitly.

        The three-tier construction's explicit failure (no ``str()`` fallback)
        is the contract; a set has no literal form anywhere.
        """
        dialect = PostgresDialect(version=(18, 0, 0))
        with pytest.raises(TypeError, match="no inline SQL literal form"):
            Literal(dialect, {1, 2, 3}, inline=True).to_sql()

