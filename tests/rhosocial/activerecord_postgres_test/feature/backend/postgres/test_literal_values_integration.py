# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_literal_values_integration.py
"""Live answers for the tier-2 literal spellings, on the scenario servers.

The rendering half of this question lives in
``test_literal_values.py``: an expression test builds a tree, renders it
through its own dialect and asserts the text; it starts no connection,
creates no schema and reads no file (expression-dialect §12). What that
file cannot answer is what the *server* accepts, so the four cases below
run against every configured scenario (the ``postgres_backend`` fixture is
parameterized over ``postgres_15`` ... ``postgres_19beta4``) and are kept
here, beside the other scenario-driven live tests, rather than beside the
rendering assertions.

The interval spelling is per-component signed, and that is a measured
answer, not a preference: live servers (13.23, 17.11, 18.6 and 19beta4,
2026-10-17/18) parse ``INTERVAL '-2 days 03:04:05.000006'`` as *minus two
days plus 03:04:05* -- the sign does not carry into the time part -- while
``INTERVAL '-2 days -3 hours -4 minutes -5.000006 seconds'`` reads back as
exactly the negated span. Every component therefore carries its own sign.

Live confirmation follows the core helper's discipline: a sentinel first
(deliberately invalid SQL must come back REJECTED), then a CREATE TABLE
whose DEFAULT clauses embed the rendered literals, read back through
``INSERT ... DEFAULT VALUES`` and compared as Python values.
"""

import datetime
import uuid as uuid_module

from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.execution_testing import (
    ExecutionOutcome,
    classify_execution,
)


UUID_TEXT = "01234567-89ab-cdef-0123-456789abcdef"


class TestLiveLiteralRoundTrip:
    """Execution confirmation on every configured scenario server.

    The defaults are rendered by the dialect's own literal construction and
    executed, then read back as Python values. All three spellings are
    accepted unconditionally: the hex bytea literal needs PostgreSQL 9.0 or
    newer and every configured scenario satisfies that (the oldest
    scenario's server measured 13.23).
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
        iv_default, _ = Literal(dialect, self.INTERVAL, inline=True).to_sql()
        b_default, _ = Literal(dialect, self.BINARY, inline=True).to_sql()
        u_default, _ = Literal(dialect, self.UUID, inline=True).to_sql()

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
        iv_default, _ = Literal(dialect, span, inline=True).to_sql()

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
        iv_default, _ = Literal(dialect, datetime.timedelta(0), inline=True).to_sql()
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
