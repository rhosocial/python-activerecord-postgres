# tests/.../feature/backend/postgres/test_money_integration.py
"""
Integration tests for PostgreSQL MONEY type with real database.

These tests require a live PostgreSQL connection and test:
- money storage and retrieval through stable numeric casts
- PostgresMoneyAdapter output accepted by PostgreSQL
- Sync/async round-trip behavior
"""
from decimal import Decimal

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.adapters.monetary import (
    PostgresMoneyAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.types.monetary import PostgresMoney


MONEY_TABLE = "test_money_types"
ASYNC_MONEY_TABLE = "test_money_types_async"


def _money_literal(value):
    adapter = PostgresMoneyAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    return f"'{database_value}'::money"


class TestSyncMoneyIntegration:
    """Synchronous integration tests for PostgreSQL money type."""

    @pytest.fixture
    def money_test_table(self, postgres_backend):
        """Create a table containing a money column for sync tests."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {MONEY_TABLE}")
        postgres_backend.execute(f"""
            CREATE TABLE {MONEY_TABLE} (
                id SERIAL PRIMARY KEY,
                amount money
            )
        """)
        yield MONEY_TABLE
        postgres_backend.execute(f"DROP TABLE IF EXISTS {MONEY_TABLE}")

    def test_insert_and_select_money_value(self, postgres_backend, money_test_table):
        """Insert a PostgresMoney value and verify PostgreSQL stores the numeric amount."""
        literal = _money_literal(PostgresMoney(Decimal("1234.56")))

        postgres_backend.execute(
            f"INSERT INTO {money_test_table} (amount) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT amount::numeric AS amount FROM {money_test_table} WHERE id = 1"
        )

        assert result["amount"] == Decimal("1234.56")

    def test_insert_negative_money_value(self, postgres_backend, money_test_table):
        """Insert a negative money value and verify the sign survives round-trip."""
        literal = _money_literal(PostgresMoney(Decimal("-42.75")))

        postgres_backend.execute(
            f"INSERT INTO {money_test_table} (amount) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT amount::numeric AS amount FROM {money_test_table} WHERE id = 1"
        )

        assert result["amount"] == Decimal("-42.75")

    def test_insert_money_from_plain_decimal(self, postgres_backend, money_test_table):
        """Pass a Decimal through the adapter and verify the stored money amount."""
        literal = _money_literal(Decimal("19.99"))

        postgres_backend.execute(
            f"INSERT INTO {money_test_table} (amount) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT amount::numeric AS amount FROM {money_test_table}"
        )

        assert result["amount"] == Decimal("19.99")

    def test_null_money_round_trip(self, postgres_backend, money_test_table):
        """Insert a NULL money value and verify the fetched value is None."""
        postgres_backend.execute(
            f"INSERT INTO {money_test_table} (amount) VALUES (NULL)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT amount FROM {money_test_table} WHERE id = 1"
        )

        assert result["amount"] is None

    def test_money_filter_by_numeric_value(self, postgres_backend, money_test_table):
        """Insert two money rows and verify numeric equality matches only one row."""
        expected = _money_literal(PostgresMoney(Decimal("25.50")))
        other = _money_literal(PostgresMoney(Decimal("30.00")))

        postgres_backend.execute(
            f"INSERT INTO {money_test_table} (amount) VALUES ({expected}), ({other})"
        )
        result = postgres_backend.fetch_one(f"""
            SELECT COUNT(*) AS match_count
            FROM {money_test_table}
            WHERE amount = {expected}
        """)

        assert result["match_count"] == 1

    def test_invalid_money_rejected_before_insert(self):
        """Pass an invalid money string and verify the adapter rejects it before SQL."""
        with pytest.raises(ValueError):
            _money_literal("not_a_number")


class TestAsyncMoneyIntegration:
    """Asynchronous integration tests for PostgreSQL money type."""

    @pytest_asyncio.fixture
    async def async_money_test_table(self, async_postgres_backend):
        """Create a table containing a money column for async tests."""
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_MONEY_TABLE}")
        await async_postgres_backend.execute(f"""
            CREATE TABLE {ASYNC_MONEY_TABLE} (
                id SERIAL PRIMARY KEY,
                amount money
            )
        """)
        yield ASYNC_MONEY_TABLE
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_MONEY_TABLE}")

    @pytest.mark.asyncio
    async def test_async_money_round_trip(
        self, async_postgres_backend, async_money_test_table
    ):
        """Insert a PostgresMoney value asynchronously and verify the numeric amount."""
        literal = _money_literal(PostgresMoney(Decimal("88.25")))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_money_test_table} (amount) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT amount::numeric AS amount FROM {async_money_test_table}"
        )

        assert result["amount"] == Decimal("88.25")

    @pytest.mark.asyncio
    async def test_async_null_money_round_trip(
        self, async_postgres_backend, async_money_test_table
    ):
        """Insert a NULL money value asynchronously and verify fetched value is None."""
        await async_postgres_backend.execute(
            f"INSERT INTO {async_money_test_table} (amount) VALUES (NULL)"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT amount FROM {async_money_test_table}"
        )

        assert result["amount"] is None
