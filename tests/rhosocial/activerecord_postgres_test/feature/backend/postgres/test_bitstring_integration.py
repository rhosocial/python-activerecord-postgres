# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_bitstring_integration.py
"""
Integration tests for PostgreSQL bit string types with real database.

These tests require a live PostgreSQL connection and test:
- bit(n) and varbit(n) storage and retrieval
- PostgresBitStringAdapter output accepted by PostgreSQL
- Sync/async round-trip behavior
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.adapters.bit_string import (
    PostgresBitStringAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.types.bit_string import PostgresBitString


BITSTRING_TABLE = "test_bitstring_types"
ASYNC_BITSTRING_TABLE = "test_bitstring_types_async"


def _bit_literal(value, length=None):
    adapter = PostgresBitStringAdapter()
    options = {"length": length} if length is not None else None
    return adapter.to_database(value, str, options)


class TestSyncBitStringIntegration:
    """Synchronous integration tests for bit string types."""

    @pytest.fixture
    def bitstring_test_table(self, postgres_backend):
        """Create test table with bit string columns."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {BITSTRING_TABLE}")
        postgres_backend.execute(f"""
            CREATE TABLE {BITSTRING_TABLE} (
                id SERIAL PRIMARY KEY,
                fixed_bits bit(8),
                variable_bits varbit(32)
            )
        """)
        yield BITSTRING_TABLE
        postgres_backend.execute(f"DROP TABLE IF EXISTS {BITSTRING_TABLE}")

    def test_insert_and_select_fixed_bit_string(self, postgres_backend, bitstring_test_table):
        """Insert a short fixed bit string and verify PostgreSQL stores the padded value."""
        literal = _bit_literal(PostgresBitString("1010", length=8))

        postgres_backend.execute(
            f"INSERT INTO {bitstring_test_table} (fixed_bits) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT fixed_bits::text AS fixed_bits FROM {bitstring_test_table} WHERE id = 1"
        )

        assert result["fixed_bits"] == "10100000"

    def test_insert_and_select_varbit_string(self, postgres_backend, bitstring_test_table):
        """Insert a varbit value and verify it is returned without fixed-length padding."""
        literal = _bit_literal(PostgresBitString("10101"))

        postgres_backend.execute(
            f"INSERT INTO {bitstring_test_table} (variable_bits) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"""
            SELECT variable_bits::text AS variable_bits,
                   length(variable_bits) AS bit_length
            FROM {bitstring_test_table}
            WHERE id = 1
            """
        )

        assert result["variable_bits"] == "10101"
        assert result["bit_length"] == 5

    def test_insert_bit_string_from_plain_str(self, postgres_backend, bitstring_test_table):
        """Pass a plain 0/1 string through the adapter and verify the stored varbit text."""
        literal = _bit_literal("11011")

        postgres_backend.execute(
            f"INSERT INTO {bitstring_test_table} (variable_bits) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT variable_bits::text AS variable_bits FROM {bitstring_test_table}"
        )

        assert result["variable_bits"] == "11011"

    def test_insert_bit_string_from_int(self, postgres_backend, bitstring_test_table):
        """Pass an integer through the adapter and verify it becomes the expected bit(8)."""
        literal = _bit_literal(5, length=8)

        postgres_backend.execute(
            f"INSERT INTO {bitstring_test_table} (fixed_bits) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT fixed_bits::text AS fixed_bits FROM {bitstring_test_table}"
        )

        assert result["fixed_bits"] == "00000101"

    def test_null_bit_string_round_trip(self, postgres_backend, bitstring_test_table):
        """Insert NULL bit strings and verify both bit columns are returned as None."""
        postgres_backend.execute(
            f"INSERT INTO {bitstring_test_table} (fixed_bits, variable_bits) VALUES (NULL, NULL)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT fixed_bits, variable_bits FROM {bitstring_test_table}"
        )

        assert result["fixed_bits"] is None
        assert result["variable_bits"] is None

    def test_where_filter_by_bit_string_value(self, postgres_backend, bitstring_test_table):
        """Insert two varbit rows and verify equality filtering matches only one row."""
        expected = _bit_literal("10101")
        other = _bit_literal("11100")

        postgres_backend.execute(
            f"INSERT INTO {bitstring_test_table} (variable_bits) VALUES ({expected}), ({other})"
        )
        result = postgres_backend.fetch_one(
            f"""
            SELECT COUNT(*) AS match_count
            FROM {bitstring_test_table}
            WHERE variable_bits = {expected}
            """
        )

        assert result["match_count"] == 1

    def test_invalid_bit_string_rejected_before_insert(self):
        """Pass a non-binary string and verify the adapter rejects it before SQL execution."""
        with pytest.raises(ValueError):
            _bit_literal("10102")


class TestAsyncBitStringIntegration:
    """Asynchronous integration tests for bit string types."""

    @pytest_asyncio.fixture
    async def async_bitstring_test_table(self, async_postgres_backend):
        """Create async test table with bit string columns."""
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_BITSTRING_TABLE}")
        await async_postgres_backend.execute(f"""
            CREATE TABLE {ASYNC_BITSTRING_TABLE} (
                id SERIAL PRIMARY KEY,
                fixed_bits bit(8),
                variable_bits varbit(32)
            )
        """)
        yield ASYNC_BITSTRING_TABLE
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_BITSTRING_TABLE}")

    @pytest.mark.asyncio
    async def test_async_fixed_bit_string_round_trip(
        self, async_postgres_backend, async_bitstring_test_table
    ):
        """Insert a short bit(n) value asynchronously and verify the padded text result."""
        literal = _bit_literal(PostgresBitString("1010", length=8))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_bitstring_test_table} (fixed_bits) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT fixed_bits::text AS fixed_bits FROM {async_bitstring_test_table}"
        )

        assert result["fixed_bits"] == "10100000"

    @pytest.mark.asyncio
    async def test_async_varbit_round_trip(
        self, async_postgres_backend, async_bitstring_test_table
    ):
        """Insert a varbit value asynchronously and verify the same bit text is returned."""
        literal = _bit_literal("10101")

        await async_postgres_backend.execute(
            f"INSERT INTO {async_bitstring_test_table} (variable_bits) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT variable_bits::text AS variable_bits FROM {async_bitstring_test_table}"
        )

        assert result["variable_bits"] == "10101"

    @pytest.mark.asyncio
    async def test_async_null_bit_string_round_trip(
        self, async_postgres_backend, async_bitstring_test_table
    ):
        """Insert NULL bit strings asynchronously and verify fetched values are None."""
        await async_postgres_backend.execute(
            f"""
            INSERT INTO {async_bitstring_test_table} (fixed_bits, variable_bits)
            VALUES (NULL, NULL)
            """
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT fixed_bits, variable_bits FROM {async_bitstring_test_table}"
        )

        assert result["fixed_bits"] is None
        assert result["variable_bits"] is None
