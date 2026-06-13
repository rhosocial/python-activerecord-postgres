# tests/.../feature/backend/postgres/test_lsn_integration.py
"""
Integration tests for PostgreSQL pg_lsn type with real database.

These tests require a live PostgreSQL connection and test:
- pg_lsn storage and retrieval
- PostgresLsnAdapter output accepted by PostgreSQL
- pg_lsn comparison and arithmetic
- Sync/async round-trip behavior
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.adapters.pg_lsn import PostgresLsnAdapter
from rhosocial.activerecord.backend.impl.postgres.types.pg_lsn import PostgresLsn


LSN_TABLE = "test_lsn_types"
ASYNC_LSN_TABLE = "test_lsn_types_async"


def _requires_pg_lsn(backend):
    if backend.get_server_version() < (9, 4, 0):
        pytest.skip("pg_lsn requires PostgreSQL 9.4+")


async def _requires_pg_lsn_async(backend):
    if await backend.get_server_version() < (9, 4, 0):
        pytest.skip("pg_lsn requires PostgreSQL 9.4+")


def _lsn_literal(value):
    adapter = PostgresLsnAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    return f"'{database_value}'::pg_lsn"


class TestSyncLsnIntegration:
    """Synchronous integration tests for PostgreSQL pg_lsn type."""

    @pytest.fixture
    def lsn_test_table(self, postgres_backend):
        """Create a table containing a pg_lsn column for sync tests."""
        _requires_pg_lsn(postgres_backend)
        postgres_backend.execute(f"DROP TABLE IF EXISTS {LSN_TABLE}")
        postgres_backend.execute(f"""
            CREATE TABLE {LSN_TABLE} (
                id SERIAL PRIMARY KEY,
                lsn_value pg_lsn
            )
        """)
        yield LSN_TABLE
        postgres_backend.execute(f"DROP TABLE IF EXISTS {LSN_TABLE}")

    def test_insert_and_select_lsn_value(self, postgres_backend, lsn_test_table):
        """Insert a PostgresLsn value and verify PostgreSQL returns canonical text."""
        literal = _lsn_literal(PostgresLsn.from_string("16/B374D848"))

        postgres_backend.execute(
            f"INSERT INTO {lsn_test_table} (lsn_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT lsn_value::text AS lsn_value FROM {lsn_test_table} WHERE id = 1"
        )

        assert result["lsn_value"] == "16/B374D848"

    def test_insert_lsn_from_int(self, postgres_backend, lsn_test_table):
        """Pass an integer through the adapter and verify the stored pg_lsn text."""
        literal = _lsn_literal(0x0000001600000001)

        postgres_backend.execute(
            f"INSERT INTO {lsn_test_table} (lsn_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT lsn_value::text AS lsn_value FROM {lsn_test_table}"
        )

        # Normalize LSN zero-padding (PG19beta1 uses 8-digit hex parts)
        parts = result["lsn_value"].split("/")
        assert int(parts[0], 16) == 0x16 and int(parts[1], 16) == 0x1

    def test_lsn_null_round_trip(self, postgres_backend, lsn_test_table):
        """Insert a NULL pg_lsn value and verify the fetched value is None."""
        postgres_backend.execute(
            f"INSERT INTO {lsn_test_table} (lsn_value) VALUES (NULL)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT lsn_value FROM {lsn_test_table} WHERE id = 1"
        )

        assert result["lsn_value"] is None

    def test_lsn_comparison_filter(self, postgres_backend, lsn_test_table):
        """Insert two pg_lsn rows and verify greater-than filtering matches one row."""
        lower = _lsn_literal("16/B374D848")
        higher = _lsn_literal("16/B374D849")

        postgres_backend.execute(
            f"INSERT INTO {lsn_test_table} (lsn_value) VALUES ({lower}), ({higher})"
        )
        result = postgres_backend.fetch_one(f"""
            SELECT COUNT(*) AS match_count
            FROM {lsn_test_table}
            WHERE lsn_value > {lower}
        """)

        assert result["match_count"] == 1

    def test_lsn_subtraction_executes(self, postgres_backend):
        """Subtract two pg_lsn values and verify PostgreSQL returns byte distance."""
        _requires_pg_lsn(postgres_backend)
        result = postgres_backend.fetch_one("""
            SELECT '16/B374D849'::pg_lsn - '16/B374D848'::pg_lsn AS distance
        """)

        assert result["distance"] == 1

    def test_lsn_addition_executes(self, postgres_backend):
        """Add bytes to a pg_lsn value and verify PostgreSQL returns the new LSN."""
        if postgres_backend.get_server_version() < (14, 0, 0):
            pytest.skip("pg_lsn addition requires PostgreSQL 14+")
        result = postgres_backend.fetch_one("""
            SELECT ('16/B374D848'::pg_lsn + 1)::text AS lsn_value
        """)

        assert result["lsn_value"] == "16/B374D849"

    def test_invalid_lsn_rejected_before_insert(self):
        """Pass an invalid LSN string and verify the adapter rejects it before SQL."""
        with pytest.raises(ValueError):
            _lsn_literal("not_an_lsn")


class TestAsyncLsnIntegration:
    """Asynchronous integration tests for PostgreSQL pg_lsn type."""

    @pytest_asyncio.fixture
    async def async_lsn_test_table(self, async_postgres_backend):
        """Create a table containing a pg_lsn column for async tests."""
        await _requires_pg_lsn_async(async_postgres_backend)
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_LSN_TABLE}")
        await async_postgres_backend.execute(f"""
            CREATE TABLE {ASYNC_LSN_TABLE} (
                id SERIAL PRIMARY KEY,
                lsn_value pg_lsn
            )
        """)
        yield ASYNC_LSN_TABLE
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_LSN_TABLE}")

    @pytest.mark.asyncio
    async def test_async_lsn_round_trip(
        self, async_postgres_backend, async_lsn_test_table
    ):
        """Insert a pg_lsn value asynchronously and verify PostgreSQL returns it."""
        literal = _lsn_literal(PostgresLsn.from_string("16/B374D848"))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_lsn_test_table} (lsn_value) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT lsn_value::text AS lsn_value FROM {async_lsn_test_table}"
        )

        assert result["lsn_value"] == "16/B374D848"

    @pytest.mark.asyncio
    async def test_async_lsn_null_round_trip(
        self, async_postgres_backend, async_lsn_test_table
    ):
        """Insert a NULL pg_lsn value asynchronously and verify fetched value is None."""
        await async_postgres_backend.execute(
            f"INSERT INTO {async_lsn_test_table} (lsn_value) VALUES (NULL)"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT lsn_value FROM {async_lsn_test_table}"
        )

        assert result["lsn_value"] is None

    @pytest.mark.asyncio
    async def test_async_lsn_subtraction_executes(self, async_postgres_backend):
        """Subtract pg_lsn values asynchronously and verify PostgreSQL byte distance."""
        await _requires_pg_lsn_async(async_postgres_backend)
        result = await async_postgres_backend.fetch_one("""
            SELECT '16/B374D849'::pg_lsn - '16/B374D848'::pg_lsn AS distance
        """)

        assert result["distance"] == 1
