# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/types/test_oid_integration.py
"""
Integration tests for PostgreSQL object identifier types with real database.

These tests require a live PostgreSQL connection and test:
- oid storage and retrieval
- tid storage and retrieval
- regclass and regtype name resolution
- Object identifier adapters output accepted by PostgreSQL
- Sync/async round-trip behavior
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.adapters.object_identifier import (
    PostgresOidAdapter,
    PostgresTidAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.types.object_identifier import (
    OID,
    RegClass,
    RegType,
    TID,
)


OID_TABLE = "test_oid_types"
ASYNC_OID_TABLE = "test_oid_types_async"


def _oid_literal(value):
    adapter = PostgresOidAdapter()
    database_value = adapter.to_database(value, int)
    if database_value is None:
        return "NULL"
    return f"{database_value}::oid"


def _tid_literal(value):
    adapter = PostgresTidAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    return f"'{database_value}'::tid"


def _regclass_literal(value):
    adapter = PostgresOidAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    escaped = database_value.replace("'", "''")
    return f"'{escaped}'::regclass"


def _regtype_literal(value):
    adapter = PostgresOidAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    escaped = database_value.replace("'", "''")
    return f"'{escaped}'::regtype"


class TestSyncObjectIdentifierIntegration:
    """Synchronous integration tests for PostgreSQL object identifier types."""

    @pytest.fixture
    def oid_test_table(self, postgres_backend):
        """Create a table containing object identifier columns for sync tests."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {OID_TABLE}")
        postgres_backend.execute(f"""
            CREATE TABLE {OID_TABLE} (
                id SERIAL PRIMARY KEY,
                oid_value oid,
                tid_value tid
            )
        """)
        yield OID_TABLE
        postgres_backend.execute(f"DROP TABLE IF EXISTS {OID_TABLE}")

    def test_insert_and_select_oid_value(self, postgres_backend, oid_test_table):
        """Insert an OID wrapper value and verify PostgreSQL returns the integer OID."""
        literal = _oid_literal(OID(16384))

        postgres_backend.execute(
            f"INSERT INTO {oid_test_table} (oid_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT oid_value::int AS oid_value FROM {oid_test_table} WHERE id = 1"
        )

        assert result["oid_value"] == 16384

    def test_insert_oid_from_int(self, postgres_backend, oid_test_table):
        """Pass an int through the OID adapter and verify the stored OID value."""
        literal = _oid_literal(20000)

        postgres_backend.execute(
            f"INSERT INTO {oid_test_table} (oid_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT oid_value::int AS oid_value FROM {oid_test_table}"
        )

        assert result["oid_value"] == 20000

    def test_insert_and_select_tid_value(self, postgres_backend, oid_test_table):
        """Insert a TID wrapper value and verify PostgreSQL returns the same tuple id."""
        literal = _tid_literal(TID(1, 2))

        postgres_backend.execute(
            f"INSERT INTO {oid_test_table} (tid_value) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT tid_value::text AS tid_value FROM {oid_test_table} WHERE id = 1"
        )

        assert result["tid_value"] == "(1,2)"

    def test_object_identifier_null_round_trip(self, postgres_backend, oid_test_table):
        """Insert NULL object identifier values and verify fetched values are None."""
        postgres_backend.execute(
            f"INSERT INTO {oid_test_table} (oid_value, tid_value) VALUES (NULL, NULL)"
        )
        result = postgres_backend.fetch_one(f"""
            SELECT oid_value, tid_value
            FROM {oid_test_table}
            WHERE id = 1
        """)

        assert result["oid_value"] is None
        assert result["tid_value"] is None

    def test_regclass_resolves_created_table(self, postgres_backend, oid_test_table):
        """Cast a RegClass table name and verify PostgreSQL resolves it to the table."""
        literal = _regclass_literal(RegClass(oid_test_table))

        result = postgres_backend.fetch_one(
            f"SELECT {literal}::text AS regclass_name"
        )

        assert result["regclass_name"] == oid_test_table

    def test_regtype_resolves_builtin_type(self, postgres_backend):
        """Cast a RegType builtin name and verify PostgreSQL resolves it to text."""
        literal = _regtype_literal(RegType("integer"))

        result = postgres_backend.fetch_one(
            f"SELECT {literal}::text AS regtype_name"
        )

        assert result["regtype_name"] == "integer"

    def test_invalid_oid_rejected_before_insert(self):
        """Pass an out-of-range OID and verify the adapter rejects it before SQL."""
        with pytest.raises(ValueError):
            _oid_literal(4294967296)


class TestAsyncObjectIdentifierIntegration:
    """Asynchronous integration tests for PostgreSQL object identifier types."""

    @pytest_asyncio.fixture
    async def async_oid_test_table(self, async_postgres_backend):
        """Create a table containing object identifier columns for async tests."""
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_OID_TABLE}")
        await async_postgres_backend.execute(f"""
            CREATE TABLE {ASYNC_OID_TABLE} (
                id SERIAL PRIMARY KEY,
                oid_value oid,
                tid_value tid
            )
        """)
        yield ASYNC_OID_TABLE
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_OID_TABLE}")

    @pytest.mark.asyncio
    async def test_async_oid_round_trip(
        self, async_postgres_backend, async_oid_test_table
    ):
        """Insert an OID asynchronously and verify PostgreSQL returns the integer value."""
        literal = _oid_literal(OID(30000))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_oid_test_table} (oid_value) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT oid_value::int AS oid_value FROM {async_oid_test_table}"
        )

        assert result["oid_value"] == 30000

    @pytest.mark.asyncio
    async def test_async_tid_round_trip(
        self, async_postgres_backend, async_oid_test_table
    ):
        """Insert a TID asynchronously and verify PostgreSQL returns the tuple id."""
        literal = _tid_literal(TID(2, 3))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_oid_test_table} (tid_value) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT tid_value::text AS tid_value FROM {async_oid_test_table}"
        )

        assert result["tid_value"] == "(2,3)"

    @pytest.mark.asyncio
    async def test_async_object_identifier_null_round_trip(
        self, async_postgres_backend, async_oid_test_table
    ):
        """Insert NULL object identifier values asynchronously and verify None values."""
        await async_postgres_backend.execute(
            f"INSERT INTO {async_oid_test_table} (oid_value, tid_value) VALUES (NULL, NULL)"
        )
        result = await async_postgres_backend.fetch_one(f"""
            SELECT oid_value, tid_value
            FROM {async_oid_test_table}
        """)

        assert result["oid_value"] is None
        assert result["tid_value"] is None
