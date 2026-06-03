# tests/.../feature/backend/postgres/test_enum_integration.py
"""
Integration tests for PostgreSQL ENUM types with real database.

These tests require a live PostgreSQL connection and test:
- CREATE TYPE / DROP TYPE lifecycle
- enum column storage and retrieval
- EnumTypeManager operations against PostgreSQL
- Sync/async round-trip behavior
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.adapters import PostgresEnumAdapter
from rhosocial.activerecord.backend.impl.postgres.types.enum import (
    EnumTypeManager,
    PostgresEnumType,
)


ENUM_TYPE = "test_status_enum"
ENUM_TABLE = "test_enum_types"
ASYNC_ENUM_TYPE = "test_status_enum_async"
ASYNC_ENUM_TABLE = "test_enum_types_async"


def _enum_literal(value, enum_type):
    adapter = PostgresEnumAdapter()
    database_value = adapter.to_database(value, str, options={"enum_type": enum_type})
    if database_value is None:
        return "NULL"
    escaped = database_value.replace("'", "''")
    type_sql, _ = enum_type.to_sql()
    return f"'{escaped}'::{type_sql}"


class TestSyncEnumIntegration:
    """Synchronous integration tests for PostgreSQL enum types."""

    @pytest.fixture
    def enum_type(self, postgres_backend):
        """Create and drop a PostgreSQL enum type for sync tests."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {ENUM_TABLE}")
        postgres_backend.execute(f"DROP TYPE IF EXISTS {ENUM_TYPE}")
        enum_type = PostgresEnumType(
            dialect=postgres_backend.dialect,
            name=ENUM_TYPE,
            values=["pending", "processing", "ready", "failed"],
        )
        manager = EnumTypeManager(postgres_backend)
        manager.create_type(enum_type)
        yield enum_type
        postgres_backend.execute(f"DROP TABLE IF EXISTS {ENUM_TABLE}")
        manager.drop_type(enum_type, if_exists=True)

    @pytest.fixture
    def enum_test_table(self, postgres_backend, enum_type):
        """Create a table containing an enum column for sync tests."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {ENUM_TABLE}")
        type_sql, _ = enum_type.to_sql()
        postgres_backend.execute(f"""
            CREATE TABLE {ENUM_TABLE} (
                id SERIAL PRIMARY KEY,
                status {type_sql}
            )
        """)
        yield ENUM_TABLE
        postgres_backend.execute(f"DROP TABLE IF EXISTS {ENUM_TABLE}")

    def test_enum_type_manager_creates_type(self, postgres_backend, enum_type):
        """Create an enum type and verify PostgreSQL reports it exists."""
        manager = EnumTypeManager(postgres_backend)

        assert manager.type_exists(ENUM_TYPE) is True
        assert manager.get_type_values(ENUM_TYPE) == [
            "pending",
            "processing",
            "ready",
            "failed",
        ]
        assert enum_type.validate_value("ready") is True

    def test_insert_and_select_enum_value(
        self, postgres_backend, enum_test_table, enum_type
    ):
        """Insert an enum value and verify PostgreSQL returns the same label."""
        literal = _enum_literal("ready", enum_type)

        postgres_backend.execute(
            f"INSERT INTO {enum_test_table} (status) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT status::text AS status FROM {enum_test_table} WHERE id = 1"
        )

        assert result["status"] == "ready"

    def test_insert_multiple_enum_values_and_filter(
        self, postgres_backend, enum_test_table, enum_type
    ):
        """Insert two enum values and verify equality filtering matches one row."""
        pending = _enum_literal("pending", enum_type)
        failed = _enum_literal("failed", enum_type)

        postgres_backend.execute(
            f"INSERT INTO {enum_test_table} (status) VALUES ({pending}), ({failed})"
        )
        result = postgres_backend.fetch_one(f"""
            SELECT COUNT(*) AS match_count
            FROM {enum_test_table}
            WHERE status = {pending}
        """)

        assert result["match_count"] == 1

    def test_null_enum_round_trip(self, postgres_backend, enum_test_table):
        """Insert a NULL enum value and verify the fetched value is None."""
        postgres_backend.execute(
            f"INSERT INTO {enum_test_table} (status) VALUES (NULL)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT status FROM {enum_test_table} WHERE id = 1"
        )

        assert result["status"] is None

    def test_invalid_enum_rejected_before_insert(self, enum_type):
        """Pass an invalid enum label and verify the adapter rejects it before SQL."""
        with pytest.raises(ValueError):
            _enum_literal("unknown", enum_type)

    def test_add_enum_value_with_manager(self, postgres_backend, enum_type):
        """Add a new enum label and verify PostgreSQL exposes the updated value list."""
        manager = EnumTypeManager(postgres_backend)

        manager.add_value(enum_type, "archived", after="failed")

        assert manager.get_type_values(ENUM_TYPE) == [
            "pending",
            "processing",
            "ready",
            "failed",
            "archived",
        ]
        assert enum_type.validate_value("archived") is True


class TestAsyncEnumIntegration:
    """Asynchronous integration tests for PostgreSQL enum types."""

    @pytest_asyncio.fixture
    async def async_enum_test_table(self, async_postgres_backend):
        """Create an enum type and table for async tests."""
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_ENUM_TABLE}")
        await async_postgres_backend.execute(f"DROP TYPE IF EXISTS {ASYNC_ENUM_TYPE}")
        await async_postgres_backend.execute(
            f"CREATE TYPE {ASYNC_ENUM_TYPE} AS ENUM ('pending', 'ready', 'failed')"
        )
        await async_postgres_backend.execute(f"""
            CREATE TABLE {ASYNC_ENUM_TABLE} (
                id SERIAL PRIMARY KEY,
                status {ASYNC_ENUM_TYPE}
            )
        """)
        enum_type = PostgresEnumType(
            dialect=async_postgres_backend.dialect,
            name=ASYNC_ENUM_TYPE,
            values=["pending", "ready", "failed"],
        )
        yield ASYNC_ENUM_TABLE, enum_type
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_ENUM_TABLE}")
        await async_postgres_backend.execute(f"DROP TYPE IF EXISTS {ASYNC_ENUM_TYPE}")

    @pytest.mark.asyncio
    async def test_async_enum_round_trip(
        self, async_postgres_backend, async_enum_test_table
    ):
        """Insert an enum value asynchronously and verify the same label is returned."""
        table_name, enum_type = async_enum_test_table
        literal = _enum_literal("ready", enum_type)

        await async_postgres_backend.execute(
            f"INSERT INTO {table_name} (status) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT status::text AS status FROM {table_name}"
        )

        assert result["status"] == "ready"

    @pytest.mark.asyncio
    async def test_async_null_enum_round_trip(
        self, async_postgres_backend, async_enum_test_table
    ):
        """Insert a NULL enum value asynchronously and verify fetched value is None."""
        table_name, _ = async_enum_test_table

        await async_postgres_backend.execute(
            f"INSERT INTO {table_name} (status) VALUES (NULL)"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT status FROM {table_name}"
        )

        assert result["status"] is None
