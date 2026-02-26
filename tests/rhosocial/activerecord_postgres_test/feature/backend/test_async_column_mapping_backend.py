# /Users/vistart/PycharmProjects/rhosocial/python-activerecord-postgres/tests/rhosocial/activerecord_postgres_test/feature/backend/test_async_column_mapping_backend.py
import pytest
import pytest_asyncio
from datetime import datetime
import uuid

from rhosocial.activerecord.backend.type_adapter import UUIDAdapter, BooleanAdapter


@pytest_asyncio.fixture
async def setup_mapped_users_table(async_postgres_backend):
    """Async fixture to create and drop a 'mapped_users' table for PostgreSQL."""
    await async_postgres_backend.execute("DROP TABLE IF EXISTS mapped_users")
    await async_postgres_backend.execute("""
        CREATE TABLE mapped_users (
            user_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL,
            user_uuid UUID,
            is_active BOOLEAN
        )
    """)
    yield
    await async_postgres_backend.execute("DROP TABLE IF EXISTS mapped_users")


@pytest.mark.asyncio
async def test_async_update_with_backend(async_postgres_backend, setup_mapped_users_table):
    """
    Tests that an async update operation via execute() works correctly for PostgreSQL.
    """
    backend = async_postgres_backend
    
    await backend.execute("INSERT INTO mapped_users (name, email, created_at, is_active) VALUES (%s, %s, %s, %s)",
                          ("Async Jane PG", "async.jane.pg@example.com", datetime.now(), True))

    sql = "UPDATE mapped_users SET name = %s WHERE user_id = %s"
    params = ("Async Jane Smith PG", 1)
    result = await backend.execute(sql, params)

    assert result.affected_rows == 1

    fetch_result = await backend.execute("SELECT name FROM mapped_users WHERE user_id = 1")
    fetched_row = fetch_result.data[0] if fetch_result.data else None
    assert fetched_row is not None
    assert fetched_row["name"] == "Async Jane Smith PG"


@pytest.mark.asyncio
async def test_async_fetch_with_combined_mapping_and_adapters(async_postgres_backend, setup_mapped_users_table):
    """
    Tests that async execute() correctly applies both column_mapping and column_adapters for PostgreSQL.
    """
    backend = async_postgres_backend
    now = datetime.now()
    test_uuid = uuid.uuid4()

    column_to_field_mapping = {
        "user_id": "pk",
        "name": "full_name",
        "user_uuid": "uuid",
        "is_active": "active"
    }
    
    column_adapters = {
        "user_uuid": (UUIDAdapter(), uuid.UUID),
        "is_active": (BooleanAdapter(), bool)
    }

    await backend.execute(
        "INSERT INTO mapped_users (name, email, created_at, user_uuid, is_active) VALUES (%s, %s, %s, %s, %s)",
        ("Async PG Combined", "asyncpgcombined@example.com", now, test_uuid, True)
    )

    result = await backend.execute(
        "SELECT * FROM mapped_users WHERE user_id = 1",
        column_mapping=column_to_field_mapping,
        column_adapters=column_adapters
    )

    fetched_row = result.data[0] if result.data else None
    assert fetched_row is not None

    # 1. Assert keys are the MAPPED FIELD NAMES
    assert "full_name" in fetched_row
    assert "uuid" in fetched_row
    assert "active" in fetched_row

    # 2. Assert values are the CORRECT PYTHON TYPES
    assert fetched_row["full_name"] == "Async PG Combined"
    assert isinstance(fetched_row["uuid"], uuid.UUID)
    assert fetched_row["uuid"] == test_uuid
    assert isinstance(fetched_row["active"], bool)
    assert fetched_row["active"] is True
