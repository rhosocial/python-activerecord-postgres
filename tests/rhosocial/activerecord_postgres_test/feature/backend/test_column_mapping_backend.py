# /Users/vistart/PycharmProjects/rhosocial/python-activerecord-postgres/tests/rhosocial/activerecord_postgres_test/feature/backend/test_column_mapping_backend.py
import pytest
from datetime import datetime
import uuid

from rhosocial.activerecord.backend.type_adapter import UUIDAdapter, BooleanAdapter


@pytest.fixture
def setup_mapped_users_table(postgres_backend):
    """Fixture to create and drop a 'mapped_users' table for PostgreSQL."""
    postgres_backend.execute("DROP TABLE IF EXISTS mapped_users")
    postgres_backend.execute("""
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
    postgres_backend.execute("DROP TABLE IF EXISTS mapped_users")


def test_insert_and_returning_with_mapping(postgres_backend, setup_mapped_users_table):
    """
    Tests that execute() with an INSERT and RETURNING clause correctly applies column_mapping.
    """
    backend = postgres_backend
    now = datetime.now()
    test_uuid = uuid.uuid4()

    column_to_field_mapping = {
        "user_id": "pk",
        "name": "full_name",
        "user_uuid": "uuid",
        "is_active": "active",
        "created_at": "creation_time"
    }

    sql = "INSERT INTO mapped_users (name, email, created_at, user_uuid, is_active) VALUES (%s, %s, %s, %s, %s)"
    params = ("Postgres User", "pg@example.com", now, test_uuid, True)

    result = backend.execute(
        sql=sql,
        params=params,
        returning=True,
        column_mapping=column_to_field_mapping
    )

    assert result.data is not None
    assert len(result.data) == 1
    returned_row = result.data[0]
    
    assert "pk" in returned_row
    assert "full_name" in returned_row
    assert "uuid" in returned_row
    assert "active" in returned_row
    assert returned_row["pk"] == 1
    assert returned_row["full_name"] == "Postgres User"
    assert returned_row["uuid"] == test_uuid
    assert returned_row["active"] is True


def test_update_with_backend(postgres_backend, setup_mapped_users_table):
    """
    Tests that an update operation via execute() works correctly for PostgreSQL.
    """
    backend = postgres_backend
    
    backend.execute("INSERT INTO mapped_users (name, email, created_at, is_active) VALUES (%s, %s, %s, %s)",
                    ("Jane PG", "jane.pg@example.com", datetime.now(), True))

    sql = "UPDATE mapped_users SET name = %s WHERE user_id = %s"
    params = ("Jane Smith PG", 1)
    result = backend.execute(sql, params)

    assert result.affected_rows == 1

    fetch_result = backend.execute("SELECT name FROM mapped_users WHERE user_id = 1")
    fetched_row = fetch_result.data[0] if fetch_result.data else None
    assert fetched_row is not None
    assert fetched_row["name"] == "Jane Smith PG"


def test_fetch_with_combined_mapping_and_adapters(postgres_backend, setup_mapped_users_table):
    """
    Tests that execute() correctly applies both column_mapping and column_adapters for PostgreSQL.
    PostgreSQL's driver (psycopg) often handles common types like UUID and bool automatically,
    so this test primarily validates that the adapter mechanism works if an adapter is provided,
    and that mapping is applied correctly.
    """
    backend = postgres_backend
    now = datetime.now()
    test_uuid = uuid.uuid4()

    column_to_field_mapping = {
        "user_id": "pk",
        "name": "full_name",
        "user_uuid": "uuid",
        "is_active": "active"
    }
    
    # Even if the driver handles it, we can test that providing an adapter still works.
    column_adapters = {
        "user_uuid": (UUIDAdapter(), uuid.UUID),
        "is_active": (BooleanAdapter(), bool)
    }

    backend.execute(
        "INSERT INTO mapped_users (name, email, created_at, user_uuid, is_active) VALUES (%s, %s, %s, %s, %s)",
        ("PG Combined", "pgcombined@example.com", now, test_uuid, True)
    )

    result = backend.execute(
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
    assert fetched_row["full_name"] == "PG Combined"
    assert isinstance(fetched_row["uuid"], uuid.UUID)
    assert fetched_row["uuid"] == test_uuid
    assert isinstance(fetched_row["active"], bool)
    assert fetched_row["active"] is True
