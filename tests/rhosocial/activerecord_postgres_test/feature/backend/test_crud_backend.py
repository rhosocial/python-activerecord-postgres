# tests/rhosocial/activerecord_postgres_test/feature/backend/test_crud_backend.py

from datetime import datetime
import pytest
from rhosocial.activerecord.backend.errors import QueryError


@pytest.fixture
def setup_test_table(postgres_backend):
    postgres_backend.execute("DROP TABLE IF EXISTS test_table")
    postgres_backend.execute("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            created_at TIMESTAMP
        )
    """)
    yield
    postgres_backend.execute("DROP TABLE IF EXISTS test_table")


def test_insert_success(postgres_backend, setup_test_table):
    """Test successful insertion"""
    sql = "INSERT INTO test_table (name, age, created_at) VALUES (%s, %s, %s)"
    params = ("test", 20, datetime.now())
    result = postgres_backend.execute(sql, params)
    assert result.affected_rows == 1


def test_insert_with_invalid_data(postgres_backend, setup_test_table):
    """Test inserting invalid data"""
    with pytest.raises(QueryError):
        sql = "INSERT INTO test_table (invalid_column) VALUES (%s)"
        params = ("value",)
        postgres_backend.execute(sql, params)


def test_fetch_one(postgres_backend, setup_test_table):
    """Test querying a single record"""
    sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
    params = ("test", 20)
    postgres_backend.execute(sql, params)
    row = postgres_backend.fetch_one("SELECT * FROM test_table WHERE name = %s", ("test",))
    assert row is not None
    assert row["name"] == "test"
    assert row["age"] == 20


def test_fetch_all(postgres_backend, setup_test_table):
    """Test querying multiple records"""
    sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
    params1 = ("test1", 20)
    params2 = ("test2", 30)
    postgres_backend.execute(sql, params1)
    postgres_backend.execute(sql, params2)
    rows = postgres_backend.fetch_all("SELECT * FROM test_table ORDER BY age")
    assert len(rows) == 2
    assert rows[0]["age"] == 20
    assert rows[1]["age"] == 30


def test_update(postgres_backend, setup_test_table):
    """Test updating a record"""
    sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
    params = ("test", 20)
    postgres_backend.execute(sql, params)

    sql = "UPDATE test_table SET age = %s WHERE name = %s"
    params = (21, "test")
    result = postgres_backend.execute(sql, params)
    assert result.affected_rows == 1

    row = postgres_backend.fetch_one("SELECT * FROM test_table WHERE name = %s", ("test",))
    assert row["age"] == 21


def test_delete(postgres_backend, setup_test_table):
    """Test deleting a record"""
    sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
    params = ("test", 20)
    postgres_backend.execute(sql, params)

    sql = "DELETE FROM test_table WHERE name = %s"
    params = ("test",)
    result = postgres_backend.execute(sql, params)
    assert result.affected_rows == 1

    row = postgres_backend.fetch_one("SELECT * FROM test_table WHERE name = %s", ("test",))
    assert row is None


def test_execute_many(postgres_backend, setup_test_table):
    """Test batch insertion"""
    data = [
        ("name1", 20, datetime.now()),
        ("name2", 30, datetime.now()),
        ("name3", 40, datetime.now())
    ]
    result = postgres_backend.execute_many(
        "INSERT INTO test_table (name, age, created_at) VALUES (%s, %s, %s)",
        data
    )
    assert result.affected_rows == 3
    rows = postgres_backend.fetch_all("SELECT * FROM test_table ORDER BY age")
    assert len(rows) == 3
