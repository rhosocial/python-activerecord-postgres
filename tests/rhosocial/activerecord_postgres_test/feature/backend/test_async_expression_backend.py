# tests/rhosocial/activerecord_postgres_test/feature/backend/test_async_expression_backend.py
import pytest
import pytest_asyncio
from datetime import datetime

from rhosocial.activerecord.backend.errors import QueryError


@pytest_asyncio.fixture
async def setup_test_table(async_postgres_backend):
    await async_postgres_backend.execute("DROP TABLE IF EXISTS test_table")
    await async_postgres_backend.execute("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            created_at TIMESTAMP
        )
    """)
    yield
    await async_postgres_backend.execute("DROP TABLE IF EXISTS test_table")


@pytest.mark.asyncio
async def test_update_with_expression(async_postgres_backend, setup_test_table):
    """Test updating with an expression"""
    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        ("test_user", 20)
    )

    result = await async_postgres_backend.execute(
        "UPDATE test_table SET age = %s WHERE name = %s",
        (async_postgres_backend.create_expression("age + 1"), "test_user")
    )

    assert result.affected_rows == 1

    row = await async_postgres_backend.fetch_one(
        "SELECT age FROM test_table WHERE name = %s",
        ("test_user",)
    )
    assert row["age"] == 21


@pytest.mark.asyncio
async def test_multiple_expressions(async_postgres_backend, setup_test_table):
    """Test using multiple expressions in the same SQL"""
    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        ("test_user", 20)
    )

    result = await async_postgres_backend.execute(
        "UPDATE test_table SET age = %s, name = %s WHERE name = %s",
        (
            async_postgres_backend.create_expression("age + 10"),
            async_postgres_backend.create_expression("CONCAT(name, '_updated')"),
            "test_user"
        )
    )

    assert result.affected_rows == 1

    row = await async_postgres_backend.fetch_one(
        "SELECT name, age FROM test_table WHERE name = %s",
        ("test_user_updated",)
    )
    assert row["age"] == 30
    assert row["name"] == "test_user_updated"


@pytest.mark.asyncio
async def test_mixed_params_and_expressions(async_postgres_backend, setup_test_table):
    """Test mixing regular parameters and expressions"""
    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        ("test_user", 20)
    )

    result = await async_postgres_backend.execute(
        "UPDATE test_table SET age = %s, name = %s WHERE name = %s AND age >= %s",
        (
            async_postgres_backend.create_expression("age * 2"),
            "new_name",
            "test_user",
            18
        )
    )

    assert result.affected_rows == 1

    row = await async_postgres_backend.fetch_one(
        "SELECT name, age FROM test_table WHERE name = %s",
        ("new_name",)
    )
    assert row["age"] == 40
    assert row["name"] == "new_name"


@pytest.mark.asyncio
async def test_expression_with_placeholder(async_postgres_backend, setup_test_table):
    """Test expression containing a question mark placeholder, which is not supported by psycopg3's format style."""
    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        ("test_user", 20)
    )
    with pytest.raises(QueryError):
        await async_postgres_backend.execute(
            "UPDATE test_table SET age = %s WHERE name = %s",
            (async_postgres_backend.create_expression("age + ?"), "test_user")
        )

@pytest.mark.asyncio
async def test_expression_in_subquery(async_postgres_backend, setup_test_table):
    """Test using expression in subquery"""
    result = await async_postgres_backend.execute_many(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        [("user1", 20), ("user2", 30)]
    )
    assert result.affected_rows == 2, "Should insert two records"

    rows = await async_postgres_backend.fetch_all(
        "SELECT * FROM test_table ORDER BY age"
    )
    assert len(rows) == 2, "Should have two records"
    assert rows[0]["age"] == 20, "First record age should be 20"
    assert rows[1]["age"] == 30, "Second record age should be 30"

    result = await async_postgres_backend.execute(
        """
        SELECT *
        FROM test_table
        WHERE age > %s
          AND age < %s
        """,
        (
            async_postgres_backend.create_expression("(SELECT MIN(age) FROM test_table)"),
            async_postgres_backend.create_expression("(SELECT MAX(age) FROM test_table)")
        )
    )

    assert len(result.data) == 0, "Should not have matching records since condition is MIN < age < MAX"


@pytest.mark.asyncio
async def test_expression_in_insert(async_postgres_backend, setup_test_table):
    """Test using expression in INSERT statement"""
    max_age_result = await async_postgres_backend.fetch_one("SELECT MAX(age) as max_age FROM test_table")
    max_age = max_age_result["max_age"] if max_age_result and max_age_result["max_age"] is not None else 0

    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age, created_at) VALUES (%s, %s, %s)",
        (
            "test_user",
            max_age + 1,
            async_postgres_backend.create_expression("CURRENT_TIMESTAMP")
        )
    )

    row = await async_postgres_backend.fetch_one(
        "SELECT * FROM test_table WHERE name = %s",
        ("test_user",)
    )

    assert row["age"] == max_age + 1
    assert isinstance(row["created_at"], datetime)


@pytest.mark.asyncio
async def test_complex_expression(async_postgres_backend, setup_test_table):
    """Test complex expression"""
    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        ("test_user", 20)
    )

    result = await async_postgres_backend.execute(
        "UPDATE test_table SET age = %s WHERE name = %s",
        (
            async_postgres_backend.create_expression("""
                CASE 
                    WHEN age < 18 THEN 18 
                    WHEN age > 60 THEN 60 
                    ELSE age + 5 
                END
            """),
            "test_user"
        )
    )

    assert result.affected_rows == 1

    row = await async_postgres_backend.fetch_one(
        "SELECT age FROM test_table WHERE name = %s",
        ("test_user",)
    )
    assert row["age"] == 25


@pytest.mark.asyncio
async def test_invalid_expression(async_postgres_backend, setup_test_table):
    """Test invalid expression"""
    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        ("test_user", 20)
    )

    with pytest.raises(QueryError):
        await async_postgres_backend.execute(
            "UPDATE test_table SET age = %s WHERE name = %s",
            (async_postgres_backend.create_expression("invalid_column + 1"), "test_user")
        )


@pytest.mark.asyncio
async def test_expression_count_mismatch(async_postgres_backend, setup_test_table):
    """Test parameter count mismatch scenario"""
    await async_postgres_backend.execute(
        "INSERT INTO test_table (name, age) VALUES (%s, %s)",
        ("test_user", 20)
    )

    with pytest.raises(ValueError, match="Parameter count mismatch: SQL has more placeholders"):
        await async_postgres_backend.execute(
            "UPDATE test_table SET age = %s WHERE name = %s AND age = %s",
            (async_postgres_backend.create_expression("age + 1"), "test_user")
        )

    with pytest.raises(ValueError, match="Parameter count mismatch: Too many parameters provided"):
        await async_postgres_backend.execute(
            "UPDATE test_table SET age = %s WHERE name = %s",
            (async_postgres_backend.create_expression("age + 1"), "test_user", 20)
        )
