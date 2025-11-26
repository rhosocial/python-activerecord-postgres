# tests/rhosocial/activerecord_postgres_test/feature/backend/test_async_transaction_backend.py
import pytest
import pytest_asyncio

@pytest_asyncio.fixture
async def setup_test_table(async_postgres_backend):
    # For transactions, we need to manually handle commit/rollback, so turn autocommit off for the backend connection.
    original_autocommit_state = async_postgres_backend._connection.autocommit
    await async_postgres_backend._connection.set_autocommit(False)
    
    try:
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute("DROP TABLE IF EXISTS test_table")
            await async_postgres_backend.execute("""
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    age INT
                )
            """)
        
        yield
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute("DROP TABLE IF EXISTS test_table")
    finally:
        # Restore autocommit state
        await async_postgres_backend._connection.set_autocommit(original_autocommit_state)


@pytest.mark.asyncio
async def test_transaction_commit(async_postgres_backend, setup_test_table):
    """Test transaction commit"""
    async with async_postgres_backend.transaction():
        sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
        params = ("test", 20)
        await async_postgres_backend.execute(sql, params)

    row = await async_postgres_backend.fetch_one("SELECT * FROM test_table WHERE name = %s", ("test",))
    assert row is not None


@pytest.mark.asyncio
async def test_transaction_rollback(async_postgres_backend, setup_test_table):
    """Test transaction rollback"""
    try:
        async with async_postgres_backend.transaction():
            sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
            params = ("test", 20)
            await async_postgres_backend.execute(sql, params)
            raise Exception("Force rollback")
    except Exception:
        pass
    
    row = await async_postgres_backend.fetch_one("SELECT * FROM test_table WHERE name = %s", ("test",))
    assert row is None


@pytest.mark.asyncio
async def test_nested_transaction(async_postgres_backend, setup_test_table):
    """Test nested transactions (savepoints)"""
    async with async_postgres_backend.transaction():
        sql_outer = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
        params_outer = ("outer", 20)
        await async_postgres_backend.execute(sql_outer, params_outer)

        async with async_postgres_backend.transaction():
            sql_inner = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
            params_inner = ("inner", 30)
            await async_postgres_backend.execute(sql_inner, params_inner)

    rows = await async_postgres_backend.fetch_all("SELECT * FROM test_table ORDER BY age")
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_transaction_get_cursor(async_postgres_backend):
    """Test that _get_cursor can be called within a transaction context."""
    original_autocommit_state = async_postgres_backend._connection.autocommit
    await async_postgres_backend._connection.set_autocommit(False)
    try:
        async with async_postgres_backend.transaction():
            cursor = await async_postgres_backend._get_cursor()
            assert cursor is not None
            await cursor.close()
    finally:
        await async_postgres_backend._connection.set_autocommit(original_autocommit_state)
