# tests/rhosocial/activerecord_postgres_test/feature/backend/test_execute_many.py
"""
PostgreSQL execute_many tests for batch operations.

This module tests execute_many functionality for both sync and async backends.
"""
import pytest
import pytest_asyncio
from decimal import Decimal


class TestSyncExecuteMany:
    """Synchronous execute_many tests for PostgreSQL backend."""

    @pytest.fixture
    def test_table(self, postgres_backend):
        """Create a test table."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_batch_table")
        postgres_backend.execute("""
            CREATE TABLE test_batch_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                age INT,
                balance DECIMAL(10, 2)
            )
        """)
        yield "test_batch_table"
        postgres_backend.execute("DROP TABLE IF EXISTS test_batch_table")

    def test_execute_many_insert(self, postgres_backend, test_table):
        """Test batch insert with execute_many."""
        params_list = [
            ("Alice", 25, Decimal("100.00")),
            ("Bob", 30, Decimal("200.00")),
            ("Charlie", 35, Decimal("300.00")),
        ]
        
        result = postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            params_list
        )
        
        assert result.affected_rows == 3
        assert result.data is None
        
        rows = postgres_backend.fetch_all(
            "SELECT name, age, balance FROM test_batch_table ORDER BY name"
        )
        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"
        assert rows[2]["name"] == "Charlie"

    def test_execute_many_update(self, postgres_backend, test_table):
        """Test batch update with execute_many."""
        postgres_backend.execute(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            ("Alice", 25, Decimal("100.00"))
        )
        postgres_backend.execute(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            ("Bob", 30, Decimal("200.00"))
        )
        
        params_list = [
            (26, Decimal("110.00"), "Alice"),
            (31, Decimal("210.00"), "Bob"),
        ]
        
        result = postgres_backend.execute_many(
            "UPDATE test_batch_table SET age = %s, balance = %s WHERE name = %s",
            params_list
        )
        
        assert result.affected_rows == 2
        
        rows = postgres_backend.fetch_all(
            "SELECT name, age, balance FROM test_batch_table ORDER BY name"
        )
        assert rows[0]["age"] == 26
        assert rows[0]["balance"] == Decimal("110.00")
        assert rows[1]["age"] == 31
        assert rows[1]["balance"] == Decimal("210.00")

    def test_execute_many_empty_params(self, postgres_backend, test_table):
        """Test execute_many with empty params list."""
        result = postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            []
        )
        
        assert result.affected_rows == 0
        
        rows = postgres_backend.fetch_all("SELECT * FROM test_batch_table")
        assert len(rows) == 0

    def test_execute_many_single_param(self, postgres_backend, test_table):
        """Test execute_many with single parameter set."""
        result = postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            [("Single", 40, Decimal("400.00"))]
        )
        
        assert result.affected_rows == 1
        
        rows = postgres_backend.fetch_all("SELECT * FROM test_batch_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "Single"

    def test_execute_many_large_batch(self, postgres_backend, test_table):
        """Test execute_many with large batch."""
        params_list = [
            (f"User{i}", 20 + i, Decimal(f"{100 + i}"))
            for i in range(100)
        ]
        
        result = postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            params_list
        )
        
        assert result.affected_rows == 100
        
        count = postgres_backend.fetch_one("SELECT COUNT(*) as cnt FROM test_batch_table")
        assert count["cnt"] == 100

    def test_execute_many_with_transaction(self, postgres_backend, test_table):
        """Test execute_many within a transaction."""
        params_list = [
            ("TxUser1", 25, Decimal("100.00")),
            ("TxUser2", 30, Decimal("200.00")),
        ]
        
        with postgres_backend.transaction():
            postgres_backend.execute_many(
                "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
                params_list
            )
        
        rows = postgres_backend.fetch_all(
            "SELECT name FROM test_batch_table ORDER BY name"
        )
        assert len(rows) == 2

    def test_execute_many_rollback_on_error(self, postgres_backend, test_table):
        """Test that transaction rolls back on error in batch."""
        params_list = [
            ("Rollback1", 25, Decimal("100.00")),
            ("Rollback2", 30, Decimal("200.00")),
        ]
        
        try:
            with postgres_backend.transaction():
                postgres_backend.execute_many(
                    "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
                    params_list
                )
                raise Exception("Force rollback")
        except Exception:
            pass
        
        rows = postgres_backend.fetch_all("SELECT * FROM test_batch_table")
        assert len(rows) == 0

    def test_execute_many_result_duration(self, postgres_backend, test_table):
        """Test that execute_many returns duration."""
        params_list = [
            (f"User{i}", 20 + i, Decimal(f"{100 + i}"))
            for i in range(10)
        ]
        
        result = postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            params_list
        )
        
        assert result.duration >= 0


class TestAsyncExecuteMany:
    """Asynchronous execute_many tests for PostgreSQL backend."""

    @pytest_asyncio.fixture
    async def async_test_table(self, async_postgres_backend):
        """Create a test table."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_batch_table")
        await async_postgres_backend.execute("""
            CREATE TABLE test_batch_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                age INT,
                balance DECIMAL(10, 2)
            )
        """)
        yield "test_batch_table"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_batch_table")

    @pytest.mark.asyncio
    async def test_async_execute_many_insert(self, async_postgres_backend, async_test_table):
        """Test batch insert with execute_many (async)."""
        params_list = [
            ("AsyncAlice", 25, Decimal("100.00")),
            ("AsyncBob", 30, Decimal("200.00")),
            ("AsyncCharlie", 35, Decimal("300.00")),
        ]
        
        result = await async_postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            params_list
        )
        
        assert result.affected_rows == 3
        assert result.data is None
        
        rows = await async_postgres_backend.fetch_all(
            "SELECT name, age, balance FROM test_batch_table ORDER BY name"
        )
        assert len(rows) == 3
        assert rows[0]["name"] == "AsyncAlice"
        assert rows[1]["name"] == "AsyncBob"
        assert rows[2]["name"] == "AsyncCharlie"

    @pytest.mark.asyncio
    async def test_async_execute_many_update(self, async_postgres_backend, async_test_table):
        """Test batch update with execute_many (async)."""
        await async_postgres_backend.execute(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            ("AsyncAlice", 25, Decimal("100.00"))
        )
        await async_postgres_backend.execute(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            ("AsyncBob", 30, Decimal("200.00"))
        )
        
        params_list = [
            (26, Decimal("110.00"), "AsyncAlice"),
            (31, Decimal("210.00"), "AsyncBob"),
        ]
        
        result = await async_postgres_backend.execute_many(
            "UPDATE test_batch_table SET age = %s, balance = %s WHERE name = %s",
            params_list
        )
        
        assert result.affected_rows == 2
        
        rows = await async_postgres_backend.fetch_all(
            "SELECT name, age, balance FROM test_batch_table ORDER BY name"
        )
        assert rows[0]["age"] == 26
        assert rows[0]["balance"] == Decimal("110.00")
        assert rows[1]["age"] == 31
        assert rows[1]["balance"] == Decimal("210.00")

    @pytest.mark.asyncio
    async def test_async_execute_many_empty_params(self, async_postgres_backend, async_test_table):
        """Test execute_many with empty params list (async)."""
        result = await async_postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            []
        )
        
        assert result.affected_rows == 0
        
        rows = await async_postgres_backend.fetch_all("SELECT * FROM test_batch_table")
        assert len(rows) == 0

    @pytest.mark.asyncio
    async def test_async_execute_many_single_param(self, async_postgres_backend, async_test_table):
        """Test execute_many with single parameter set (async)."""
        result = await async_postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            [("AsyncSingle", 40, Decimal("400.00"))]
        )
        
        assert result.affected_rows == 1
        
        rows = await async_postgres_backend.fetch_all("SELECT * FROM test_batch_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "AsyncSingle"

    @pytest.mark.asyncio
    async def test_async_execute_many_large_batch(self, async_postgres_backend, async_test_table):
        """Test execute_many with large batch (async)."""
        params_list = [
            (f"AsyncUser{i}", 20 + i, Decimal(f"{100 + i}"))
            for i in range(100)
        ]
        
        result = await async_postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            params_list
        )
        
        assert result.affected_rows == 100
        
        count = await async_postgres_backend.fetch_one("SELECT COUNT(*) as cnt FROM test_batch_table")
        assert count["cnt"] == 100

    @pytest.mark.asyncio
    async def test_async_execute_many_with_transaction(self, async_postgres_backend, async_test_table):
        """Test execute_many within a transaction (async)."""
        params_list = [
            ("AsyncTxUser1", 25, Decimal("100.00")),
            ("AsyncTxUser2", 30, Decimal("200.00")),
        ]
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute_many(
                "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
                params_list
            )
        
        rows = await async_postgres_backend.fetch_all(
            "SELECT name FROM test_batch_table ORDER BY name"
        )
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_async_execute_many_rollback_on_error(self, async_postgres_backend, async_test_table):
        """Test that transaction rolls back on error in batch (async)."""
        params_list = [
            ("AsyncRollback1", 25, Decimal("100.00")),
            ("AsyncRollback2", 30, Decimal("200.00")),
        ]
        
        try:
            async with async_postgres_backend.transaction():
                await async_postgres_backend.execute_many(
                    "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
                    params_list
                )
                raise Exception("Force rollback")
        except Exception:
            pass
        
        rows = await async_postgres_backend.fetch_all("SELECT * FROM test_batch_table")
        assert len(rows) == 0

    @pytest.mark.asyncio
    async def test_async_execute_many_result_duration(self, async_postgres_backend, async_test_table):
        """Test that execute_many returns duration (async)."""
        params_list = [
            (f"AsyncUser{i}", 20 + i, Decimal(f"{100 + i}"))
            for i in range(10)
        ]
        
        result = await async_postgres_backend.execute_many(
            "INSERT INTO test_batch_table (name, age, balance) VALUES (%s, %s, %s)",
            params_list
        )
        
        assert result.duration >= 0
