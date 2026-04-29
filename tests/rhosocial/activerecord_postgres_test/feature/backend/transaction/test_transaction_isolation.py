# tests/rhosocial/activerecord_postgres_test/feature/backend/transaction/test_transaction_isolation.py
"""
PostgreSQL transaction isolation level tests.

This module tests transaction isolation levels for both sync and async backends.
"""
import pytest
import pytest_asyncio
from decimal import Decimal

from rhosocial.activerecord.backend.errors import IsolationLevelError
from rhosocial.activerecord.backend.transaction import IsolationLevel


class TestSyncTransactionIsolation:
    """Synchronous transaction isolation level tests."""

    @pytest.fixture
    def test_table(self, postgres_backend):
        """Create a test table."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_isolation_table")
        postgres_backend.execute("""
            CREATE TABLE test_isolation_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_isolation_table"
        postgres_backend.execute("DROP TABLE IF EXISTS test_isolation_table")

    def test_set_isolation_level_before_transaction(self, postgres_backend, test_table):
        """Test setting isolation level before starting a transaction."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("IsolationTest", Decimal("100.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_isolation_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "IsolationTest"

    def test_set_repeatable_read_isolation_level(self, postgres_backend, test_table):
        """Test REPEATABLE READ isolation level."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.REPEATABLE_READ
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("RepeatableTest", Decimal("200.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_isolation_table")
        assert len(rows) == 1

    def test_set_serializable_isolation_level(self, postgres_backend, test_table):
        """Test SERIALIZABLE isolation level."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("SerializableTest", Decimal("300.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_isolation_table")
        assert len(rows) == 1

    def test_set_read_uncommitted_isolation_level(self, postgres_backend, test_table):
        """Test READ UNCOMMITTED isolation level."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.READ_UNCOMMITTED
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("ReadUncommittedTest", Decimal("400.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_isolation_table")
        assert len(rows) == 1

    def test_cannot_change_isolation_during_active_transaction(self, postgres_backend, test_table):
        """Test that isolation level cannot be changed during active transaction."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        
        with postgres_backend.transaction():
            with pytest.raises(IsolationLevelError):
                tx_manager.isolation_level = IsolationLevel.SERIALIZABLE

    def test_get_current_isolation_level(self, postgres_backend, test_table):
        """Test getting current isolation level."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        
        with postgres_backend.transaction():
            level = tx_manager.get_current_isolation_level()
            assert level == IsolationLevel.READ_COMMITTED

    def test_isolation_level_persists_across_transactions(self, postgres_backend, test_table):
        """Test that isolation level setting persists across multiple transactions."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.REPEATABLE_READ
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("Tx1", Decimal("100.00"))
            )

        with postgres_backend.transaction():
            level = tx_manager.get_current_isolation_level()
            assert level == IsolationLevel.REPEATABLE_READ


class TestAsyncTransactionIsolation:
    """Asynchronous transaction isolation level tests."""

    @pytest_asyncio.fixture
    async def async_test_table(self, async_postgres_backend):
        """Create a test table."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_isolation_table")
        await async_postgres_backend.execute("""
            CREATE TABLE test_isolation_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_isolation_table"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_isolation_table")

    @pytest.mark.asyncio
    async def test_async_set_isolation_level_before_transaction(self, async_postgres_backend, async_test_table):
        """Test setting isolation level before starting a transaction (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("AsyncIsolationTest", Decimal("100.00"))
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_isolation_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "AsyncIsolationTest"

    @pytest.mark.asyncio
    async def test_async_set_repeatable_read_isolation_level(self, async_postgres_backend, async_test_table):
        """Test REPEATABLE READ isolation level (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.REPEATABLE_READ
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("AsyncRepeatableTest", Decimal("200.00"))
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_isolation_table")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_async_set_serializable_isolation_level(self, async_postgres_backend, async_test_table):
        """Test SERIALIZABLE isolation level (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("AsyncSerializableTest", Decimal("300.00"))
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_isolation_table")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_async_cannot_change_isolation_during_active_transaction(self, async_postgres_backend, async_test_table):
        """Test that isolation level cannot be changed during active transaction (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        
        async with async_postgres_backend.transaction():
            with pytest.raises(IsolationLevelError):
                tx_manager.isolation_level = IsolationLevel.SERIALIZABLE

    @pytest.mark.asyncio
    async def test_async_get_current_isolation_level(self, async_postgres_backend, async_test_table):
        """Test getting current isolation level (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        
        async with async_postgres_backend.transaction():
            level = await tx_manager.get_current_isolation_level()
            assert level == IsolationLevel.READ_COMMITTED

    @pytest.mark.asyncio
    async def test_async_isolation_level_persists_across_transactions(self, async_postgres_backend, async_test_table):
        """Test that isolation level setting persists across multiple transactions (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.REPEATABLE_READ
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute(
                "INSERT INTO test_isolation_table (name, amount) VALUES (%s, %s)",
                ("AsyncTx1", Decimal("100.00"))
            )

        async with async_postgres_backend.transaction():
            level = await tx_manager.get_current_isolation_level()
            assert level == IsolationLevel.REPEATABLE_READ
