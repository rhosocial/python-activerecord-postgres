# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_transaction_postgres.py
"""
PostgreSQL-specific transaction tests.

This module tests PostgreSQL-specific transaction features:
- DEFERRABLE mode for SERIALIZABLE transactions
- Transaction state tracking
- Connection state checking
"""
import pytest
import pytest_asyncio
from decimal import Decimal

from rhosocial.activerecord.backend.errors import TransactionError
from rhosocial.activerecord.backend.transaction import IsolationLevel, TransactionState


class TestSyncPostgresTransactionFeatures:
    """Synchronous PostgreSQL-specific transaction tests."""

    @pytest.fixture
    def test_table(self, postgres_backend):
        """Create a test table."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_pg_transaction_table")
        postgres_backend.execute("""
            CREATE TABLE test_pg_transaction_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_pg_transaction_table"
        postgres_backend.execute("DROP TABLE IF EXISTS test_pg_transaction_table")

    def test_set_deferrable_on_serializable(self, postgres_backend, test_table):
        """Test setting DEFERRABLE mode on SERIALIZABLE transaction."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager.set_deferrable(True)
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_pg_transaction_table (name, amount) VALUES (%s, %s)",
                ("DeferrableTest", Decimal("100.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_pg_transaction_table")
        assert len(rows) == 1

    def test_set_not_deferrable_on_serializable(self, postgres_backend, test_table):
        """Test setting NOT DEFERRABLE mode on SERIALIZABLE transaction."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager.set_deferrable(False)
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_pg_transaction_table (name, amount) VALUES (%s, %s)",
                ("NotDeferrableTest", Decimal("100.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_pg_transaction_table")
        assert len(rows) == 1

    def test_deferrable_only_affects_serializable(self, postgres_backend, test_table):
        """Test that DEFERRABLE setting only affects SERIALIZABLE transactions."""
        tx_manager = postgres_backend.transaction_manager
        
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        tx_manager.set_deferrable(True)
        
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_pg_transaction_table (name, amount) VALUES (%s, %s)",
                ("NonSerializableDeferrable", Decimal("100.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_pg_transaction_table")
        assert len(rows) == 1

    def test_cannot_set_deferrable_during_active_transaction(self, postgres_backend, test_table):
        """Test that deferrable mode cannot be set during active transaction."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        
        with postgres_backend.transaction():
            with pytest.raises(TransactionError):
                tx_manager.set_deferrable(True)

    def test_transaction_state_tracking(self, postgres_backend, test_table):
        """Test transaction state tracking through lifecycle."""
        tx_manager = postgres_backend.transaction_manager
        
        assert tx_manager.state == TransactionState.INACTIVE
        
        tx_manager.begin()
        assert tx_manager.state == TransactionState.ACTIVE
        
        tx_manager.commit()
        # After commit, base class resets state to INACTIVE when transaction_level == 0
        assert tx_manager.state == TransactionState.INACTIVE
        assert tx_manager.transaction_level == 0

    def test_transaction_state_after_rollback(self, postgres_backend, test_table):
        """Test transaction state after rollback."""
        tx_manager = postgres_backend.transaction_manager
        
        tx_manager.begin()
        assert tx_manager.state == TransactionState.ACTIVE
        
        tx_manager.rollback()
        # After rollback, base class resets state to INACTIVE when transaction_level == 0
        assert tx_manager.state == TransactionState.INACTIVE
        assert tx_manager.transaction_level == 0

    def test_is_active_with_connection_state(self, postgres_backend, test_table):
        """Test is_active property reflects connection state."""
        tx_manager = postgres_backend.transaction_manager
        
        assert not tx_manager.is_active
        
        with postgres_backend.transaction():
            assert tx_manager.is_active
        
        assert not tx_manager.is_active

    def test_transaction_level_tracking(self, postgres_backend, test_table):
        """Test transaction nesting level tracking."""
        tx_manager = postgres_backend.transaction_manager
        
        assert tx_manager.transaction_level == 0
        
        with postgres_backend.transaction():
            assert tx_manager.transaction_level == 1
            
            with postgres_backend.transaction():
                assert tx_manager.transaction_level == 2
            
            assert tx_manager.transaction_level == 1
        
        assert tx_manager.transaction_level == 0

    def test_begin_statement_with_isolation_level(self, postgres_backend, test_table):
        """Test that BEGIN statement includes isolation level."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.REPEATABLE_READ

        begin_sql, _ = tx_manager._build_begin_sql()
        assert "ISOLATION LEVEL REPEATABLE READ" in begin_sql

    def test_begin_statement_with_deferrable(self, postgres_backend, test_table):
        """Test that BEGIN statement includes DEFERRABLE for SERIALIZABLE."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager.set_deferrable(True)

        begin_sql, _ = tx_manager._build_begin_sql()
        assert "DEFERRABLE" in begin_sql


class TestAsyncPostgresTransactionFeatures:
    """Asynchronous PostgreSQL-specific transaction tests."""

    @pytest_asyncio.fixture
    async def async_test_table(self, async_postgres_backend):
        """Create a test table."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_pg_transaction_table")
        await async_postgres_backend.execute("""
            CREATE TABLE test_pg_transaction_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_pg_transaction_table"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_pg_transaction_table")

    @pytest.mark.asyncio
    async def test_async_deferrable_mode_setting(self, async_postgres_backend, async_test_table):
        """Test deferrable mode can be set on SERIALIZABLE transaction (async).
        
        Note: AsyncPostgresTransactionManager sets _is_deferrable directly.
        """
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager._is_deferrable = True
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute(
                "INSERT INTO test_pg_transaction_table (name, amount) VALUES (%s, %s)",
                ("AsyncDeferrableTest", Decimal("100.00"))
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_pg_transaction_table")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_async_not_deferrable_mode_setting(self, async_postgres_backend, async_test_table):
        """Test NOT DEFERRABLE mode can be set on SERIALIZABLE transaction (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager._is_deferrable = False
        
        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute(
                "INSERT INTO test_pg_transaction_table (name, amount) VALUES (%s, %s)",
                ("AsyncNotDeferrableTest", Decimal("100.00"))
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_pg_transaction_table")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_async_transaction_state_tracking(self, async_postgres_backend, async_test_table):
        """Test transaction state tracking through lifecycle (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        assert tx_manager.state == TransactionState.INACTIVE
        
        await tx_manager.begin()
        assert tx_manager.state == TransactionState.ACTIVE
        
        await tx_manager.commit()
        # After commit, base class resets state to INACTIVE when transaction_level == 0
        assert tx_manager.state == TransactionState.INACTIVE
        assert tx_manager.transaction_level == 0

    @pytest.mark.asyncio
    async def test_async_transaction_state_after_rollback(self, async_postgres_backend, async_test_table):
        """Test transaction state after rollback (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        await tx_manager.begin()
        assert tx_manager.state == TransactionState.ACTIVE
        
        await tx_manager.rollback()
        # After rollback, base class resets state to INACTIVE when transaction_level == 0
        assert tx_manager.state == TransactionState.INACTIVE
        assert tx_manager.transaction_level == 0

    @pytest.mark.asyncio
    async def test_async_is_active_with_connection_state(self, async_postgres_backend, async_test_table):
        """Test is_active property reflects connection state (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        assert not tx_manager.is_active
        
        async with async_postgres_backend.transaction():
            assert tx_manager.is_active
        
        assert not tx_manager.is_active

    @pytest.mark.asyncio
    async def test_async_transaction_level_tracking(self, async_postgres_backend, async_test_table):
        """Test transaction nesting level tracking (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        assert tx_manager.transaction_level == 0
        
        async with async_postgres_backend.transaction():
            assert tx_manager.transaction_level == 1
            
            async with async_postgres_backend.transaction():
                assert tx_manager.transaction_level == 2
            
            assert tx_manager.transaction_level == 1
        
        assert tx_manager.transaction_level == 0

    @pytest.mark.asyncio
    async def test_async_begin_statement_with_isolation_level(self, async_postgres_backend, async_test_table):
        """Test that BEGIN statement includes isolation level (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.REPEATABLE_READ

        begin_sql, _ = tx_manager._build_begin_sql()
        assert "ISOLATION LEVEL REPEATABLE READ" in begin_sql

    @pytest.mark.asyncio
    async def test_async_begin_statement_with_deferrable(self, async_postgres_backend, async_test_table):
        """Test that BEGIN statement includes DEFERRABLE for SERIALIZABLE (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager._is_deferrable = True

        begin_sql, _ = tx_manager._build_begin_sql()
        assert "DEFERRABLE" in begin_sql
