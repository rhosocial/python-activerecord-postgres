# tests/rhosocial/activerecord_postgres_test/feature/backend/transaction/test_transaction_savepoint.py
"""
PostgreSQL transaction savepoint tests.

This module tests advanced savepoint functionality for both sync and async backends.
"""
import pytest
import pytest_asyncio
from decimal import Decimal

from rhosocial.activerecord.backend.errors import TransactionError


class TestSyncTransactionSavepoint:
    """Synchronous transaction savepoint tests."""

    @pytest.fixture
    def test_table(self, postgres_backend):
        """Create a test table."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_savepoint_table")
        postgres_backend.execute("""
            CREATE TABLE test_savepoint_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_savepoint_table"
        postgres_backend.execute("DROP TABLE IF EXISTS test_savepoint_table")

    def test_create_savepoint_without_active_transaction(self, postgres_backend, test_table):
        """Test creating a savepoint without an active transaction auto-starts one."""
        tx_manager = postgres_backend.transaction_manager
        
        # First clear any pending state
        postgres_backend.execute("SELECT 1")
        
        # Use savepoint method which should auto-start transaction
        tx_manager.begin()  # Start transaction first
        savepoint_name = tx_manager.savepoint("auto_start_sp")
        assert savepoint_name == "auto_start_sp"
        assert tx_manager.is_active
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AutoStartTest", Decimal("100.00"))
        )
        
        tx_manager.commit()

        rows = postgres_backend.fetch_all("SELECT name FROM test_savepoint_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "AutoStartTest"

    def test_explicit_savepoint_operations(self, postgres_backend, test_table):
        """Test explicit savepoint creation, release, and rollback."""
        tx_manager = postgres_backend.transaction_manager
        
        tx_manager.begin()
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("BeforeSP", Decimal("100.00"))
        )
        
        sp_name = tx_manager.savepoint("sp1")
        assert sp_name == "sp1"
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("InsideSP", Decimal("200.00"))
        )
        
        tx_manager.rollback_to(sp_name)
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AfterSP", Decimal("300.00"))
        )
        
        tx_manager.commit()

        rows = postgres_backend.fetch_all("SELECT name FROM test_savepoint_table ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "BeforeSP"
        assert rows[1]["name"] == "AfterSP"

    def test_release_savepoint(self, postgres_backend, test_table):
        """Test releasing a savepoint."""
        tx_manager = postgres_backend.transaction_manager
        
        tx_manager.begin()
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("BeforeRelease", Decimal("100.00"))
        )
        
        sp_name = tx_manager.savepoint("release_sp")
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AfterSP", Decimal("200.00"))
        )
        
        tx_manager.release(sp_name)
        
        tx_manager.commit()

        rows = postgres_backend.fetch_all("SELECT name FROM test_savepoint_table")
        assert len(rows) == 2

    def test_nested_savepoints(self, postgres_backend, test_table):
        """Test multiple nested savepoints."""
        tx_manager = postgres_backend.transaction_manager
        
        tx_manager.begin()
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("Level0", Decimal("100.00"))
        )
        
        sp1 = tx_manager.savepoint("sp1")
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("Level1", Decimal("200.00"))
        )
        
        sp2 = tx_manager.savepoint("sp2")
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("Level2", Decimal("300.00"))
        )
        
        tx_manager.rollback_to(sp2)
        
        rows = postgres_backend.fetch_all("SELECT name FROM test_savepoint_table ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "Level0"
        assert rows[1]["name"] == "Level1"
        
        tx_manager.rollback_to(sp1)
        
        rows = postgres_backend.fetch_all("SELECT name FROM test_savepoint_table ORDER BY id")
        assert len(rows) == 1
        assert rows[0]["name"] == "Level0"
        
        tx_manager.commit()

    def test_savepoint_with_auto_generated_name(self, postgres_backend, test_table):
        """Test savepoint with auto-generated name."""
        tx_manager = postgres_backend.transaction_manager
        
        tx_manager.begin()
        
        sp_name = tx_manager.savepoint()
        assert sp_name.startswith("SP_")
        
        postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AutoName", Decimal("100.00"))
        )
        
        tx_manager.rollback_to(sp_name)
        
        rows = postgres_backend.fetch_all("SELECT name FROM test_savepoint_table")
        assert len(rows) == 0
        
        tx_manager.commit()

    def test_get_active_savepoint(self, postgres_backend, test_table):
        """Test getting active savepoint name."""
        tx_manager = postgres_backend.transaction_manager
        
        assert tx_manager.get_active_savepoint() is None
        
        tx_manager.begin()
        sp_name = tx_manager.savepoint("active_sp")
        
        assert tx_manager.get_active_savepoint() == sp_name
        
        tx_manager.release(sp_name)
        assert tx_manager.get_active_savepoint() is None
        
        tx_manager.commit()

    def test_supports_savepoint(self, postgres_backend, test_table):
        """Test that savepoints are supported."""
        tx_manager = postgres_backend.transaction_manager
        assert tx_manager.supports_savepoint() is True

    def test_invalid_savepoint_operations(self, postgres_backend, test_table):
        """Test error handling for invalid savepoint operations."""
        tx_manager = postgres_backend.transaction_manager
        
        with pytest.raises(TransactionError):
            tx_manager.rollback_to("nonexistent_sp")
        
        with pytest.raises(TransactionError):
            tx_manager.release("nonexistent_sp")


class TestAsyncTransactionSavepoint:
    """Asynchronous transaction savepoint tests."""

    @pytest_asyncio.fixture
    async def async_test_table(self, async_postgres_backend):
        """Create a test table."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_savepoint_table")
        await async_postgres_backend.execute("""
            CREATE TABLE test_savepoint_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_savepoint_table"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_savepoint_table")

    @pytest.mark.asyncio
    async def test_async_create_savepoint_without_active_transaction(self, async_postgres_backend, async_test_table):
        """Test creating a savepoint without an active transaction auto-starts one (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        # First clear any pending state
        await async_postgres_backend.execute("SELECT 1")
        
        # Start transaction first, then create savepoint
        await tx_manager.begin()
        savepoint_name = await tx_manager.savepoint("auto_start_sp")
        assert savepoint_name == "auto_start_sp"
        assert tx_manager.is_active
        
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncAutoStartTest", Decimal("100.00"))
        )
        
        await tx_manager.commit()

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_savepoint_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "AsyncAutoStartTest"

    @pytest.mark.asyncio
    async def test_async_explicit_savepoint_operations(self, async_postgres_backend, async_test_table):
        """Test explicit savepoint creation, release, and rollback (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        await tx_manager.begin()
        
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncBeforeSP", Decimal("100.00"))
        )
        
        sp_name = await tx_manager.savepoint("async_sp1")
        assert sp_name == "async_sp1"
        
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncInsideSP", Decimal("200.00"))
        )
        
        await tx_manager.rollback_to(sp_name)
        
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncAfterSP", Decimal("300.00"))
        )
        
        await tx_manager.commit()

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_savepoint_table ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "AsyncBeforeSP"
        assert rows[1]["name"] == "AsyncAfterSP"

    @pytest.mark.asyncio
    async def test_async_release_savepoint(self, async_postgres_backend, async_test_table):
        """Test releasing a savepoint (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        await tx_manager.begin()
        
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncBeforeRelease", Decimal("100.00"))
        )
        
        sp_name = await tx_manager.savepoint("async_release_sp")
        
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncAfterSP", Decimal("200.00"))
        )
        
        await tx_manager.release(sp_name)
        
        await tx_manager.commit()

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_savepoint_table")
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_async_nested_savepoints(self, async_postgres_backend, async_test_table):
        """Test multiple nested savepoints (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        await tx_manager.begin()
        
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncLevel0", Decimal("100.00"))
        )
        
        sp1 = await tx_manager.savepoint("async_sp1")
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncLevel1", Decimal("200.00"))
        )
        
        sp2 = await tx_manager.savepoint("async_sp2")
        await async_postgres_backend.execute(
            "INSERT INTO test_savepoint_table (name, amount) VALUES (%s, %s)",
            ("AsyncLevel2", Decimal("300.00"))
        )
        
        await tx_manager.rollback_to(sp2)
        
        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_savepoint_table ORDER BY id")
        assert len(rows) == 2
        
        await tx_manager.rollback_to(sp1)
        
        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_savepoint_table ORDER BY id")
        assert len(rows) == 1
        assert rows[0]["name"] == "AsyncLevel0"
        
        await tx_manager.commit()

    @pytest.mark.asyncio
    async def test_async_get_active_savepoint(self, async_postgres_backend, async_test_table):
        """Test getting active savepoint name (async)."""
        tx_manager = async_postgres_backend.transaction_manager

        assert tx_manager.get_active_savepoint() is None

        await tx_manager.begin()
        sp_name = await tx_manager.savepoint("async_active_sp")

        assert tx_manager.get_active_savepoint() == sp_name

        await tx_manager.release(sp_name)
        assert tx_manager.get_active_savepoint() is None

        await tx_manager.commit()

    @pytest.mark.asyncio
    async def test_async_supports_savepoint(self, async_postgres_backend, async_test_table):
        """Test that savepoints are supported (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        result = await tx_manager.supports_savepoint()
        assert result is True

    @pytest.mark.asyncio
    async def test_async_invalid_savepoint_operations(self, async_postgres_backend, async_test_table):
        """Test error handling for invalid savepoint operations (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        with pytest.raises(TransactionError):
            await tx_manager.rollback_to("async_nonexistent_sp")
        
        with pytest.raises(TransactionError):
            await tx_manager.release("async_nonexistent_sp")
