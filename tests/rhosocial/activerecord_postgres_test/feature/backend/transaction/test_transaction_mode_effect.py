# tests/rhosocial/activerecord_postgres_test/feature/backend/transaction/test_transaction_mode_effect.py
"""
PostgreSQL transaction access mode effect tests.

This module tests the actual effect of READ ONLY and READ WRITE transaction modes.
It verifies that the modes actually affect database operations as expected.
"""
import pytest
import pytest_asyncio
from decimal import Decimal

from rhosocial.activerecord.backend.transaction import TransactionMode


class TestSyncTransactionModeEffect:
    """Synchronous transaction access mode effect tests."""

    @pytest.fixture
    def test_table(self, postgres_backend):
        """Create a test table."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_mode_table")
        postgres_backend.execute("""
            CREATE TABLE test_mode_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_mode_table"
        postgres_backend.execute("DROP TABLE IF EXISTS test_mode_table")

    def test_read_write_mode_allows_insert(self, postgres_backend, test_table):
        """Test that READ WRITE mode allows INSERT operations."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_WRITE

        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
                ("ReadWriteTest", Decimal("100.00"))
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_mode_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "ReadWriteTest"

    def test_read_write_mode_allows_update(self, postgres_backend, test_table):
        """Test that READ WRITE mode allows UPDATE operations."""
        # First insert a row
        postgres_backend.execute(
            "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
            ("Original", Decimal("100.00"))
        )

        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_WRITE

        with postgres_backend.transaction():
            postgres_backend.execute(
                "UPDATE test_mode_table SET amount = %s WHERE name = %s",
                (Decimal("200.00"), "Original")
            )

        rows = postgres_backend.fetch_all("SELECT amount FROM test_mode_table WHERE name = %s", ("Original",))
        assert len(rows) == 1
        assert rows[0]["amount"] == Decimal("200.00")

    def test_read_write_mode_allows_delete(self, postgres_backend, test_table):
        """Test that READ WRITE mode allows DELETE operations."""
        # First insert a row
        postgres_backend.execute(
            "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
            ("ToBeDeleted", Decimal("100.00"))
        )

        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_WRITE

        with postgres_backend.transaction():
            postgres_backend.execute(
                "DELETE FROM test_mode_table WHERE name = %s",
                ("ToBeDeleted",)
            )

        rows = postgres_backend.fetch_all("SELECT * FROM test_mode_table")
        assert len(rows) == 0

    def test_read_only_mode_allows_select(self, postgres_backend, test_table):
        """Test that READ ONLY mode allows SELECT operations."""
        # Insert test data first
        postgres_backend.execute(
            "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
            ("ReadOnlySelect", Decimal("100.00"))
        )

        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        with postgres_backend.transaction():
            rows = postgres_backend.fetch_all("SELECT * FROM test_mode_table")

        assert len(rows) == 1
        assert rows[0]["name"] == "ReadOnlySelect"

    def test_read_only_mode_rejects_insert(self, postgres_backend, test_table):
        """Test that READ ONLY mode rejects INSERT operations.

        PostgreSQL raises an error when attempting to write in READ ONLY mode.
        Error code: 25006 (read_only_sql_transaction)
        """
        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        tx_manager.begin()
        # PostgreSQL raises an error for write operations in READ ONLY mode
        with pytest.raises(Exception) as exc_info:
            postgres_backend.execute(
                "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
                ("ShouldFail", Decimal("100.00"))
            )
        # Verify it's a read-only transaction error
        error_msg = str(exc_info.value).lower()
        assert "read-only" in error_msg or "cannot" in error_msg

        # Transaction is auto-aborted after error, check if rollback is needed
        if tx_manager.is_active:
            tx_manager.rollback()

        # Verify no data was inserted
        rows = postgres_backend.fetch_all("SELECT * FROM test_mode_table")
        assert len(rows) == 0

    def test_read_only_mode_rejects_update(self, postgres_backend, test_table):
        """Test that READ ONLY mode rejects UPDATE operations."""
        # Insert test data first
        postgres_backend.execute(
            "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
            ("Original", Decimal("100.00"))
        )

        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        tx_manager.begin()
        with pytest.raises(Exception) as exc_info:
            postgres_backend.execute(
                "UPDATE test_mode_table SET amount = %s WHERE name = %s",
                (Decimal("200.00"), "Original")
            )
        error_msg = str(exc_info.value).lower()
        assert "read-only" in error_msg or "cannot" in error_msg

        if tx_manager.is_active:
            tx_manager.rollback()

        # Verify data was not modified
        rows = postgres_backend.fetch_all("SELECT amount FROM test_mode_table WHERE name = %s", ("Original",))
        assert rows[0]["amount"] == Decimal("100.00")

    def test_read_only_mode_rejects_delete(self, postgres_backend, test_table):
        """Test that READ ONLY mode rejects DELETE operations."""
        # Insert test data first
        postgres_backend.execute(
            "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
            ("ToBeDeleted", Decimal("100.00"))
        )

        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        tx_manager.begin()
        with pytest.raises(Exception) as exc_info:
            postgres_backend.execute(
                "DELETE FROM test_mode_table WHERE name = %s",
                ("ToBeDeleted",)
            )
        error_msg = str(exc_info.value).lower()
        assert "read-only" in error_msg or "cannot" in error_msg

        if tx_manager.is_active:
            tx_manager.rollback()

        # Verify data was not deleted
        rows = postgres_backend.fetch_all("SELECT * FROM test_mode_table")
        assert len(rows) == 1

    def test_mode_cannot_change_during_active_transaction(self, postgres_backend, test_table):
        """Test that transaction mode cannot be changed during an active transaction."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_WRITE

        with postgres_backend.transaction():
            from rhosocial.activerecord.backend.errors import TransactionError
            with pytest.raises(TransactionError):
                tx_manager.transaction_mode = TransactionMode.READ_ONLY

    def test_mode_persists_across_transactions(self, postgres_backend, test_table):
        """Test that transaction mode setting persists across multiple transactions."""
        tx_manager = postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        # First transaction
        with postgres_backend.transaction():
            rows = postgres_backend.fetch_all("SELECT * FROM test_mode_table")
            assert rows == []

        # Second transaction should still be READ ONLY
        tx_manager.begin()
        with pytest.raises(Exception) as exc_info:
            postgres_backend.execute(
                "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
                ("ShouldFail", Decimal("100.00"))
            )
        error_msg = str(exc_info.value).lower()
        assert "read-only" in error_msg

        if tx_manager.is_active:
            tx_manager.rollback()

    def test_mode_combined_with_isolation_level(self, postgres_backend, test_table):
        """Test that mode can be combined with isolation level."""
        from rhosocial.activerecord.backend.transaction import IsolationLevel

        tx_manager = postgres_backend.transaction_manager
        tx_manager.isolation_level = IsolationLevel.REPEATABLE_READ
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        # Insert test data first (outside transaction)
        postgres_backend.execute(
            "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
            ("CombinedTest", Decimal("100.00"))
        )

        tx_manager.begin()
        # Should be able to read
        rows = postgres_backend.fetch_all("SELECT * FROM test_mode_table")
        assert len(rows) == 1

        # Should not be able to write
        with pytest.raises(Exception) as exc_info:
            postgres_backend.execute(
                "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
                ("ShouldFail", Decimal("200.00"))
            )
        error_msg = str(exc_info.value).lower()
        assert "read-only" in error_msg

        if tx_manager.is_active:
            tx_manager.rollback()


class TestAsyncTransactionModeEffect:
    """Asynchronous transaction access mode effect tests."""

    @pytest_asyncio.fixture
    async def async_test_table(self, async_postgres_backend):
        """Create a test table."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_mode_table")
        await async_postgres_backend.execute("""
            CREATE TABLE test_mode_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                amount DECIMAL(10, 2)
            )
        """)
        yield "test_mode_table"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_mode_table")

    @pytest.mark.asyncio
    async def test_async_read_write_mode_allows_insert(self, async_postgres_backend, async_test_table):
        """Test that READ WRITE mode allows INSERT operations (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_WRITE

        async with async_postgres_backend.transaction():
            await async_postgres_backend.execute(
                "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
                ("AsyncReadWriteTest", Decimal("100.00"))
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_mode_table")
        assert len(rows) == 1
        assert rows[0]["name"] == "AsyncReadWriteTest"

    @pytest.mark.asyncio
    async def test_async_read_only_mode_rejects_insert(self, async_postgres_backend, async_test_table):
        """Test that READ ONLY mode rejects INSERT operations (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        await tx_manager.begin()
        with pytest.raises(Exception) as exc_info:
            await async_postgres_backend.execute(
                "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
                ("ShouldFail", Decimal("100.00"))
            )
        error_msg = str(exc_info.value).lower()
        assert "read-only" in error_msg or "cannot" in error_msg

        if tx_manager.is_active:
            await tx_manager.rollback()

        rows = await async_postgres_backend.fetch_all("SELECT * FROM test_mode_table")
        assert len(rows) == 0

    @pytest.mark.asyncio
    async def test_async_read_only_mode_allows_select(self, async_postgres_backend, async_test_table):
        """Test that READ ONLY mode allows SELECT operations (async)."""
        await async_postgres_backend.execute(
            "INSERT INTO test_mode_table (name, amount) VALUES (%s, %s)",
            ("AsyncReadOnlySelect", Decimal("100.00"))
        )

        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_ONLY

        async with async_postgres_backend.transaction():
            rows = await async_postgres_backend.fetch_all("SELECT * FROM test_mode_table")

        assert len(rows) == 1
        assert rows[0]["name"] == "AsyncReadOnlySelect"

    @pytest.mark.asyncio
    async def test_async_mode_cannot_change_during_active_transaction(self, async_postgres_backend, async_test_table):
        """Test that transaction mode cannot be changed during an active transaction (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        tx_manager.transaction_mode = TransactionMode.READ_WRITE

        async with async_postgres_backend.transaction():
            from rhosocial.activerecord.backend.errors import TransactionError
            with pytest.raises(TransactionError):
                tx_manager.transaction_mode = TransactionMode.READ_ONLY
