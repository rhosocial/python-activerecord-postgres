# tests/rhosocial/activerecord_postgres_test/feature/backend/test_transaction_advanced_features.py
"""
PostgreSQL advanced transaction feature tests.

This module tests DEFERRABLE transactions, SET TRANSACTION, and SESSION CHARACTERISTICS.
"""
import pytest
import pytest_asyncio
from decimal import Decimal

from rhosocial.activerecord.backend.transaction import IsolationLevel, TransactionMode


class TestSyncDeferrableTransaction:
    """Tests for DEFERRABLE transaction behavior with SERIALIZABLE isolation."""

    @pytest.fixture
    def test_tables(self, postgres_backend):
        """Create test tables with deferred constraint."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_child CASCADE")
        postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_parent CASCADE")

        postgres_backend.execute("""
            CREATE TABLE test_deferred_parent (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        postgres_backend.execute("""
            CREATE TABLE test_deferred_child (
                id SERIAL PRIMARY KEY,
                parent_id INT NOT NULL,
                amount DECIMAL(10, 2),
                CONSTRAINT deferred_fk
                    FOREIGN KEY (parent_id) REFERENCES test_deferred_parent(id)
                    DEFERRABLE INITIALLY IMMEDIATE
            )
        """)
        yield
        postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_child CASCADE")
        postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_parent CASCADE")

    def test_deferrable_serializable_allows_deferred_checking(self, postgres_backend, test_tables):
        """Test DEFERRABLE SERIALIZABLE transaction with deferred constraint checking.

        DEFERRABLE affects when constraint checking occurs:
        - INITIALLY IMMEDIATE: constraints checked at statement level by default
        - SET CONSTRAINTS ... DEFERRED: defer until transaction commit

        This test verifies the transaction manager correctly sets DEFERRABLE mode.
        """
        tx_manager = postgres_backend.transaction_manager

        # Set SERIALIZABLE isolation with DEFERRABLE
        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager.set_deferrable(True)

        with postgres_backend.transaction():
            # Defer the constraint
            tx_manager.defer_constraint("deferred_fk")

            # Insert child before parent - normally would fail immediately
            # but deferred constraint allows it until commit
            postgres_backend.execute(
                "INSERT INTO test_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (1, Decimal("100.00"))
            )

            # Insert parent to satisfy constraint before commit
            postgres_backend.execute(
                "INSERT INTO test_deferred_parent (id, name) VALUES (%s, %s)",
                (1, "Parent1")
            )

        # Verify data was committed
        rows = postgres_backend.fetch_all("SELECT * FROM test_deferred_parent")
        assert len(rows) == 1

    def test_not_deferrable_serializable(self, postgres_backend, test_tables):
        """Test NOT DEFERRABLE SERIALIZABLE transaction."""
        tx_manager = postgres_backend.transaction_manager

        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager.set_deferrable(False)

        # Transaction should start normally
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_deferred_parent (name) VALUES (%s)",
                ("Parent2",)
            )

        rows = postgres_backend.fetch_all("SELECT * FROM test_deferred_parent")
        assert len(rows) == 1

    def test_deferrable_requires_serializable(self, postgres_backend, test_tables):
        """Test that DEFERRABLE setting is only meaningful for SERIALIZABLE isolation.

        The dialect should ignore DEFERRABLE for non-SERIALIZABLE isolation levels.
        """
        tx_manager = postgres_backend.transaction_manager

        # Set READ COMMITTED with DEFERRABLE (should be ignored)
        tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
        tx_manager.set_deferrable(True)

        # Transaction should work normally (DEFERRABLE ignored)
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_deferred_parent (name) VALUES (%s)",
                ("Parent3",)
            )

        rows = postgres_backend.fetch_all("SELECT * FROM test_deferred_parent")
        assert len(rows) == 1


class TestSyncSetTransaction:
    """Tests for SET TRANSACTION statement."""

    @pytest.fixture
    def test_table(self, postgres_backend):
        """Create a test table."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_set_trans_table")
        postgres_backend.execute("""
            CREATE TABLE test_set_trans_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        yield "test_set_trans_table"
        postgres_backend.execute("DROP TABLE IF EXISTS test_set_trans_table")

    def test_set_transaction_isolation_level(self, postgres_backend, test_table):
        """Test setting isolation level via SET TRANSACTION expression.

        SET TRANSACTION sets characteristics for the current transaction.
        It must be called before any queries in the transaction.
        """
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        # Start transaction
        postgres_backend.transaction_manager.begin()

        # Set isolation level using expression
        expr = SetTransactionExpression(postgres_backend.dialect)
        expr.isolation_level(IsolationLevel.SERIALIZABLE)
        sql, params = expr.to_sql()
        postgres_backend.execute(sql, params)

        # Verify isolation level was set
        current_level = postgres_backend.transaction_manager.get_current_isolation_level()
        assert current_level == IsolationLevel.SERIALIZABLE

        postgres_backend.transaction_manager.commit()

    def test_set_transaction_read_only(self, postgres_backend, test_table):
        """Test setting READ ONLY mode via SET TRANSACTION expression."""
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        postgres_backend.transaction_manager.begin()

        expr = SetTransactionExpression(postgres_backend.dialect)
        expr.read_only()
        sql, params = expr.to_sql()
        postgres_backend.execute(sql, params)

        # Verify READ ONLY mode by attempting a write
        with pytest.raises(Exception) as exc_info:
            postgres_backend.execute(
                "INSERT INTO test_set_trans_table (name) VALUES (%s)",
                ("ShouldFail",)
            )
        assert "read-only" in str(exc_info.value).lower()

        # Transaction is aborted after error, rollback to clean up
        try:
            postgres_backend.transaction_manager.rollback()
        except Exception:
            pass  # Transaction may already be terminated


class TestSyncSessionCharacteristics:
    """Tests for SESSION CHARACTERISTICS."""

    @pytest.fixture
    def test_table(self, postgres_backend):
        """Create a test table."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_session_table")
        postgres_backend.execute("""
            CREATE TABLE test_session_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        yield "test_session_table"
        postgres_backend.execute("DROP TABLE IF EXISTS test_session_table")

    def test_session_characteristics_persist_across_transactions(self, postgres_backend, test_table):
        """Test that SESSION CHARACTERISTICS affect all subsequent transactions."""
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        try:
            # Set session characteristics to READ ONLY
            expr = SetTransactionExpression(postgres_backend.dialect)
            expr.session(True).read_only()
            sql, params = expr.to_sql()
            postgres_backend.execute(sql, params)

            # First transaction should be READ ONLY
            tx_manager = postgres_backend.transaction_manager
            tx_manager.begin()
            with pytest.raises(Exception) as exc_info:
                postgres_backend.execute(
                    "INSERT INTO test_session_table (name) VALUES (%s)",
                    ("ShouldFail1",)
                )
            assert "read-only" in str(exc_info.value).lower()
            if tx_manager.is_active:
                tx_manager.rollback()

            # Second transaction should also be READ ONLY
            tx_manager.begin()
            with pytest.raises(Exception) as exc_info:
                postgres_backend.execute(
                    "INSERT INTO test_session_table (name) VALUES (%s)",
                    ("ShouldFail2",)
                )
            assert "read-only" in str(exc_info.value).lower()
            if tx_manager.is_active:
                tx_manager.rollback()
        finally:
            # Reset session characteristics to READ WRITE for teardown
            expr = SetTransactionExpression(postgres_backend.dialect)
            expr.session(True).read_write()
            postgres_backend.execute(expr.to_sql()[0])

    def test_reset_session_characteristics(self, postgres_backend, test_table):
        """Test resetting session characteristics to READ WRITE."""
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        # Set session characteristics to READ ONLY
        expr = SetTransactionExpression(postgres_backend.dialect)
        expr.session(True).read_only()
        postgres_backend.execute(expr.to_sql()[0])

        # Reset to READ WRITE
        expr = SetTransactionExpression(postgres_backend.dialect)
        expr.session(True).read_write()
        postgres_backend.execute(expr.to_sql()[0])

        # Now transactions should allow writes
        with postgres_backend.transaction():
            postgres_backend.execute(
                "INSERT INTO test_session_table (name) VALUES (%s)",
                ("ShouldSucceed",)
            )

        rows = postgres_backend.fetch_all("SELECT * FROM test_session_table")
        assert len(rows) == 1

    def test_session_characteristics_with_isolation_level(self, postgres_backend, test_table):
        """Test session characteristics with isolation level.

        When isolation_level is not explicitly set (None), SESSION CHARACTERISTICS
        should take effect. This test verifies:
        1. SESSION CHARACTERISTICS can set default isolation level
        2. BEGIN without explicit isolation level uses session default
        """
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        try:
            # Set session characteristics with SERIALIZABLE isolation
            expr = SetTransactionExpression(postgres_backend.dialect)
            expr.session(True).isolation_level(IsolationLevel.SERIALIZABLE).read_write()
            postgres_backend.execute(expr.to_sql()[0])

            # Verify isolation_level is None (not explicitly set)
            tx_manager = postgres_backend.transaction_manager
            assert tx_manager.isolation_level is None, \
                "isolation_level should be None to allow SESSION CHARACTERISTICS to take effect"

            # Use begin() directly (not context manager) to avoid implicit isolation level setting
            tx_manager.begin()
            level = tx_manager.get_current_isolation_level()
            assert level == IsolationLevel.SERIALIZABLE, \
                f"Expected SERIALIZABLE from SESSION CHARACTERISTICS, got {level}"
            tx_manager.commit()

            # Now test with explicit isolation level override
            tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
            with postgres_backend.transaction():
                level = tx_manager.get_current_isolation_level()
                assert level == IsolationLevel.READ_COMMITTED, \
                    f"Explicit isolation level should override SESSION CHARACTERISTICS, got {level}"
        finally:
            # Reset to default READ WRITE with READ COMMITTED
            tx_manager = postgres_backend.transaction_manager
            tx_manager.isolation_level = None
            expr = SetTransactionExpression(postgres_backend.dialect)
            expr.session(True).read_write()
            postgres_backend.execute(expr.to_sql()[0])


class TestAsyncDeferrableTransaction:
    """Async tests for DEFERRABLE transaction behavior."""

    @pytest_asyncio.fixture
    async def async_test_tables(self, async_postgres_backend):
        """Create test tables with deferred constraint."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_deferred_child CASCADE")
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_deferred_parent CASCADE")

        await async_postgres_backend.execute("""
            CREATE TABLE test_async_deferred_parent (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        await async_postgres_backend.execute("""
            CREATE TABLE test_async_deferred_child (
                id SERIAL PRIMARY KEY,
                parent_id INT NOT NULL,
                amount DECIMAL(10, 2),
                CONSTRAINT async_deferred_fk
                    FOREIGN KEY (parent_id) REFERENCES test_async_deferred_parent(id)
                    DEFERRABLE INITIALLY IMMEDIATE
            )
        """)
        yield
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_deferred_child CASCADE")
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_deferred_parent CASCADE")

    @pytest.mark.asyncio
    async def test_async_deferrable_serializable(self, async_postgres_backend, async_test_tables):
        """Test DEFERRABLE SERIALIZABLE transaction (async)."""
        tx_manager = async_postgres_backend.transaction_manager

        tx_manager.isolation_level = IsolationLevel.SERIALIZABLE
        tx_manager.set_deferrable(True)

        async with async_postgres_backend.transaction():
            await tx_manager.defer_constraint("async_deferred_fk")

            await async_postgres_backend.execute(
                "INSERT INTO test_async_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (1, Decimal("100.00"))
            )

            await async_postgres_backend.execute(
                "INSERT INTO test_async_deferred_parent (id, name) VALUES (%s, %s)",
                (1, "AsyncParent1")
            )

        rows = await async_postgres_backend.fetch_all("SELECT * FROM test_async_deferred_parent")
        assert len(rows) == 1


class TestAsyncSetTransaction:
    """Async tests for SET TRANSACTION statement."""

    @pytest_asyncio.fixture
    async def async_test_table(self, async_postgres_backend):
        """Create a test table."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_set_trans_table")
        await async_postgres_backend.execute("""
            CREATE TABLE test_async_set_trans_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        yield "test_async_set_trans_table"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_set_trans_table")

    @pytest.mark.asyncio
    async def test_async_set_transaction_isolation_level(self, async_postgres_backend, async_test_table):
        """Test setting isolation level via SET TRANSACTION (async)."""
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        await async_postgres_backend.transaction_manager.begin()

        expr = SetTransactionExpression(async_postgres_backend.dialect)
        expr.isolation_level(IsolationLevel.SERIALIZABLE)
        sql, params = expr.to_sql()
        await async_postgres_backend.execute(sql, params)

        current_level = await async_postgres_backend.transaction_manager.get_current_isolation_level()
        assert current_level == IsolationLevel.SERIALIZABLE

        await async_postgres_backend.transaction_manager.commit()

    @pytest.mark.asyncio
    async def test_async_set_transaction_read_only(self, async_postgres_backend, async_test_table):
        """Test setting READ ONLY mode via SET TRANSACTION (async)."""
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        await async_postgres_backend.transaction_manager.begin()

        expr = SetTransactionExpression(async_postgres_backend.dialect)
        expr.read_only()
        await async_postgres_backend.execute(expr.to_sql()[0])

        with pytest.raises(Exception) as exc_info:
            await async_postgres_backend.execute(
                "INSERT INTO test_async_set_trans_table (name) VALUES (%s)",
                ("ShouldFail",)
            )
        assert "read-only" in str(exc_info.value).lower()

        # Transaction is aborted after error, rollback to clean up
        try:
            await async_postgres_backend.transaction_manager.rollback()
        except Exception:
            pass  # Transaction may already be terminated


class TestAsyncSessionCharacteristics:
    """Async tests for SESSION CHARACTERISTICS."""

    @pytest_asyncio.fixture
    async def async_test_table(self, async_postgres_backend):
        """Create a test table."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_session_table")
        await async_postgres_backend.execute("""
            CREATE TABLE test_async_session_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        yield "test_async_session_table"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_async_session_table")

    @pytest.mark.asyncio
    async def test_async_session_characteristics_persist(self, async_postgres_backend, async_test_table):
        """Test that SESSION CHARACTERISTICS affect subsequent transactions (async)."""
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        try:
            expr = SetTransactionExpression(async_postgres_backend.dialect)
            expr.session(True).read_only()
            await async_postgres_backend.execute(expr.to_sql()[0])

            tx_manager = async_postgres_backend.transaction_manager
            await tx_manager.begin()
            with pytest.raises(Exception) as exc_info:
                await async_postgres_backend.execute(
                    "INSERT INTO test_async_session_table (name) VALUES (%s)",
                    ("ShouldFail",)
                )
            assert "read-only" in str(exc_info.value).lower()
            if tx_manager.is_active:
                await tx_manager.rollback()

            # Second transaction also READ ONLY
            await tx_manager.begin()
            with pytest.raises(Exception) as exc_info:
                await async_postgres_backend.execute(
                    "INSERT INTO test_async_session_table (name) VALUES (%s)",
                    ("ShouldFail2",)
                )
            assert "read-only" in str(exc_info.value).lower()
            if tx_manager.is_active:
                await tx_manager.rollback()
        finally:
            # Reset session characteristics to READ WRITE for teardown
            expr = SetTransactionExpression(async_postgres_backend.dialect)
            expr.session(True).read_write()
            await async_postgres_backend.execute(expr.to_sql()[0])

    @pytest.mark.asyncio
    async def test_async_session_characteristics_with_isolation_level(self, async_postgres_backend, async_test_table):
        """Test session characteristics with isolation level (async).

        When isolation_level is not explicitly set (None), SESSION CHARACTERISTICS
        should take effect. This test verifies:
        1. SESSION CHARACTERISTICS can set default isolation level
        2. BEGIN without explicit isolation level uses session default
        """
        from rhosocial.activerecord.backend.expression.transaction import SetTransactionExpression

        try:
            # Set session characteristics with SERIALIZABLE isolation
            expr = SetTransactionExpression(async_postgres_backend.dialect)
            expr.session(True).isolation_level(IsolationLevel.SERIALIZABLE).read_write()
            await async_postgres_backend.execute(expr.to_sql()[0])

            # Verify isolation_level is None (not explicitly set)
            tx_manager = async_postgres_backend.transaction_manager
            assert tx_manager.isolation_level is None, \
                "isolation_level should be None to allow SESSION CHARACTERISTICS to take effect"

            # Use begin() directly (not context manager) to avoid implicit isolation level setting
            await tx_manager.begin()
            level = await tx_manager.get_current_isolation_level()
            assert level == IsolationLevel.SERIALIZABLE, \
                f"Expected SERIALIZABLE from SESSION CHARACTERISTICS, got {level}"
            await tx_manager.commit()

            # Now test with explicit isolation level override
            tx_manager.isolation_level = IsolationLevel.READ_COMMITTED
            async with async_postgres_backend.transaction():
                level = await tx_manager.get_current_isolation_level()
                assert level == IsolationLevel.READ_COMMITTED, \
                    f"Explicit isolation level should override SESSION CHARACTERISTICS, got {level}"
        finally:
            # Reset to default READ WRITE with no explicit isolation level
            tx_manager = async_postgres_backend.transaction_manager
            tx_manager.isolation_level = None
            expr = SetTransactionExpression(async_postgres_backend.dialect)
            expr.session(True).read_write()
            await async_postgres_backend.execute(expr.to_sql()[0])
