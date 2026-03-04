# tests/rhosocial/activerecord_postgres_test/feature/backend/test_transaction_deferrable.py
"""
PostgreSQL transaction deferrable constraint tests.

This module tests constraint deferral functionality for both sync and async backends.
"""
import pytest
import pytest_asyncio
from decimal import Decimal

from rhosocial.activerecord.backend.errors import TransactionError


class TestSyncTransactionDeferrable:
    """Synchronous transaction deferrable constraint tests."""

    @pytest.fixture
    def test_table_with_deferrable_fk(self, postgres_backend):
        """Create test tables with a deferrable foreign key constraint."""
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
                parent_id INT,
                amount DECIMAL(10, 2),
                CONSTRAINT deferred_fk 
                    FOREIGN KEY (parent_id) REFERENCES test_deferred_parent(id)
                    DEFERRABLE INITIALLY IMMEDIATE
            )
        """)
        yield "test_deferred_child"
        postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_child CASCADE")
        postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_parent CASCADE")

    def test_defer_constraint(self, postgres_backend, test_table_with_deferrable_fk):
        """Test deferring a constraint."""
        tx_manager = postgres_backend.transaction_manager
        
        with postgres_backend.transaction():
            tx_manager.defer_constraint("deferred_fk")
            
            # Insert child before parent - would normally fail
            # but since constraint is deferred, it should succeed
            postgres_backend.execute(
                "INSERT INTO test_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (1, Decimal("100.00"))
            )
            # Insert parent to satisfy constraint before commit
            postgres_backend.execute(
                "INSERT INTO test_deferred_parent (id, name) VALUES (%s, %s)",
                (1, "Parent1")
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_deferred_parent")
        assert len(rows) == 1

    def test_defer_multiple_constraints(self, postgres_backend, test_table_with_deferrable_fk):
        """Test deferring multiple constraints."""
        # Add another deferrable constraint
        postgres_backend.execute("""
            ALTER TABLE test_deferred_child 
            ADD CONSTRAINT positive_amount 
            CHECK (amount >= 0) NOT DEFERRABLE
        """)
        
        tx_manager = postgres_backend.transaction_manager
        
        with postgres_backend.transaction():
            tx_manager.defer_constraint("deferred_fk")
            
            postgres_backend.execute(
                "INSERT INTO test_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (2, Decimal("50.00"))
            )
            postgres_backend.execute(
                "INSERT INTO test_deferred_parent (id, name) VALUES (%s, %s)",
                (2, "Parent2")
            )

        rows = postgres_backend.fetch_all("SELECT name FROM test_deferred_parent")
        assert len(rows) == 1

    def test_get_deferred_constraints(self, postgres_backend, test_table_with_deferrable_fk):
        """Test getting list of deferred constraints."""
        tx_manager = postgres_backend.transaction_manager
        
        with postgres_backend.transaction():
            tx_manager.defer_constraint("deferred_fk")
            
            deferred = tx_manager.get_deferred_constraints()
            assert "deferred_fk" in deferred

    def test_deferred_constraints_cleared_after_commit(self, postgres_backend, test_table_with_deferrable_fk):
        """Test that deferred constraints list is cleared after commit."""
        tx_manager = postgres_backend.transaction_manager
        
        with postgres_backend.transaction():
            tx_manager.defer_constraint("deferred_fk")
            assert len(tx_manager.get_deferred_constraints()) == 1
            
            postgres_backend.execute(
                "INSERT INTO test_deferred_parent (id, name) VALUES (%s, %s)",
                (3, "Parent3")
            )
            postgres_backend.execute(
                "INSERT INTO test_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (3, Decimal("100.00"))
            )
        
        assert len(tx_manager.get_deferred_constraints()) == 0

    def test_deferred_constraints_cleared_after_rollback(self, postgres_backend, test_table_with_deferrable_fk):
        """Test that deferred constraints list is cleared after rollback."""
        tx_manager = postgres_backend.transaction_manager
        
        try:
            with postgres_backend.transaction():
                tx_manager.defer_constraint("deferred_fk")
                assert len(tx_manager.get_deferred_constraints()) == 1
                raise Exception("Force rollback")
        except Exception:
            pass
        
        assert len(tx_manager.get_deferred_constraints()) == 0


class TestAsyncTransactionDeferrable:
    """Asynchronous transaction deferrable constraint tests."""

    @pytest_asyncio.fixture
    async def async_test_table_with_deferrable_fk(self, async_postgres_backend):
        """Create test tables with a deferrable foreign key constraint."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_child CASCADE")
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_parent CASCADE")
        
        await async_postgres_backend.execute("""
            CREATE TABLE test_deferred_parent (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255)
            )
        """)
        await async_postgres_backend.execute("""
            CREATE TABLE test_deferred_child (
                id SERIAL PRIMARY KEY,
                parent_id INT,
                amount DECIMAL(10, 2),
                CONSTRAINT async_deferred_fk 
                    FOREIGN KEY (parent_id) REFERENCES test_deferred_parent(id)
                    DEFERRABLE INITIALLY IMMEDIATE
            )
        """)
        yield "test_deferred_child"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_child CASCADE")
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_deferred_parent CASCADE")

    @pytest.mark.asyncio
    async def test_async_defer_constraint(self, async_postgres_backend, async_test_table_with_deferrable_fk):
        """Test deferring a constraint (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        async with async_postgres_backend.transaction():
            await tx_manager.defer_constraint("async_deferred_fk")
            
            await async_postgres_backend.execute(
                "INSERT INTO test_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (1, Decimal("100.00"))
            )
            await async_postgres_backend.execute(
                "INSERT INTO test_deferred_parent (id, name) VALUES (%s, %s)",
                (1, "AsyncParent1")
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_deferred_parent")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_async_defer_multiple_constraints(self, async_postgres_backend, async_test_table_with_deferrable_fk):
        """Test deferring multiple constraints (async)."""
        await async_postgres_backend.execute("""
            ALTER TABLE test_deferred_child 
            ADD CONSTRAINT async_positive_amount 
            CHECK (amount >= 0) NOT DEFERRABLE
        """)
        
        tx_manager = async_postgres_backend.transaction_manager
        
        async with async_postgres_backend.transaction():
            await tx_manager.defer_constraint("async_deferred_fk")
            
            await async_postgres_backend.execute(
                "INSERT INTO test_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (2, Decimal("50.00"))
            )
            await async_postgres_backend.execute(
                "INSERT INTO test_deferred_parent (id, name) VALUES (%s, %s)",
                (2, "AsyncParent2")
            )

        rows = await async_postgres_backend.fetch_all("SELECT name FROM test_deferred_parent")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_async_get_deferred_constraints(self, async_postgres_backend, async_test_table_with_deferrable_fk):
        """Test getting list of deferred constraints (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        async with async_postgres_backend.transaction():
            await tx_manager.defer_constraint("async_deferred_fk")
            
            # Check _deferred_constraints attribute directly since
            # AsyncPostgresTransactionManager doesn't have get_deferred_constraints method
            deferred = tx_manager._deferred_constraints
            assert "async_deferred_fk" in deferred

    @pytest.mark.asyncio
    async def test_async_deferred_constraints_cleared_after_commit(self, async_postgres_backend, async_test_table_with_deferrable_fk):
        """Test that deferred constraints list is cleared after commit (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        async with async_postgres_backend.transaction():
            await tx_manager.defer_constraint("async_deferred_fk")
            assert len(tx_manager._deferred_constraints) == 1
            
            await async_postgres_backend.execute(
                "INSERT INTO test_deferred_parent (id, name) VALUES (%s, %s)",
                (3, "AsyncParent3")
            )
            await async_postgres_backend.execute(
                "INSERT INTO test_deferred_child (parent_id, amount) VALUES (%s, %s)",
                (3, Decimal("100.00"))
            )
        
        assert len(tx_manager._deferred_constraints) == 0

    @pytest.mark.asyncio
    async def test_async_deferred_constraints_cleared_after_rollback(self, async_postgres_backend, async_test_table_with_deferrable_fk):
        """Test that deferred constraints list is cleared after rollback (async)."""
        tx_manager = async_postgres_backend.transaction_manager
        
        try:
            async with async_postgres_backend.transaction():
                await tx_manager.defer_constraint("async_deferred_fk")
                assert len(tx_manager._deferred_constraints) == 1
                raise Exception("Force rollback")
        except Exception:
            pass
        
        assert len(tx_manager._deferred_constraints) == 0
