# tests/rhosocial/activerecord_postgres_test/feature/backend/test_advisory_lock.py
"""
PostgreSQL advisory lock tests.

This module tests PostgreSQL's advisory lock functionality:
- Session-level locks
- Transaction-level locks
- Exclusive locks
- Shared locks
- Non-blocking try variants
- Context manager usage
"""

from rhosocial.activerecord.backend.impl.postgres.expression.advisory import (
    AdvisoryLockExpression,
    AdvisoryUnlockExpression,
    AdvisoryUnlockAllExpression,
    TryAdvisoryLockExpression,
)


class TestAdvisoryLockExpression:
    """Test advisory lock expression SQL generation."""

    def test_advisory_lock_expression_basic(self, postgres_backend_single):
        """Test basic advisory lock expression."""
        dialect = postgres_backend_single.dialect

        expr = AdvisoryLockExpression(dialect, key=12345)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_advisory_lock(%s)"
        assert params == (12345,)

    def test_advisory_lock_expression_shared(self, postgres_backend_single):
        """Test shared advisory lock expression."""
        dialect = postgres_backend_single.dialect

        expr = AdvisoryLockExpression(dialect, key=12345, shared=True)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_advisory_lock_shared(%s)"
        assert params == (12345,)

    def test_advisory_lock_expression_transaction_level(self, postgres_backend_single):
        """Test transaction-level advisory lock expression."""
        dialect = postgres_backend_single.dialect

        expr = AdvisoryLockExpression(dialect, key=12345, session=False)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_advisory_xact_lock(%s)"
        assert params == (12345,)

    def test_advisory_lock_expression_two_keys(self, postgres_backend_single):
        """Test advisory lock with two keys."""
        dialect = postgres_backend_single.dialect

        expr = AdvisoryLockExpression(dialect, key=(123, 456))
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_advisory_lock(%s, %s)"
        assert params == (123, 456)

    def test_advisory_unlock_expression(self, postgres_backend_single):
        """Test advisory unlock expression."""
        dialect = postgres_backend_single.dialect

        expr = AdvisoryUnlockExpression(dialect, key=12345)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_advisory_unlock(%s)"
        assert params == (12345,)

    def test_advisory_unlock_all_expression(self, postgres_backend_single):
        """Test advisory unlock all expression."""
        dialect = postgres_backend_single.dialect

        expr = AdvisoryUnlockAllExpression(dialect)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_advisory_unlock_all()"
        assert params == ()

    def test_try_advisory_lock_expression(self, postgres_backend_single):
        """Test try advisory lock expression."""
        dialect = postgres_backend_single.dialect

        expr = TryAdvisoryLockExpression(dialect, key=12345)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_try_advisory_lock(%s)"
        assert params == (12345,)

    def test_try_advisory_lock_expression_shared(self, postgres_backend_single):
        """Test try shared advisory lock expression."""
        dialect = postgres_backend_single.dialect

        expr = TryAdvisoryLockExpression(dialect, key=12345, shared=True)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_try_advisory_lock_shared(%s)"
        assert params == (12345,)


class TestAdvisoryLockBackend:
    """Test advisory lock backend methods."""

    def test_execute_advisory_lock(self, postgres_backend_single):
        """Test acquiring and releasing advisory lock."""
        # Acquire lock
        postgres_backend_single.execute_advisory_lock(key=12345)
        # Release lock
        result = postgres_backend_single.execute_advisory_unlock(key=12345)
        assert result is True

    def test_execute_advisory_lock_two_keys(self, postgres_backend_single):
        """Test advisory lock with two keys."""
        # Acquire lock
        postgres_backend_single.execute_advisory_lock(key=(123, 456))
        # Release lock
        result = postgres_backend_single.execute_advisory_unlock(key=(123, 456))
        assert result is True

    def test_execute_advisory_lock_shared(self, postgres_backend_single):
        """Test shared advisory lock."""
        # Acquire shared lock
        postgres_backend_single.execute_advisory_lock(key=12345, shared=True)
        # Release shared lock (must use shared=True)
        result = postgres_backend_single.execute_advisory_unlock(key=12345, shared=True)
        assert result is True

    def test_try_advisory_lock_success(self, postgres_backend_single):
        """Test try advisory lock when available."""
        result = postgres_backend_single.try_advisory_lock(key=12345)
        assert result is True
        # Clean up
        postgres_backend_single.execute_advisory_unlock(key=12345)

    def test_try_advisory_lock_conflict(self, postgres_backend_single, postgres_control_backend):
        """Test try advisory lock when already locked."""
        # Acquire lock in main backend
        postgres_backend_single.execute_advisory_lock(key=99999)

        try:
            # Try to acquire same lock in control backend (should fail)
            result = postgres_control_backend.try_advisory_lock(key=99999)
            assert result is False
        finally:
            # Clean up
            postgres_backend_single.execute_advisory_unlock(key=99999)

    def test_advisory_unlock_all(self, postgres_backend_single):
        """Test releasing all advisory locks."""
        # Acquire multiple locks
        postgres_backend_single.execute_advisory_lock(key=11111)
        postgres_backend_single.execute_advisory_lock(key=22222)
        postgres_backend_single.execute_advisory_lock(key=33333)

        # Release all
        postgres_backend_single.execute_advisory_unlock_all()

        # Verify locks are released
        result = postgres_backend_single.try_advisory_lock(key=11111)
        assert result is True
        postgres_backend_single.execute_advisory_unlock(key=11111)

    def test_advisory_lock_context_manager(self, postgres_backend_single):
        """Test advisory lock context manager."""
        with postgres_backend_single.advisory_lock(key=12345):
            # Lock should be held
            # Try to acquire from same session - should succeed
            # But if we try from another session it would block
            pass
        # Lock should be released after context exit
        result = postgres_backend_single.try_advisory_lock(key=12345)
        assert result is True
        postgres_backend_single.execute_advisory_unlock(key=12345)


class TestAsyncAdvisoryLockExpression:
    """Async tests for advisory lock expression SQL generation."""

    async def test_async_advisory_lock_expression_basic(self, async_postgres_backend_single):
        """Test basic advisory lock expression (async)."""
        dialect = async_postgres_backend_single.dialect

        expr = AdvisoryLockExpression(dialect, key=12345)
        sql, params = expr.to_sql()
        assert sql == "SELECT pg_advisory_lock(%s)"
        assert params == (12345,)


class TestAsyncAdvisoryLockBackend:
    """Async tests for advisory lock backend methods."""

    async def test_async_execute_advisory_lock(self, async_postgres_backend_single):
        """Test acquiring and releasing advisory lock (async)."""
        # Acquire lock
        await async_postgres_backend_single.execute_advisory_lock(key=12345)
        # Release lock
        result = await async_postgres_backend_single.execute_advisory_unlock(key=12345)
        assert result is True

    async def test_async_try_advisory_lock(self, async_postgres_backend_single):
        """Test try advisory lock (async)."""
        result = await async_postgres_backend_single.try_advisory_lock(key=12345)
        assert result is True
        # Clean up
        await async_postgres_backend_single.execute_advisory_unlock(key=12345)

    async def test_async_advisory_lock_context_manager(self, async_postgres_backend_single):
        """Test advisory lock async context manager."""
        async with async_postgres_backend_single.advisory_lock(key=12345):
            # Lock should be held
            pass
        # Lock should be released after context exit
        result = await async_postgres_backend_single.try_advisory_lock(key=12345)
        assert result is True
        await async_postgres_backend_single.execute_advisory_unlock(key=12345)