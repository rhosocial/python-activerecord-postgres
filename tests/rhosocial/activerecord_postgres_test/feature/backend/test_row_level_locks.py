# tests/rhosocial/activerecord_postgres_test/feature/query/test_row_level_locks.py
"""
PostgreSQL row-level lock tests.

This module tests PostgreSQL's advanced row-level locking features:
- FOR UPDATE (exclusive lock)
- FOR NO KEY UPDATE (weaker exclusive lock)
- FOR SHARE (shared lock)
- FOR KEY SHARE (weakest shared lock)

Version requirements:
- FOR UPDATE: All PostgreSQL versions
- FOR NO KEY UPDATE: PostgreSQL 9.0+
- FOR SHARE: PostgreSQL 9.0+
- FOR KEY SHARE: PostgreSQL 9.3+
- SKIP LOCKED: PostgreSQL 9.5+
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression.query_parts import (
    ForUpdateClause,
    LockStrength,
)
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class TestRowLevelLockStrength:
    """Test PostgreSQL row-level lock strength support."""

    def test_supports_for_update(self, postgres_backend_single):
        """Test that FOR UPDATE is always supported."""
        dialect = postgres_backend_single.dialect
        assert dialect.supports_for_update() is True
        assert dialect.supports_lock_strength(LockStrength.UPDATE) is True

    def test_supports_for_share(self, postgres_backend_single):
        """Test FOR SHARE support (PostgreSQL 9.0+)."""
        dialect = postgres_backend_single.dialect
        if dialect.version >= (9, 0, 0):
            assert dialect.supports_for_share() is True
            assert dialect.supports_lock_strength(LockStrength.SHARE) is True
        else:
            assert dialect.supports_for_share() is False

    def test_supports_for_no_key_update(self, postgres_backend_single):
        """Test FOR NO KEY UPDATE support (PostgreSQL 9.0+)."""
        dialect = postgres_backend_single.dialect
        if dialect.version >= (9, 0, 0):
            assert dialect.supports_for_no_key_update() is True
            assert dialect.supports_lock_strength(LockStrength.NO_KEY_UPDATE) is True
        else:
            assert dialect.supports_for_no_key_update() is False

    def test_supports_for_key_share(self, postgres_backend_single):
        """Test FOR KEY SHARE support (PostgreSQL 9.3+)."""
        dialect = postgres_backend_single.dialect
        if dialect.version >= (9, 3, 0):
            assert dialect.supports_for_key_share() is True
            assert dialect.supports_lock_strength(LockStrength.KEY_SHARE) is True
        else:
            assert dialect.supports_for_key_share() is False


class TestForUpdateClauseFormatting:
    """Test FOR UPDATE clause SQL formatting."""

    def test_for_update_basic(self, postgres_backend_single):
        """Test basic FOR UPDATE clause."""
        dialect = postgres_backend_single.dialect
        clause = ForUpdateClause(dialect)
        sql, params = clause.to_sql()
        assert sql == "FOR UPDATE"
        assert params == ()

    def test_for_share_basic(self, postgres_backend_single):
        """Test FOR SHARE clause."""
        dialect = postgres_backend_single.dialect
        if not dialect.supports_for_share():
            pytest.skip("FOR SHARE not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, strength=LockStrength.SHARE)
        sql, params = clause.to_sql()
        assert sql == "FOR SHARE"
        assert params == ()

    def test_for_no_key_update_basic(self, postgres_backend_single):
        """Test FOR NO KEY UPDATE clause."""
        dialect = postgres_backend_single.dialect
        if not dialect.supports_for_no_key_update():
            pytest.skip("FOR NO KEY UPDATE not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, strength=LockStrength.NO_KEY_UPDATE)
        sql, params = clause.to_sql()
        assert sql == "FOR NO KEY UPDATE"
        assert params == ()

    def test_for_key_share_basic(self, postgres_backend_single):
        """Test FOR KEY SHARE clause."""
        dialect = postgres_backend_single.dialect
        if not dialect.supports_for_key_share():
            pytest.skip("FOR KEY SHARE not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, strength=LockStrength.KEY_SHARE)
        sql, params = clause.to_sql()
        assert sql == "FOR KEY SHARE"
        assert params == ()

    def test_for_update_with_nowait(self, postgres_backend_single):
        """Test FOR UPDATE with NOWAIT."""
        dialect = postgres_backend_single.dialect
        clause = ForUpdateClause(dialect, nowait=True)
        sql, params = clause.to_sql()
        assert sql == "FOR UPDATE NOWAIT"
        assert params == ()

    def test_for_share_with_nowait(self, postgres_backend_single):
        """Test FOR SHARE with NOWAIT."""
        dialect = postgres_backend_single.dialect
        if not dialect.supports_for_share():
            pytest.skip("FOR SHARE not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, strength=LockStrength.SHARE, nowait=True)
        sql, params = clause.to_sql()
        assert sql == "FOR SHARE NOWAIT"
        assert params == ()

    def test_for_update_with_skip_locked(self, postgres_backend_single):
        """Test FOR UPDATE with SKIP LOCKED."""
        dialect = postgres_backend_single.dialect
        if not dialect.supports_for_update_skip_locked():
            pytest.skip("SKIP LOCKED not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, skip_locked=True)
        sql, params = clause.to_sql()
        assert sql == "FOR UPDATE SKIP LOCKED"
        assert params == ()

    def test_for_share_with_skip_locked(self, postgres_backend_single):
        """Test FOR SHARE with SKIP LOCKED."""
        dialect = postgres_backend_single.dialect
        if not dialect.supports_for_share():
            pytest.skip("FOR SHARE not supported on this PostgreSQL version")
        if not dialect.supports_for_update_skip_locked():
            pytest.skip("SKIP LOCKED not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, strength=LockStrength.SHARE, skip_locked=True)
        sql, params = clause.to_sql()
        assert sql == "FOR SHARE SKIP LOCKED"
        assert params == ()

    def test_for_update_with_of_columns(self, postgres_backend_single):
        """Test FOR UPDATE with OF columns."""
        dialect = postgres_backend_single.dialect
        clause = ForUpdateClause(dialect, of_columns=["users", "orders"])
        sql, params = clause.to_sql()
        assert sql == 'FOR UPDATE OF "users", "orders"'
        assert params == ()

    def test_for_share_with_of_columns(self, postgres_backend_single):
        """Test FOR SHARE with OF columns."""
        dialect = postgres_backend_single.dialect
        if not dialect.supports_for_share():
            pytest.skip("FOR SHARE not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, strength=LockStrength.SHARE, of_columns=["users"])
        sql, params = clause.to_sql()
        assert sql == 'FOR SHARE OF "users"'
        assert params == ()

    def test_unsupported_lock_strength_raises_error(self, postgres_backend_single):
        """Test that unsupported lock strength raises error."""
        dialect = postgres_backend_single.dialect

        # Create a mock dialect that doesn't support FOR SHARE
        if dialect.supports_for_share():
            # Skip if this version actually supports it
            pytest.skip("This test requires a PostgreSQL version < 9.0")

        clause = ForUpdateClause(dialect, strength=LockStrength.SHARE)
        with pytest.raises(UnsupportedFeatureError):
            clause.to_sql()


class TestAsyncRowLevelLockStrength:
    """Async tests for PostgreSQL row-level lock strength support."""

    async def test_async_supports_for_update(self, async_postgres_backend_single):
        """Test that FOR UPDATE is always supported (async)."""
        dialect = async_postgres_backend_single.dialect
        assert dialect.supports_for_update() is True
        assert dialect.supports_lock_strength(LockStrength.UPDATE) is True

    async def test_async_supports_for_share(self, async_postgres_backend_single):
        """Test FOR SHARE support (async)."""
        dialect = async_postgres_backend_single.dialect
        if dialect.version >= (9, 0, 0):
            assert dialect.supports_for_share() is True
        else:
            assert dialect.supports_for_share() is False

    async def test_async_for_update_basic(self, async_postgres_backend_single):
        """Test basic FOR UPDATE clause (async)."""
        dialect = async_postgres_backend_single.dialect
        clause = ForUpdateClause(dialect)
        sql, params = clause.to_sql()
        assert sql == "FOR UPDATE"
        assert params == ()

    async def test_async_for_share_basic(self, async_postgres_backend_single):
        """Test FOR SHARE clause (async)."""
        dialect = async_postgres_backend_single.dialect
        if not dialect.supports_for_share():
            pytest.skip("FOR SHARE not supported on this PostgreSQL version")

        clause = ForUpdateClause(dialect, strength=LockStrength.SHARE)
        sql, params = clause.to_sql()
        assert sql == "FOR SHARE"
        assert params == ()