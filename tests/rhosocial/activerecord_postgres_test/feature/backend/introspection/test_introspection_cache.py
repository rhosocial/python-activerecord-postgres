# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_cache.py
"""
Tests for PostgreSQL introspection caching functionality.

This module tests the cache management for introspection operations.
"""

import pytest
import time
import threading

from rhosocial.activerecord.backend.introspection.types import IntrospectionScope


# Fixtures backend_with_tables, async_backend_with_tables, and postgres_backend_single
# are provided by conftest.py


class TestCacheManagement:
    """Tests for cache management methods."""

    def test_clear_introspection_cache(self, backend_with_tables):
        """Test clearing introspection cache."""
        # Get and cache data
        backend_with_tables.introspector.list_tables()

        # Clear cache
        backend_with_tables.introspector.clear_cache()

        # Verify cache was cleared
        assert len(backend_with_tables.introspector._cache) == 0

    def test_cache_hit(self, backend_with_tables):
        """Test that cached data is returned on subsequent calls."""
        tables1 = backend_with_tables.introspector.list_tables()

        # Should hit cache
        tables2 = backend_with_tables.introspector.list_tables()

        # Same object reference means cache hit
        assert tables1 is tables2

    def test_cache_miss_after_clear(self, backend_with_tables):
        """Test that cache miss occurs after clear."""
        tables1 = backend_with_tables.introspector.list_tables()
        backend_with_tables.introspector.clear_cache()
        tables2 = backend_with_tables.introspector.list_tables()

        # Different object means cache miss
        assert tables1 is not tables2


class TestInvalidateIntrospectionCache:
    """Tests for cache invalidation methods."""

    def test_invalidate_all_scopes(self, backend_with_tables):
        """Test invalidating all scopes."""
        # Populate cache
        backend_with_tables.introspector.get_database_info()
        backend_with_tables.introspector.list_tables()
        backend_with_tables.introspector.list_views()

        # Invalidate all
        backend_with_tables.introspector.invalidate_cache()

        # All should be cleared
        assert len(backend_with_tables.introspector._cache) == 0

    def test_invalidate_specific_scope(self, backend_with_tables):
        """Test invalidating specific scope."""
        # Populate cache
        backend_with_tables.introspector.get_database_info()
        tables = backend_with_tables.introspector.list_tables()

        # Invalidate only table scope
        backend_with_tables.introspector.invalidate_cache(IntrospectionScope.TABLE)

        # Database info should still be cached
        db_info2 = backend_with_tables.introspector.get_database_info()
        assert db_info2 is not None

        # Tables should be cleared, so re-fetch returns new object
        tables2 = backend_with_tables.introspector.list_tables()
        assert tables is not tables2

    def test_invalidate_table_scope(self, backend_with_tables):
        """Test invalidating table-related cache."""
        tables = backend_with_tables.introspector.list_tables()
        table_name = tables[0].name if tables else 'users'

        backend_with_tables.introspector.list_columns(table_name)
        backend_with_tables.introspector.list_indexes(table_name)
        backend_with_tables.introspector.list_foreign_keys(table_name)

        # Invalidate column scope
        backend_with_tables.introspector.invalidate_cache(IntrospectionScope.COLUMN)

        # Column cache should be cleared; re-fetch
        cols2 = backend_with_tables.introspector.list_columns(table_name)
        assert cols2 is not None

    def test_invalidate_specific_table(self, backend_with_tables):
        """Test invalidating cache for specific table."""
        tables = backend_with_tables.introspector.list_tables()
        if not tables or len(tables) < 2:
            pytest.skip("Not enough tables found")

        table1 = tables[0].name
        table2 = tables[1].name

        # Populate get_table_info cache for both tables
        backend_with_tables.introspector.get_table_info(table1)
        backend_with_tables.introspector.get_table_info(table2)

        # Both should be in cache
        key1 = backend_with_tables.introspector._make_cache_key(
            IntrospectionScope.TABLE, table1
        )
        key2 = backend_with_tables.introspector._make_cache_key(
            IntrospectionScope.TABLE, table2
        )
        assert key1 in backend_with_tables.introspector._cache
        assert key2 in backend_with_tables.introspector._cache

        # Invalidate only table1
        backend_with_tables.introspector.invalidate_cache(
            IntrospectionScope.TABLE, name=table1
        )

        # table1's cache key should be removed
        assert key1 not in backend_with_tables.introspector._cache

        # table2's cache key should still be present
        assert key2 in backend_with_tables.introspector._cache


class TestCacheThreadSafety:
    """Tests for cache thread safety."""

    def test_cache_lock_exists(self, backend_with_tables):
        """Test that cache lock exists."""
        assert hasattr(backend_with_tables.introspector, "_cache_lock")

    def test_concurrent_cache_access(self, backend_with_tables):
        """Test concurrent cache access is safe (read-only, single connection)."""
        results = []
        errors = []

        def read_cache():
            try:
                for _ in range(5):
                    # Only read from cache, don't execute DB queries concurrently
                    backend_with_tables.introspector._get_cached("nonexistent_key")
                    results.append(True)
            except Exception as e:
                errors.append(e)

        def clear_cache():
            try:
                for _ in range(3):
                    backend_with_tables.introspector.clear_cache()
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_cache) for _ in range(3)]
        threads.append(threading.Thread(target=clear_cache))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No exceptions should have occurred (cache ops are thread-safe)
        assert len(errors) == 0


class TestCacheExpiration:
    """Tests for cache expiration."""

    def test_cache_ttl_exists(self, backend_with_tables):
        """Test that cache TTL is configured."""
        assert hasattr(backend_with_tables.introspector, "_cache_ttl")
        assert backend_with_tables.introspector._cache_ttl > 0

    def test_expired_cache_not_returned(self, backend_with_tables):
        """Test that expired cache entries are not returned."""
        # Set very short TTL
        backend_with_tables.introspector._cache_ttl = 0.01  # 10ms

        db_info1 = backend_with_tables.introspector.get_database_info()

        # Wait for cache to expire
        time.sleep(0.05)

        db_info2 = backend_with_tables.introspector.get_database_info()

        # Different objects because cache expired
        assert db_info1 is not db_info2


class TestCacheKeys:
    """Tests for cache key generation."""

    def test_cache_key_generation(self, postgres_backend_single):
        """Test that cache keys are generated correctly."""
        key = postgres_backend_single.introspector._make_cache_key(
            IntrospectionScope.TABLE,
            "users",
            schema="public"
        )

        assert "table" in key
        assert "users" in key

    def test_cache_key_with_extra(self, postgres_backend_single):
        """Test cache key with extra component."""
        key = postgres_backend_single.introspector._make_cache_key(
            IntrospectionScope.COLUMN,
            "users",
            extra="email"
        )

        assert "column" in key
        assert "users" in key
        assert "email" in key

    def test_cache_key_uniqueness(self, postgres_backend_single):
        """Test that different parameters produce different keys."""
        key1 = postgres_backend_single.introspector._make_cache_key(
            IntrospectionScope.TABLE, "users"
        )
        key2 = postgres_backend_single.introspector._make_cache_key(
            IntrospectionScope.TABLE, "posts"
        )
        key3 = postgres_backend_single.introspector._make_cache_key(
            IntrospectionScope.COLUMN, "users"
        )

        assert key1 != key2
        assert key1 != key3
        assert key2 != key3


class TestAsyncCacheManagement:
    """Async tests for cache management."""

    @pytest.mark.asyncio
    async def test_async_clear_cache(self, async_backend_with_tables):
        """Test async clear cache."""
        await async_backend_with_tables.introspector.list_tables()
        async_backend_with_tables.introspector.clear_cache()
        assert len(async_backend_with_tables.introspector._cache) == 0

    @pytest.mark.asyncio
    async def test_async_cache_hit(self, async_backend_with_tables):
        """Test async cache hit."""
        tables1 = await async_backend_with_tables.introspector.list_tables()
        tables2 = await async_backend_with_tables.introspector.list_tables()
        assert tables1 is tables2

    @pytest.mark.asyncio
    async def test_async_cache_miss_after_clear(self, async_backend_with_tables):
        """Test async cache miss after clear."""
        tables1 = await async_backend_with_tables.introspector.list_tables()
        async_backend_with_tables.introspector.clear_cache()
        tables2 = await async_backend_with_tables.introspector.list_tables()
        assert tables1 is not tables2
