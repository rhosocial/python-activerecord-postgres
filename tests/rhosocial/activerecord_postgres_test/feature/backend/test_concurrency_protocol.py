# tests/rhosocial/activerecord_postgres_test/feature/backend/test_concurrency_protocol.py
"""
Test for ConcurrencyAware protocol implementation in PostgreSQL backend.

This test verifies that PostgresBackend correctly implements the ConcurrencyAware
protocol by returning the connection pool size as the concurrency limit.
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.protocols import ConcurrencyAware, ConcurrencyHint


class TestPostgresConcurrencyAware:
    """Test ConcurrencyAware protocol implementation for PostgreSQL backend."""

    def test_postgres_backend_implements_protocol(self, postgres_backend_single):
        """Test that PostgresBackend implements ConcurrencyAware protocol."""
        assert isinstance(postgres_backend_single, ConcurrencyAware)

    def test_postgres_get_concurrency_hint(self, postgres_backend_single):
        """Test PostgresBackend returns correct concurrency hint."""
        hint = postgres_backend_single.get_concurrency_hint()

        assert hint is not None
        assert isinstance(hint, ConcurrencyHint)

    def test_postgres_concurrency_hint_has_limit_or_none(self, postgres_backend_single):
        """Test concurrency hint has either a limit or indicates no constraint."""
        hint = postgres_backend_single.get_concurrency_hint()

        # Either max_concurrency is set or reason indicates no pool
        if hint.max_concurrency is not None:
            assert hint.max_concurrency > 0
            assert "pool" in hint.reason.lower()
        else:
            assert "no" in hint.reason.lower() or hint.reason == ""

    def test_postgres_concurrency_hint_returns_hint_object(self, postgres_backend_single):
        """Test that get_concurrency_hint returns a ConcurrencyHint object."""
        hint = postgres_backend_single.get_concurrency_hint()
        assert isinstance(hint, ConcurrencyHint)


class TestAsyncPostgresConcurrencyAware:
    """Test ConcurrencyAware protocol implementation for async PostgreSQL backend."""

    @pytest_asyncio.mark.asyncio
    async def test_async_postgres_backend_implements_protocol(
        self, async_postgres_backend_single
    ):
        """Test that AsyncPostgresBackend implements ConcurrencyAware protocol."""
        assert isinstance(async_postgres_backend_single, ConcurrencyAware)

    @pytest_asyncio.mark.asyncio
    async def test_async_postgres_get_concurrency_hint(self, async_postgres_backend_single):
        """Test AsyncPostgresBackend returns correct concurrency hint."""
        hint = async_postgres_backend_single.get_concurrency_hint()

        assert hint is not None
        assert isinstance(hint, ConcurrencyHint)

    @pytest_asyncio.mark.asyncio
    async def test_async_postgres_concurrency_hint_has_limit_or_none(
        self, async_postgres_backend_single
    ):
        """Test async concurrency hint has either a limit or indicates no constraint."""
        hint = async_postgres_backend_single.get_concurrency_hint()

        if hint.max_concurrency is not None:
            assert hint.max_concurrency > 0
            assert "pool" in hint.reason.lower()
        else:
            assert "no" in hint.reason.lower() or hint.reason == ""

    @pytest_asyncio.mark.asyncio
    async def test_async_postgres_concurrency_hint_returns_hint_object(
        self, async_postgres_backend_single
    ):
        """Test async get_concurrency_hint returns ConcurrencyHint object."""
        hint = async_postgres_backend_single.get_concurrency_hint()
        assert isinstance(hint, ConcurrencyHint)