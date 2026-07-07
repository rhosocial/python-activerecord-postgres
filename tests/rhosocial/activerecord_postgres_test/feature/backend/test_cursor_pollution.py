# tests/rhosocial/activerecord_postgres_test/feature/backend/test_cursor_pollution.py
"""
PostgreSQL cursor result set pollution tests.

Verifies that after get_server_version() executes SELECT version()
and closes the cursor, subsequent queries on the same connection
do not see polluted cursor.description.
"""

import logging

import pytest

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.feature, pytest.mark.backend]


class TestPostgresCursorPollution:
    """Cursor pollution: sync PostgreSQL backend."""

    def test_get_server_version_then_query(self, postgres_backend):
        """get_server_version() then a normal query."""
        backend = postgres_backend
        version = backend.get_server_version()
        assert version is not None

        cursor = backend._get_cursor()
        cursor.execute("SELECT 1 AS marker")
        rows = cursor.fetchall()
        cursor.close()

        assert len(rows) > 0
        assert cursor.description is not None
        col_name = cursor.description[0][0]
        assert col_name == "marker", (
            f"cursor.description polluted! expected 'marker', got: {col_name}"
        )

    def test_introspect_and_adapt_then_query(self, postgres_backend):
        """introspect_and_adapt() then a normal query."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        cursor = backend._get_cursor()
        cursor.execute("SELECT 'ok' AS status")
        rows = cursor.fetchall()
        cursor.close()

        assert len(rows) > 0
        assert cursor.description is not None
        col_name = cursor.description[0][0]
        assert col_name == "status", (
            f"cursor.description polluted! expected 'status', got: {col_name}"
        )

    def test_high_frequency_version_query_cycle(self, postgres_backend):
        """Repeated get_server_version → query cycle to expose state leaks."""
        backend = postgres_backend

        for i in range(200):
            backend.get_server_version()

            cursor = backend._get_cursor()
            cursor.execute(f"SELECT {i} AS cycle")
            rows = cursor.fetchall()
            cursor.close()

            assert len(rows) > 0
            assert cursor.description is not None
            col_name = cursor.description[0][0]
            assert col_name == "cycle", (
                f"Iteration {i}: cursor.description polluted! "
                f"expected 'cycle', got: {col_name}"
            )

        logger.info("PostgreSQL high-frequency version query cycle 200 iterations passed")


class TestAsyncPostgresCursorPollution:
    """Cursor pollution: async PostgreSQL backend."""

    @pytest.mark.asyncio
    async def test_get_server_version_then_query(self, async_postgres_backend):
        """Async get_server_version() then a normal query."""
        backend = async_postgres_backend
        version = await backend.get_server_version()
        assert version is not None

        cursor = await backend._get_cursor()
        await cursor.execute("SELECT 1 AS marker")
        rows = await cursor.fetchall()
        await cursor.close()

        assert len(rows) > 0
        assert cursor.description is not None
        col_name = cursor.description[0][0]
        assert col_name == "marker", (
            f"Async cursor.description polluted! expected 'marker', got: {col_name}"
        )

    @pytest.mark.asyncio
    async def test_introspect_and_adapt_then_query(self, async_postgres_backend):
        """Async introspect_and_adapt() then a normal query."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        cursor = await backend._get_cursor()
        await cursor.execute("SELECT 'ok' AS status")
        rows = await cursor.fetchall()
        await cursor.close()

        assert len(rows) > 0
        assert cursor.description is not None
        col_name = cursor.description[0][0]
        assert col_name == "status", (
            f"Async cursor.description polluted after introspect! "
            f"expected 'status', got: {col_name}"
        )

    @pytest.mark.asyncio
    async def test_context_entry_workflow(self, async_postgres_backend):
        """backend.context() workflow (production pattern)."""
        backend = async_postgres_backend

        async with backend.context():
            cursor = await backend._get_cursor()
            await cursor.execute("SELECT 'ctx_ok' AS ctx_status")
            rows = await cursor.fetchall()
            await cursor.close()

            assert len(rows) > 0
            assert cursor.description is not None
            col_name = cursor.description[0][0]
            assert col_name == "ctx_status", (
                f"Async cursor.description polluted after context()! "
                f"expected 'ctx_status', got: {col_name}"
            )

    @pytest.mark.asyncio
    async def test_high_frequency_version_query_cycle(self, async_postgres_backend):
        """Async high-frequency version query cycle to expose state leaks."""
        backend = async_postgres_backend

        for i in range(200):
            await backend.get_server_version()

            cursor = await backend._get_cursor()
            await cursor.execute(f"SELECT {i} AS cycle")
            rows = await cursor.fetchall()
            await cursor.close()

            assert len(rows) > 0
            assert cursor.description is not None
            col_name = cursor.description[0][0]
            assert col_name == "cycle", (
                f"Iteration {i}: async cursor.description polluted! "
                f"expected 'cycle', got: {col_name}"
            )

        logger.info("Async PostgreSQL high-frequency version query cycle 200 iterations passed")