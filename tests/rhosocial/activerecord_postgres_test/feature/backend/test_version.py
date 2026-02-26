# tests/rhosocial/activerecord_postgres_test/feature/backend/test_version.py
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend, AsyncPostgresBackend


class TestPostgresVersion:
    """Tests for server version retrieval against a live database."""

    def test_get_version_parsing(self, postgres_backend: PostgresBackend):
        """Test that the method correctly parses the PostgreSQL version string."""
        version = postgres_backend.get_server_version()

        assert isinstance(version, tuple)
        assert len(version) >= 2

        for component in version:
            assert isinstance(component, int)

        assert version[0] > 8

    @pytest.mark.asyncio
    async def test_async_get_version_parsing(self, async_postgres_backend: AsyncPostgresBackend):
        """Test version parsing for the async backend."""
        version = await async_postgres_backend.get_server_version()

        assert isinstance(version, tuple)
        assert len(version) >= 2
        assert version[0] > 8
