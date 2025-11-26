# tests/rhosocial/activerecord_postgres_test/feature/backend/test_version.py
import pytest
from unittest.mock import patch

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend, AsyncPostgresBackend


@pytest.fixture(autouse=True)
def clear_cache(request):
    """
    This fixture automatically runs before each test in this module.
    It clears the instance-level cache on the backend fixtures before a test runs.
    """
    if "postgres_backend" in request.fixturenames:
        backend = request.getfixturevalue("postgres_backend")
        if hasattr(backend, '_server_version_cache'):
            backend._server_version_cache = None

    if "async_postgres_backend" in request.fixturenames:
        async_backend = request.getfixturevalue("async_postgres_backend")
        if hasattr(async_backend, '_server_version_cache'):
            async_backend._server_version_cache = None


class TestPostgresVersion:
    """Tests for server version retrieval and parsing against a live database."""

    def test_get_version_parsing(self, postgres_backend: PostgresBackend):
        """Test that the method correctly parses the PostgreSQL version string."""
        # Get the version from the backend provided by the fixture
        version = postgres_backend.get_server_version()

        # Check that the version is a tuple with at least two elements
        assert isinstance(version, tuple)
        assert len(version) >= 2

        # Check that all components are integers
        for component in version:
            assert isinstance(component, int)

        # Assuming a reasonably modern PostgreSQL
        assert version[0] > 8

    def test_version_caching(self, postgres_backend: PostgresBackend):
        """Test that the version is cached at the instance level after first retrieval."""
        # First call should fetch the version and cache it.
        version1 = postgres_backend.get_server_version()
        assert version1 is not None
        assert postgres_backend._server_version_cache is not None

        # To prove it's cached, let's try to get it again.
        # We can patch the cursor to see if it's being used.
        with patch.object(postgres_backend._connection, 'cursor') as mock_cursor:
            version2 = postgres_backend.get_server_version()
            # The cursor should not have been called because the value is cached.
            mock_cursor.assert_not_called()

        assert version1 == version2

    @pytest.mark.asyncio
    async def test_async_get_version_parsing(self, async_postgres_backend: AsyncPostgresBackend):
        """Test version parsing for the async backend."""
        # Get the version from the backend provided by the fixture
        version = await async_postgres_backend.get_server_version()

        # Check that the version is a tuple with at least two elements
        assert isinstance(version, tuple)
        assert len(version) >= 2
        assert version[0] > 8

    @pytest.mark.asyncio
    async def test_async_version_caching(self, async_postgres_backend: AsyncPostgresBackend):
        """Test that the version is cached at the instance level for async backend."""
        # First call should fetch the version and cache it.
        version1 = await async_postgres_backend.get_server_version()
        assert version1 is not None
        assert async_postgres_backend._server_version_cache is not None

        # To prove it's cached, let's try to get it again.
        with patch.object(async_postgres_backend._connection, 'cursor') as mock_cursor:
            version2 = await async_postgres_backend.get_server_version()
            # The cursor should not have been called because the value is cached.
            mock_cursor.assert_not_called()

        assert version1 == version2
