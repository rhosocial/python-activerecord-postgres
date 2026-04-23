# tests/rhosocial/activerecord_postgres_test/feature/backend/named_connection/conftest.py
"""
Test fixtures for PostgreSQL named connection tests.
"""
import types
from unittest.mock import MagicMock
import pytest


@pytest.fixture
def mock_backend_cls():
    """Create a mock backend class for testing."""
    return MagicMock(name="MockPostgresBackend")


@pytest.fixture
def connection_module():
    """Create a test module with named connections."""
    from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

    module = types.ModuleType("test_postgres_connections")

    def dev_db(backend_cls, database: str = "test_db"):
        return PostgresConnectionConfig(
            host="localhost",
            port=5432,
            database=database,
            username="postgres",
            password="password",
        )

    module.dev_db = dev_db
    return module


class TestCliArgs:
    """Helper class to create mock CLI args for testing."""

    @staticmethod
    def create(named_connection: str = None, **kwargs):
        """Create a mock args namespace."""
        from argparse import Namespace

        defaults = {
            "named_connection": named_connection,
            "connection_params": [],
        }
        defaults.update(kwargs)
        return Namespace(**defaults)
