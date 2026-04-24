# tests/rhosocial/activerecord_postgres_test/feature/backend/named_connection/test_resolver.py
"""
Tests for PostgreSQL named connection resolver.

This test module covers:
- NamedConnectionResolver with PostgreSQL backend
- PostgreSQL-specific connection configurations
- Integration tests using example_connections module
"""
import types
from unittest.mock import MagicMock, patch
import pytest

from rhosocial.activerecord.backend.named_connection.resolver import (
    NamedConnectionResolver,
    resolve_named_connection,
    list_named_connections_in_module,
)
from rhosocial.activerecord.backend.named_connection.exceptions import (
    NamedConnectionNotFoundError,
    NamedConnectionModuleNotFoundError,
    NamedConnectionInvalidReturnTypeError,
    NamedConnectionNotCallableError,
    NamedConnectionMissingParameterError,
    NamedConnectionInvalidParameterError,
)
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


class TestPostgresNamedConnectionResolverUnit:
    """Unit tests for NamedConnectionResolver with PostgreSQL backend."""

    def test_resolve_postgres_config(self):
        """Test resolving a PostgreSQL connection config."""
        module = types.ModuleType("test_postgres_connections")

        def dev_db(database: str = "test_db"):
            return PostgresConnectionConfig(
                host="localhost",
                port=5432,
                database=database,
                username="postgres",
                password="password",
            )

        module.dev_db = dev_db
        with patch("importlib.import_module", return_value=module):
            config = NamedConnectionResolver("test_postgres_connections.dev_db").load().resolve({})
            assert isinstance(config, PostgresConnectionConfig)
            assert config.host == "localhost"
            assert config.database == "test_db"

    def test_resolve_postgres_with_custom_database(self):
        """Test resolving PostgreSQL config with custom database parameter."""
        module = types.ModuleType("test_postgres_connections")

        def dev_db(database: str = "test_db"):
            return PostgresConnectionConfig(
                host="localhost",
                port=5432,
                database=database,
                username="postgres",
                password="password",
            )

        module.dev_db = dev_db
        with patch("importlib.import_module", return_value=module):
            config = NamedConnectionResolver("test_postgres_connections.dev_db").load().resolve(
                {"database": "my_app_db"}
            )
            assert isinstance(config, PostgresConnectionConfig)
            assert config.database == "my_app_db"

    def test_resolve_postgres_missing_required_param(self):
        """Test resolve fails when required PostgreSQL parameter is missing."""
        module = types.ModuleType("test_postgres_connections")

        def strict_db(host: str):
            return PostgresConnectionConfig(host=host)

        module.strict_db = strict_db
        with patch("importlib.import_module", return_value=module):
            resolver = NamedConnectionResolver("test_postgres_connections.strict_db").load()
            with pytest.raises(NamedConnectionMissingParameterError):
                resolver.resolve({})

    def test_resolve_postgres_invalid_return_type(self):
        """Test resolve fails when callable returns non-BaseConfig."""
        module = types.ModuleType("test_postgres_connections")

        def bad_connection():
            return {"host": "localhost"}

        module.bad_connection = bad_connection
        with patch("importlib.import_module", return_value=module):
            resolver = NamedConnectionResolver("test_postgres_connections.bad_connection").load()
            with pytest.raises(NamedConnectionInvalidReturnTypeError):
                resolver.resolve({})

    def test_list_postgres_connections(self):
        """Test listing PostgreSQL connections in a module."""
        module = types.ModuleType("test_postgres_connections")

        def dev_db(database: str = "test_db"):
            return PostgresConnectionConfig(host="localhost", database=database)

        def prod_db():
            return PostgresConnectionConfig(host="prod.example.com", database="prod")

        module.dev_db = dev_db
        module.prod_db = prod_db

        with patch("importlib.import_module", return_value=module):
            connections = list_named_connections_in_module("test_postgres_connections")
            names = [c["name"] for c in connections]
            assert "dev_db" in names
            assert "prod_db" in names


class TestPostgresNamedConnectionsIntegration:
    """Integration tests using actual example_connections module."""

    def test_postgres_18_connection(self):
        """Test resolving the postgres_18 named connection."""
        config = resolve_named_connection(
            "tests.rhosocial.activerecord_postgres_test.feature.backend.named_connection.example_connections.postgres_18",
            {},
        )
        assert isinstance(config, PostgresConnectionConfig)
        assert config.host == "db-dev-1-n.rho.im"
        assert config.port == 15441
        assert config.database == "test_db"
        assert config.sslmode == "prefer"

    def test_postgres_18_with_custom_database(self):
        """Test resolving postgres_18 with custom database parameter."""
        config = resolve_named_connection(
            "tests.rhosocial.activerecord_postgres_test.feature.backend.named_connection.example_connections.postgres_18",
            {"database": "my_app"},
        )
        assert isinstance(config, PostgresConnectionConfig)
        assert config.database == "my_app"

    def test_postgres_18_with_pool(self):
        """Test resolving postgres_18_with_pool named connection."""
        config = resolve_named_connection(
            "tests.rhosocial.activerecord_postgres_test.feature.backend.named_connection.example_connections.postgres_18_with_pool",
            {},
        )
        assert isinstance(config, PostgresConnectionConfig)
        assert config.pool_size == 5

    def test_postgres_18_with_custom_pool_size(self):
        """Test resolving postgres_18_with_pool with custom pool_size."""
        config = resolve_named_connection(
            "tests.rhosocial.activerecord_postgres_test.feature.backend.named_connection.example_connections.postgres_18_with_pool",
            {"pool_size": "10"},
        )
        assert isinstance(config, PostgresConnectionConfig)
        assert config.pool_size == 10

    def test_postgres_18_readonly(self):
        """Test resolving postgres_18_readonly named connection."""
        config = resolve_named_connection(
            "tests.rhosocial.activerecord_postgres_test.feature.backend.named_connection.example_connections.postgres_18_readonly",
            {},
        )
        assert isinstance(config, PostgresConnectionConfig)
        assert config.pool_timeout == 10

    def test_list_example_connections(self):
        """Test listing connections in example_connections module."""
        connections = list_named_connections_in_module(
            "tests.rhosocial.activerecord_postgres_test.feature.backend.named_connection.example_connections"
        )
        names = [c["name"] for c in connections]
        assert "postgres_18" in names
        assert "postgres_18_with_pool" in names
        assert "postgres_18_readonly" in names

    def test_describe_postgres_18(self):
        """Test describing the postgres_18 connection."""
        resolver = NamedConnectionResolver(
            "tests.rhosocial.activerecord_postgres_test.feature.backend.named_connection.example_connections.postgres_18"
        ).load()
        info = resolver.describe()
        assert info["is_class"] is False
        assert "database" in info["parameters"]
        if info.get("config_preview"):
            assert "password" not in info["config_preview"]
