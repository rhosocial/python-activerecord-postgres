# tests/rhosocial/activerecord_postgres_test/feature/backend/named_connection/test_cli.py
"""
Tests for PostgreSQL CLI parameter resolution.

This module tests the CLI parameter resolution priority for PostgreSQL.
"""
import os
import tempfile
import types
from unittest.mock import MagicMock, patch
import pytest

from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


class MockArgs:
    """Mock arguments for testing."""

    def __init__(
        self,
        host=None,
        port=None,
        database=None,
        user=None,
        password=None,
        named_connection=None,
        connection_params=None,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.named_connection = named_connection
        self.connection_params = connection_params or []


class TestPostgresConnectionConfigPriority:
    """Test connection config resolution priority for PostgreSQL CLI."""

    def test_default_values(self):
        """Test that default PostgreSQL values are used when no connection specified."""
        args = MockArgs()
        from rhosocial.activerecord.backend.impl.postgres.cli.connection import resolve_connection_config_from_args

        with patch(
            "rhosocial.activerecord.backend.named_connection.NamedConnectionResolver"
        ):
            config = resolve_connection_config_from_args(args)

        assert config.host == "localhost"
        assert config.port == 5432

    def test_explicit_params_only(self):
        """Test explicit --host, --port, etc. without named connection."""
        args = MockArgs(
            host="myhost",
            port=5433,
            database="mydb",
            user="myuser",
            password="mypass",
        )
        from rhosocial.activerecord.backend.impl.postgres.cli.connection import resolve_connection_config_from_args

        with patch(
            "rhosocial.activerecord.backend.named_connection.NamedConnectionResolver"
        ):
            config = resolve_connection_config_from_args(args)

        assert config.host == "myhost"
        assert config.port == 5433
        assert config.database == "mydb"
        assert config.username == "myuser"

    def test_named_connection_only(self):
        """Test --named-connection without explicit params."""
        args = MockArgs(
            named_connection="myapp.connections.prod_db",
        )
        from rhosocial.activerecord.backend.impl.postgres.cli.connection import resolve_connection_config_from_args

        mock_resolver = MagicMock()
        mock_config = PostgresConnectionConfig(
            host="prod.example.com",
            port=5432,
            database="prod",
        )
        mock_resolver.load.return_value = mock_resolver
        mock_resolver.resolve.return_value = mock_config

        with patch(
            "rhosocial.activerecord.backend.named_connection.NamedConnectionResolver",
            return_value=mock_resolver,
        ):
            config = resolve_connection_config_from_args(args)

        mock_resolver.load.assert_called_once()
        assert config.host == "prod.example.com"

    def test_named_connection_with_params(self):
        """Test --named-connection with --conn-param overrides."""
        args = MockArgs(
            named_connection="myapp.connections.prod_db",
            connection_params=["database=custom_db"],
        )
        from rhosocial.activerecord.backend.impl.postgres.cli.connection import resolve_connection_config_from_args

        mock_resolver = MagicMock()
        mock_config = PostgresConnectionConfig(host="prod.example.com")
        mock_resolver.load.return_value = mock_resolver
        mock_resolver.resolve.return_value = mock_config

        with patch(
            "rhosocial.activerecord.backend.named_connection.NamedConnectionResolver",
            return_value=mock_resolver,
        ):
            config = resolve_connection_config_from_args(args)

        mock_resolver.resolve.assert_called_once_with({"database": "custom_db"})

    def test_explicit_params_override_named_connection(self):
        """Test explicit params should NOT override named connection (PostgreSQL uses different approach).

        Note: Unlike SQLite where --db-file overrides named connection's database,
        PostgreSQL uses --conn-param for overrides, not explicit host/port.
        """
        args = MockArgs(
            host="myhost",
            named_connection="myapp.connections.prod_db",
        )
        from rhosocial.activerecord.backend.impl.postgres.cli.connection import resolve_connection_config_from_args

        mock_resolver = MagicMock()
        mock_config = PostgresConnectionConfig(host="prod.example.com")
        mock_resolver.load.return_value = mock_resolver
        mock_resolver.resolve.return_value = mock_config

        with patch(
            "rhosocial.activerecord.backend.named_connection.NamedConnectionResolver",
            return_value=mock_resolver,
        ):
            config = resolve_connection_config_from_args(args)

        # Named connection is used, explicit host is ignored when named_connection present
        assert config.host == "prod.example.com"