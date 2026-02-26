# tests/rhosocial/activerecord_postgres_test/feature/backend/test_dialect.py
"""
PostgreSQL backend dialect tests using real database connection.

This module tests PostgreSQL dialect formatting using real database.
Each test has sync and async versions for complete coverage.
"""
import pytest
import pytest_asyncio


class TestPostgreSQLDialectBackend:
    """Synchronous dialect tests for PostgreSQL backend."""

    def test_format_identifier(self, postgres_backend):
        """Test identifier formatting."""
        dialect = postgres_backend.dialect

        result = dialect.format_identifier("test_table")
        assert result == '"test_table"'

        result = dialect.format_identifier("user_name")
        assert result == '"user_name"'

    def test_quote_parameter(self, postgres_backend):
        """Test parameter quoting for PostgreSQL."""
        sql = "SELECT * FROM users WHERE name = $1"
        params = ("John",)

        result_sql, result_params = postgres_backend._prepare_sql_and_params(sql, params)

        assert "$1" in result_sql


class TestAsyncPostgreSQLDialectBackend:
    """Asynchronous dialect tests for PostgreSQL backend."""

    async def test_async_format_identifier(self, async_postgres_backend):
        """Test identifier formatting (async)."""
        dialect = async_postgres_backend.dialect

        result = dialect.format_identifier("test_table")
        assert result == '"test_table"'

        result = dialect.format_identifier("user_name")
        assert result == '"user_name"'
