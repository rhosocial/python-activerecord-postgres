# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_tables.py
"""
Tests for PostgreSQL table introspection functionality.

This module tests the list_tables, get_table_info, and table_exists methods for retrieving table metadata.
"""

import pytest

from rhosocial.activerecord.backend.introspection.types import (
    TableInfo,
    TableType,
)


class TestListTables:
    """Tests for list_tables method."""

    def test_list_tables_returns_list(self, backend_with_tables):
        """Test that list_tables returns a list."""
        tables = backend_with_tables.introspector.list_tables()

        assert isinstance(tables, list)
        assert len(tables) > 0

    def test_list_tables_includes_expected_tables(self, backend_with_tables):
        """Test that list_tables includes expected tables."""
        tables = backend_with_tables.introspector.list_tables()
        table_names = [t.name for t in tables]

        assert "users" in table_names
        assert "posts" in table_names
        assert "tags" in table_names
        assert "post_tags" in table_names

    def test_list_tables_excludes_system_tables(self, backend_with_tables):
        """Test that list_tables excludes system tables by default."""
        tables = backend_with_tables.introspector.list_tables()
        table_names = [t.name for t in tables]

        # PostgreSQL system tables start with pg_
        for name in table_names:
            assert not name.startswith("pg_")

    def test_list_tables_caching(self, backend_with_tables):
        """Test that table list is cached."""
        tables1 = backend_with_tables.introspector.list_tables()
        tables2 = backend_with_tables.introspector.list_tables()

        # Should return the same cached list
        assert tables1 is tables2


class TestGetTableInfo:
    """Tests for get_table_info method."""

    def test_get_table_info_existing(self, backend_with_tables):
        """Test get_table_info for existing table."""
        table = backend_with_tables.introspector.get_table_info("users")

        assert table is not None
        assert isinstance(table, TableInfo)
        assert table.name == "users"
        assert table.table_type == TableType.BASE_TABLE

        # Should have columns,        assert table.columns is not None
        assert len(table.columns) > 0

    def test_get_table_info_nonexistent(self, backend_with_tables):
        """Test get_table_info for non-existent table."""
        table = backend_with_tables.introspector.get_table_info("nonexistent_table")

        assert table is None

    def test_get_table_info_includes_columns(self, backend_with_tables):
        """Test that get_table_info includes column info."""
        table = backend_with_tables.introspector.get_table_info("users")

        assert table is not None
        assert table.columns is not None
        column_names = [col.name for col in table.columns]
        assert "id" in column_names
        assert "name" in column_names

    def test_get_table_info_includes_indexes(self, backend_with_tables):
        """Test that get_table_info includes index info."""
        table = backend_with_tables.introspector.get_table_info("users")

        assert table is not None
        assert table.indexes is not None
        # Should include primary key
        pk = backend_with_tables.introspector.get_primary_key("users")
        assert pk is not None
        index_names = [idx.name for idx in table.indexes]
        assert pk.name in index_names

    def test_get_table_info_includes_foreign_keys(self, backend_with_tables):
        """Test that get_table_info includes foreign key info."""
        table = backend_with_tables.introspector.get_table_info("posts")

        assert table is not None
        assert table.foreign_keys is not None
        assert len(table.foreign_keys) > 0
        # Check foreign key details
        fk = table.foreign_keys[0]
        assert fk.name == "fk_posts_user"
        assert fk.referenced_table == "users"


class TestTableExists:
    """Tests for table_exists method."""

    def test_table_exists_true(self, backend_with_tables):
        """Test table_exists returns True for existing table."""
        assert backend_with_tables.introspector.table_exists("users") is True
        assert backend_with_tables.introspector.table_exists("posts") is True

    def test_table_exists_false(self, backend_with_tables):
        """Test table_exists returns False for non-existent table."""
        assert backend_with_tables.introspector.table_exists("nonexistent_table") is False

    def test_table_exists_case_sensitive(self, backend_with_tables):
        """Test that table_exists is case-sensitive."""
        # PostgreSQL is case-sensitive for identifiers
        assert backend_with_tables.introspector.table_exists("USERS") is False
        assert backend_with_tables.introspector.table_exists("Users") is False


class TestTableInfoDetails:
    """Tests for TableInfo detail properties."""

    def test_table_type_base_table(self, backend_with_tables):
        """Test that base tables have correct type."""
        tables = backend_with_tables.introspector.list_tables()

        for table in tables:
            if table.name in ["users", "posts", "tags"]:
                assert table.table_type == TableType.BASE_TABLE

    def test_table_schema(self, backend_with_tables):
        """Test that table schema is correctly identified."""
        tables = backend_with_tables.introspector.list_tables()

        for table in tables:
            # Default schema in PostgreSQL is 'public'
            assert table.schema == "public"

    def test_table_row_count(self, backend_with_tables):
        """Test that row count is available."""
        tables = backend_with_tables.introspector.list_tables()

        for table in tables:
            if table.name in ["users", "posts", "tags"]:
                # Row count should be 0 or None after just creation
                assert table.row_count is None or table.row_count == 0

    def test_table_create_time(self, backend_with_tables):
        """Test that create_time is available (may be None in PostgreSQL)."""
        tables = backend_with_tables.introspector.list_tables()

        for table in tables:
            if table.name in ["users", "posts", "tags"]:
                # PostgreSQL pg_catalog does not store creation time,
                # so create_time is expected to be None
                assert table.create_time is None


class TestAsyncTableIntrospection:
    """Async tests for table introspection."""

    @pytest.mark.asyncio
    async def test_async_list_tables(self, async_backend_with_tables):
        """Test async list_tables returns table info."""
        tables = await async_backend_with_tables.introspector.list_tables()

        assert isinstance(tables, list)
        assert len(tables) > 0

    @pytest.mark.asyncio
    async def test_async_get_table_info(self, async_backend_with_tables):
        """Test async get_table_info for existing table."""
        table = await async_backend_with_tables.introspector.get_table_info(
            "users"
        )

        assert table is not None
        assert table.name == "users"

    @pytest.mark.asyncio
    async def test_async_table_exists(self, async_backend_with_tables):
        """Test async table_exists method."""
        exists = await async_backend_with_tables.introspector.table_exists("users")

        assert exists is True

        exists = await async_backend_with_tables.introspector.table_exists(
            "nonexistent_table"
        )

        assert exists is False

    @pytest.mark.asyncio
    async def test_async_list_tables_caching(self, async_backend_with_tables):
        """Test that async table list is cached."""
        tables1 = await async_backend_with_tables.introspector.list_tables()
        tables2 = await async_backend_with_tables.introspector.list_tables()

        # Should return the same cached list
        assert tables1 is tables2
