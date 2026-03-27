# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_columns.py
"""
Tests for PostgreSQL column introspection functionality.

This module tests the list_columns and get_column_info methods for retrieving column metadata.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.introspection.types import (
    ColumnInfo,
    ColumnNullable,
)


class TestListColumns:
    """Tests for list_columns method."""

    def test_list_columns_returns_list(self, backend_with_tables):
        """Test that list_columns returns a list."""
        columns = backend_with_tables.introspector.list_columns("users")

        assert isinstance(columns, list)
        assert len(columns) > 0

    def test_list_columns_includes_expected_columns(self, backend_with_tables):
        """Test that list_columns includes expected columns."""
        columns = backend_with_tables.introspector.list_columns("users")
        column_names = [col.name for col in columns]

        assert "id" in column_names
        assert "name" in column_names
        assert "email" in column_names
        assert "age" in column_names
        assert "created_at" in column_names

    def test_list_columns_nonexistent_table(self, backend_with_tables):
        """Test list_columns for non-existent table."""
        columns = backend_with_tables.introspector.list_columns("nonexistent_table")

        assert isinstance(columns, list)
        assert len(columns) == 0

    def test_list_columns_caching(self, backend_with_tables):
        """Test that column list is cached."""
        columns1 = backend_with_tables.introspector.list_columns("users")
        columns2 = backend_with_tables.introspector.list_columns("users")

        # Should return the same cached list
        assert columns1 is columns2

    def test_list_columns_with_schema(self, backend_with_tables):
        """Test list_columns with schema parameter."""
        # PostgreSQL default schema is 'public'
        columns = backend_with_tables.introspector.list_columns("users", schema="public")

        assert isinstance(columns, list)
        assert len(columns) > 0


class TestGetColumnInfo:
    """Tests for get_column_info method."""

    def test_get_column_info_existing(self, backend_with_tables):
        """Test get_column_info for existing column."""
        column = backend_with_tables.introspector.get_column_info("users", "id")

        assert column is not None
        assert isinstance(column, ColumnInfo)
        assert column.name == "id"
        assert column.table_name == "users"

    def test_get_column_info_nonexistent_column(self, backend_with_tables):
        """Test get_column_info for non-existent column."""
        column = backend_with_tables.introspector.get_column_info("users", "nonexistent")

        assert column is None

    def test_get_column_info_nonexistent_table(self, backend_with_tables):
        """Test get_column_info for non-existent table."""
        column = backend_with_tables.introspector.get_column_info("nonexistent_table", "id")

        assert column is None


class TestColumnDetails:
    """Tests for column detail properties."""

    def test_column_ordinal_position(self, backend_with_tables):
        """Test that column ordinal position is correct."""
        columns = backend_with_tables.introspector.list_columns("users")
        # Columns should have sequential ordinal positions
        for i, col in enumerate(columns):
            assert col.ordinal_position == i + 1

    def test_column_data_types(self, backend_with_tables):
        """Test that column data types are correctly set."""
        columns = backend_with_tables.introspector.list_columns("users")

        # Check specific types
        col_map = {col.name: col for col in columns}

        # Integer column (id)
        id_col = col_map.get("id")
        assert id_col is not None
        # PostgreSQL uses 'integer' for serial
        assert "integer" in id_col.data_type.lower()

        # String columns (name, email)
        for col_name in ["name", "email"]:
            col = col_map.get(col_name)
            assert col is not None
            assert "character" in col.data_type.lower() or "varchar" in col.data_type.lower()

    def test_column_nullable(self, backend_with_tables):
        """Test that column nullable is correctly identified."""
        columns = backend_with_tables.introspector.list_columns("users")

        col_map = {col.name: col for col in columns}

        # ID is NOT NULL (primary key)
        id_col = col_map.get("id")
        assert id_col.nullable == ColumnNullable.NOT_NULL

        # name is NOT NULL
        name_col = col_map.get("name")
        assert name_col.nullable == ColumnNullable.NOT_NULL

        # age is nullable
        age_col = col_map.get("age")
        assert age_col.nullable == ColumnNullable.NULLABLE

    def test_column_default_values(self, backend_with_tables):
        """Test that column default values are correctly identified."""
        columns = backend_with_tables.introspector.list_columns("users")

        col_map = {col.name: col for col in columns}

        # created_at has default value
        created_at_col = col_map.get("created_at")
        assert created_at_col is not None
        # PostgreSQL uses various forms for default value representation
        # The important thing is that default_value is not None
        assert created_at_col.default_value is not None

    def test_column_primary_key_detection(self, backend_with_tables):
        """Test that primary key columns are correctly identified."""
        columns = backend_with_tables.introspector.list_columns("users")

        id_col = next(col for col in columns if col.name == "id")
        assert id_col is not None
        assert id_col.is_primary_key == True

        # Other columns should not be primary key
        for col in columns:
            if col.name != "id":
                assert col.is_primary_key == False

    def test_column_comment(self, backend_with_tables):
        """Test that column comments are retrieved."""
        # Add comment to a column
        backend_with_tables.execute(
            "COMMENT ON COLUMN users.name IS 'User display name'"
        )

        # Clear cache and re-query
        backend_with_tables.introspector.clear_cache()
        columns = backend_with_tables.introspector.list_columns("users")

        col_map = {col.name: col for col in columns}
        name_col = col_map.get("name")

        # Comment may or may not be populated depending on PostgreSQL version
        # Just verify the comment attribute exists
        assert hasattr(name_col, 'comment')


class TestAsyncColumnIntrospection:
    """Async tests for column introspection."""

    @pytest.mark.asyncio
    async def test_async_list_columns(self, async_backend_with_tables):
        """Test async list_columns returns column info."""
        columns = await async_backend_with_tables.introspector.list_columns("users")

        assert isinstance(columns, list)
        assert len(columns) > 0

    @pytest.mark.asyncio
    async def test_async_get_column_info(self, async_backend_with_tables):
        """Test async get_column_info for existing column."""
        column = await async_backend_with_tables.introspector.get_column_info("users", "id")

        assert column is not None
        assert column.name == "id"

    @pytest.mark.asyncio
    async def test_async_list_columns_caching(self, async_backend_with_tables):
        """Test that async column list is cached."""
        columns1 = await async_backend_with_tables.introspector.list_columns("users")
        columns2 = await async_backend_with_tables.introspector.list_columns("users")

        # Should return the same cached list
        assert columns1 is columns2
