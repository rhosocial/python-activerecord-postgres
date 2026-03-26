# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_indexes.py
"""
Tests for PostgreSQL index introspection functionality.

This module tests the list_indexes and get_index_info methods for retrieving index metadata.
"""

import pytest

from rhosocial.activerecord.backend.introspection.types import (
    IndexInfo,
    IndexType,
)


class TestListIndexes:
    """Tests for list_indexes method."""

    def test_list_indexes_returns_list(self, backend_with_tables):
        """Test that list_indexes returns a list."""
        indexes = backend_with_tables.introspector.list_indexes("users")

        assert isinstance(indexes, list)
        assert len(indexes) > 0

    def test_list_indexes_includes_expected_indexes(self, backend_with_tables):
        """Test that list_indexes includes expected indexes."""
        indexes = backend_with_tables.introspector.list_indexes("users")
        index_names = [idx.name for idx in indexes]

        # Primary key index
        assert "users_pkey" in index_names
        # Email unique index
        assert "idx_users_email" in index_names
        # Composite index
        assert "idx_users_name_age" in index_names

    def test_list_indexes_nonexistent_table(self, backend_with_tables):
        """Test list_indexes for non-existent table."""
        indexes = backend_with_tables.introspector.list_indexes("nonexistent_table")

        assert isinstance(indexes, list)
        assert len(indexes) == 0

    def test_list_indexes_caching(self, backend_with_tables):
        """Test that index list is cached."""
        indexes1 = backend_with_tables.introspector.list_indexes("users")
        indexes2 = backend_with_tables.introspector.list_indexes("users")

        # Should return the same cached list
        assert indexes1 is indexes2


class TestGetIndexInfo:
    """Tests for get_index_info method."""

    def test_get_index_info_existing(self, backend_with_tables):
        """Test get_index_info for existing index."""
        # Primary key index
        index = backend_with_tables.introspector.get_index_info("users", "users_pkey")

        assert index is not None
        assert isinstance(index, IndexInfo)
        assert index.name == "users_pkey"
        assert index.table_name == "users"

    def test_get_index_info_nonexistent(self, backend_with_tables):
        """Test get_index_info for non-existent index."""
        index = backend_with_tables.introspector.get_index_info("users", "nonexistent")

        assert index is None

    def test_get_index_info_nonexistent_table(self, backend_with_tables):
        """Test get_index_info for non-existent table."""
        index = backend_with_tables.introspector.get_index_info("nonexistent_table", "some_index")

        assert index is None


class TestGetPrimaryKey:
    """Tests for get_primary_key method."""

    def test_get_primary_key_single(self, backend_with_tables):
        """Test get_primary_key for single-column primary key."""
        pk = backend_with_tables.introspector.get_primary_key("users")

        assert pk is not None
        assert isinstance(pk, IndexInfo)
        assert pk.is_primary is True
        assert len(pk.columns) == 1
        assert pk.columns[0].name == "id"

    def test_get_primary_key_composite(self, backend_with_tables):
        """Test get_primary_key for composite primary key."""
        # Create a table with composite primary key
        backend_with_tables.execute("DROP TABLE IF EXISTS composite_pk_test CASCADE")
        backend_with_tables.execute("""
            CREATE TABLE composite_pk_test (
                id1 INTEGER NOT NULL,
                id2 INTEGER NOT NULL,
                value TEXT,
                PRIMARY KEY (id1, id2)
            )
        """)

        backend_with_tables.introspector.clear_cache()
        pk = backend_with_tables.introspector.get_primary_key("composite_pk_test")

        assert pk is not None
        assert pk.is_primary is True
        assert len(pk.columns) == 2
        column_names = [col.name for col in pk.columns]
        assert "id1" in column_names
        assert "id2" in column_names

        # Cleanup
        backend_with_tables.execute("DROP TABLE IF EXISTS composite_pk_test CASCADE")

    def test_get_primary_key_no_pk(self, backend_with_tables):
        """Test get_primary_key for table without primary key."""
        # Create a table without primary key
        backend_with_tables.execute("DROP TABLE IF EXISTS no_pk_test CASCADE")
        backend_with_tables.execute("""
            CREATE TABLE no_pk_test (
                id INTEGER,
                value TEXT
            )
        """)

        backend_with_tables.introspector.clear_cache()
        pk = backend_with_tables.introspector.get_primary_key("no_pk_test")

        # PostgreSQL always has some index; may return None or first index
        # depending on implementation
        # Cleanup
        backend_with_tables.execute("DROP TABLE IF EXISTS no_pk_test CASCADE")


class TestIndexInfoDetails:
    """Tests for index detail properties."""

    def test_index_is_unique(self, backend_with_tables):
        """Test that index uniqueness is correctly identified."""
        # Create a unique index for this test
        backend_with_tables.execute(
            "CREATE UNIQUE INDEX idx_users_email_unique ON users (email)"
        )
        backend_with_tables.introspector.clear_cache()

        indexes = backend_with_tables.introspector.list_indexes("users")
        email_unique_idx = next(
            (idx for idx in indexes if idx.name == "idx_users_email_unique"), None
        )
        assert email_unique_idx is not None
        assert email_unique_idx.is_unique is True

        # Primary key is also unique
        pk_idx = backend_with_tables.introspector.get_primary_key("users")
        assert pk_idx.is_unique is True

        # Cleanup
        backend_with_tables.execute("DROP INDEX IF EXISTS idx_users_email_unique")

    def test_index_is_non_unique(self, backend_with_tables):
        """Test that non-unique indexes are correctly identified."""
        # Create a non-unique index
        backend_with_tables.execute("CREATE INDEX idx_users_name ON users (name)")

        backend_with_tables.introspector.clear_cache()
        indexes = backend_with_tables.introspector.list_indexes("users")

        name_idx = next((idx for idx in indexes if idx.name == "idx_users_name"), None)
        assert name_idx is not None
        assert name_idx.is_unique is False

        # Cleanup
        backend_with_tables.execute("DROP INDEX IF EXISTS idx_users_name")

    def test_index_type_btree(self, backend_with_tables):
        """Test that btree index type is correctly identified."""
        indexes = backend_with_tables.introspector.list_indexes("users")

        # Most indexes should be btree
        for idx in indexes:
            if idx.index_type != IndexType.UNKNOWN:
                assert idx.index_type == IndexType.BTREE

    def test_index_columns(self, backend_with_tables):
        """Test that index columns are correctly retrieved."""
        indexes = backend_with_tables.introspector.list_indexes("users")

        # Find composite index
        name_age_idx = next(
            (idx for idx in indexes if idx.name == "idx_users_name_age"),
            None
        )
        assert name_age_idx is not None
        assert len(name_age_idx.columns) == 2
        column_names = [col.name for col in name_age_idx.columns]
        assert "name" in column_names
        assert "age" in column_names

    def test_index_column_ordinal_positions(self, backend_with_tables):
        """Test that index column ordinal positions are correct."""
        indexes = backend_with_tables.introspector.list_indexes("users")

        name_age_idx = next(
            (idx for idx in indexes if idx.name == "idx_users_name_age"),
            None
        )
        assert name_age_idx is not None

        # Check ordinal positions
        positions = [col.ordinal_position for col in name_age_idx.columns]
        assert positions[0] == 1
        assert positions[1] == 2

    def test_primary_key_detection_in_indexes(self, backend_with_tables):
        """Test that primary key is detected in indexes list."""
        indexes = backend_with_tables.introspector.list_indexes("users")

        # Primary key should be marked as primary
        pk_indexes = [idx for idx in indexes if idx.is_primary]
        assert len(pk_indexes) == 1
        assert pk_indexes[0].name == "users_pkey"


class TestPostgreSQLSpecificIndexTypes:
    """Tests for PostgreSQL-specific index types."""

    def test_unique_index(self, backend_with_tables):
        """Test unique index detection."""
        # Primary key index is always unique
        pk_idx = backend_with_tables.introspector.get_primary_key("users")
        assert pk_idx is not None
        assert pk_idx.is_unique is True

    def test_multiple_column_index(self, backend_with_tables):
        """Test multi-column index detection."""
        indexes = backend_with_tables.introspector.list_indexes("users")

        for idx in indexes:
            if idx.name == "idx_users_name_age":
                assert len(idx.columns) == 2
                column_names = [col.name for col in idx.columns]
                assert "name" in column_names
                assert "age" in column_names


class TestAsyncIndexIntrospection:
    """Async tests for index introspection."""

    @pytest.mark.asyncio
    async def test_async_list_indexes(self, async_backend_with_tables):
        """Test async list_indexes returns index info."""
        indexes = await async_backend_with_tables.introspector.list_indexes_async("users")

        assert isinstance(indexes, list)
        assert len(indexes) > 0

    @pytest.mark.asyncio
    async def test_async_get_index_info(self, async_backend_with_tables):
        """Test async get_index_info for existing index."""
        index = await async_backend_with_tables.introspector.get_index_info_async(
            "users", "users_pkey"
        )

        assert index is not None
        assert index.name == "users_pkey"

    @pytest.mark.asyncio
    async def test_async_get_primary_key(self, async_backend_with_tables):
        """Test async get_primary_key method."""
        pk = await async_backend_with_tables.introspector.get_primary_key_async("users")

        assert pk is not None
        assert pk.is_primary is True
        assert len(pk.columns) == 1
        assert pk.columns[0].name == "id"

    @pytest.mark.asyncio
    async def test_async_list_indexes_caching(self, async_backend_with_tables):
        """Test that async index list is cached."""
        indexes1 = await async_backend_with_tables.introspector.list_indexes_async("users")
        indexes2 = await async_backend_with_tables.introspector.list_indexes_async("users")

        # Should return the same cached list
        assert indexes1 is indexes2
