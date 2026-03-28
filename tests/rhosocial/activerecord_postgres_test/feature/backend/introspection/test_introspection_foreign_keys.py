# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_foreign_keys.py
"""
Tests for PostgreSQL foreign key introspection functionality.

This module tests the list_foreign_keys and get_foreign_key_info methods for retrieving foreign key metadata.
"""

import pytest

from rhosocial.activerecord.backend.introspection.types import (
    ForeignKeyInfo,
    ReferentialAction,
)


class TestListForeignKeys:
    """Tests for list_foreign_keys method."""

    def test_list_foreign_keys_returns_list(self, backend_with_tables):
        """Test that list_foreign_keys returns a list."""
        fks = backend_with_tables.introspector.list_foreign_keys("posts")

        assert isinstance(fks, list)
        assert len(fks) > 0

    def test_list_foreign_keys_includes_expected_fks(self, backend_with_tables):
        """Test that list_foreign_keys includes expected foreign keys."""
        fks = backend_with_tables.introspector.list_foreign_keys("posts")
        fk_names = [fk.name for fk in fks]

        assert "fk_posts_user" in fk_names

    def test_list_foreign_keys_nonexistent_table(self, backend_with_tables):
        """Test list_foreign_keys for non-existent table."""
        fks = backend_with_tables.introspector.list_foreign_keys("nonexistent_table")

        assert isinstance(fks, list)
        assert len(fks) == 0

    def test_list_foreign_keys_caching(self, backend_with_tables):
        """Test that foreign key list is cached."""
        fks1 = backend_with_tables.introspector.list_foreign_keys("posts")
        fks2 = backend_with_tables.introspector.list_foreign_keys("posts")

        # Should return the same cached list
        assert fks1 is fks2

    def test_list_foreign_keys_with_schema(self, backend_with_tables):
        """Test list_foreign_keys with schema parameter."""
        # PostgreSQL default schema is 'public'
        fks = backend_with_tables.introspector.list_foreign_keys("posts", schema="public")

        assert isinstance(fks, list)
        assert len(fks) > 0


class TestGetForeignKeyInfo:
    """Tests for get_foreign_key_info method."""

    def test_get_foreign_key_info_existing(self, backend_with_tables):
        """Test get_foreign_key_info for existing foreign key."""
        fk = backend_with_tables.introspector.get_foreign_key_info(
            "posts", "fk_posts_user"
        )

        assert fk is not None
        assert isinstance(fk, ForeignKeyInfo)
        assert fk.name == "fk_posts_user"
        assert fk.table_name == "posts"

    def test_get_foreign_key_info_nonexistent(self, backend_with_tables):
        """Test get_foreign_key_info for non-existent foreign key."""
        fk = backend_with_tables.introspector.get_foreign_key_info(
            "posts", "nonexistent_fk"
        )

        assert fk is None

    def test_get_foreign_key_info_nonexistent_table(self, backend_with_tables):
        """Test get_foreign_key_info for non-existent table."""
        fk = backend_with_tables.introspector.get_foreign_key_info(
            "nonexistent_table", "some_fk"
        )

        assert fk is None


class TestForeignKeyDetails:
    """Tests for foreign key detail properties."""

    def test_foreign_key_referenced_table(self, backend_with_tables):
        """Test that referenced table is correctly identified."""
        fks = backend_with_tables.introspector.list_foreign_keys("posts")

        for fk in fks:
            if fk.name == "fk_posts_user":
                assert fk.referenced_table == "users"
                assert fk.referenced_schema == "public"

    def test_foreign_key_columns(self, backend_with_tables):
        """Test that foreign key columns are correctly identified."""
        fks = backend_with_tables.introspector.list_foreign_keys("posts")

        for fk in fks:
            if fk.name == "fk_posts_user":
                assert len(fk.columns) == 1
                assert "user_id" in fk.columns
                assert len(fk.referenced_columns) == 1
                assert "id" in fk.referenced_columns

    def test_foreign_key_on_delete_cascade(self, backend_with_tables):
        """Test that ON DELETE CASCADE is correctly identified."""
        fks = backend_with_tables.introspector.list_foreign_keys("posts")

        for fk in fks:
            if fk.name == "fk_posts_user":
                assert fk.on_delete == ReferentialAction.CASCADE
    def test_foreign_key_on_update_no_action(self, backend_with_tables):
        """Test that ON UPDATE NO ACTION is default."""
        fks = backend_with_tables.introspector.list_foreign_keys("posts")

        for fk in fks:
            if fk.name == "fk_posts_user":
                # Default is NO ACTION
                assert fk.on_update == ReferentialAction.NO_ACTION
    def test_composite_foreign_key(self, backend_with_tables):
        """Test composite foreign key detection."""
        # Create a table with composite primary key as reference target
        backend_with_tables.execute("DROP TABLE IF EXISTS composite_fk_test CASCADE")
        backend_with_tables.execute("DROP TABLE IF EXISTS ref_table CASCADE")
        backend_with_tables.execute("""
            CREATE TABLE ref_table (
                id1 INTEGER NOT NULL,
                id2 INTEGER NOT NULL,
                value TEXT,
                PRIMARY KEY (id1, id2)
            )
        """)
        backend_with_tables.execute("""
            CREATE TABLE composite_fk_test (
                id SERIAL PRIMARY KEY,
                ref_id1 INTEGER NOT NULL,
                ref_id2 INTEGER NOT NULL,
                value TEXT,
                CONSTRAINT fk_composite
                    FOREIGN KEY (ref_id1, ref_id2)
                    REFERENCES ref_table(id1, id2)
            )
        """)

        backend_with_tables.introspector.clear_cache()
        fks = backend_with_tables.introspector.list_foreign_keys("composite_fk_test")

        assert len(fks) == 1
        assert fks[0].name == "fk_composite"
        assert len(fks[0].columns) == 2
        assert len(fks[0].referenced_columns) == 2
        # Cleanup
        backend_with_tables.execute("DROP TABLE IF EXISTS composite_fk_test CASCADE")
        backend_with_tables.execute("DROP TABLE IF EXISTS ref_table CASCADE")


class TestAsyncForeignKeyIntrospection:
    """Async tests for foreign key introspection."""

    @pytest.mark.asyncio
    async def test_async_list_foreign_keys(self, async_backend_with_tables):
        """Test async list_foreign_keys returns foreign key info."""
        fks = await async_backend_with_tables.introspector.list_foreign_keys("posts")

        assert isinstance(fks, list)
        assert len(fks) > 0

    @pytest.mark.asyncio
    async def test_async_get_foreign_key_info(self, async_backend_with_tables):
        """Test async get_foreign_key_info for existing foreign key."""
        fk = await async_backend_with_tables.introspector.get_foreign_key_info(
            "posts", "fk_posts_user"
        )

        assert fk is not None
        assert fk.name == "fk_posts_user"
    @pytest.mark.asyncio
    async def test_async_list_foreign_keys_caching(self, async_backend_with_tables):
        """Test that async foreign key list is cached."""
        fks1 = await async_backend_with_tables.introspector.list_foreign_keys("posts")
        fks2 = await async_backend_with_tables.introspector.list_foreign_keys("posts")

        # Should return the same cached list
        assert fks1 is fks2
