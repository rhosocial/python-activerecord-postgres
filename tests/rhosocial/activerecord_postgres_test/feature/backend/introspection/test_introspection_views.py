# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_views.py
"""
Tests for PostgreSQL view introspection functionality.

This module tests the list_views and get_view_info methods for retrieving view metadata.
"""

import pytest

from rhosocial.activerecord.backend.introspection.types import ViewInfo


# SQL statements for async view tests (duplicated from conftest to avoid import issues)
_VIEW_SQL = [
    "DROP VIEW IF EXISTS user_summary CASCADE",
    "DROP TABLE IF EXISTS user_stats CASCADE",
    "DROP TABLE IF EXISTS users CASCADE",
    """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) NOT NULL
    )
    """,
    """
    CREATE TABLE user_stats (
        user_id INTEGER PRIMARY KEY,
        post_count INTEGER DEFAULT 0
    )
    """,
    """
    CREATE VIEW user_summary AS
    SELECT u.id, u.name, u.email, COALESCE(s.post_count, 0) as post_count
    FROM users u
    LEFT JOIN user_stats s ON u.id = s.user_id
    """,
]

_CLEANUP_VIEW_SQL = [
    "DROP VIEW IF EXISTS user_summary CASCADE",
    "DROP TABLE IF EXISTS user_stats CASCADE",
    "DROP TABLE IF EXISTS users CASCADE",
]


class TestListViews:
    """Tests for list_views method."""

    def test_list_views_returns_list(self, backend_with_view):
        """Test that list_views returns a list."""
        views = backend_with_view.introspector.list_views()

        assert isinstance(views, list)
        assert len(views) > 0

    def test_list_views_includes_expected_views(self, backend_with_view):
        """Test that list_views includes expected views."""
        views = backend_with_view.introspector.list_views()
        view_names = [v.name for v in views]

        assert "user_summary" in view_names

    def test_list_views_excludes_system_views(self, backend_with_view):
        """Test that list_views excludes system views by default."""
        views = backend_with_view.introspector.list_views()

        # PostgreSQL system views are in pg_catalog or information_schema
        for view in views:
            if view.schema:
                assert view.schema not in ["pg_catalog", "information_schema"]

    def test_list_views_caching(self, backend_with_view):
        """Test that view list is cached."""
        views1 = backend_with_view.introspector.list_views()
        views2 = backend_with_view.introspector.list_views()

        # Should return the same cached list
        assert views1 is views2


class TestGetViewInfo:
    """Tests for get_view_info method."""

    def test_get_view_info_existing(self, backend_with_view):
        """Test get_view_info for existing view."""
        view = backend_with_view.introspector.get_view_info("user_summary")

        assert view is not None
        assert isinstance(view, ViewInfo)
        assert view.name == "user_summary"

    def test_get_view_info_nonexistent(self, backend_with_view):
        """Test get_view_info for non-existent view."""
        view = backend_with_view.introspector.get_view_info("nonexistent_view")

        assert view is None

    def test_get_view_info_includes_definition(self, backend_with_view):
        """Test that get_view_info includes view definition."""
        view = backend_with_view.introspector.get_view_info("user_summary")

        assert view is not None
        assert view.definition is not None
        assert "SELECT" in view.definition.upper()


class TestViewInfoDetails:
    """Tests for ViewInfo detail properties."""

    def test_view_schema(self, backend_with_view):
        """Test that view schema is correctly identified."""
        views = backend_with_view.introspector.list_views()

        for view in views:
            if view.name == "user_summary":
                # Default schema in PostgreSQL is 'public'
                assert view.schema == "public"

    def test_view_columns(self, backend_with_view):
        """Test that view columns are correctly identified via list_columns."""
        columns = backend_with_view.introspector.list_columns("user_summary")

        assert columns is not None
        column_names = [col.name for col in columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "email" in column_names
        assert "post_count" in column_names


class TestAsyncViewIntrospection:
    """Async tests for view introspection."""

    @pytest.mark.asyncio
    async def test_async_list_views(self, async_postgres_backend_single):
        """Test async list_views returns view info."""
        # Create view manually for async test
        for sql in _VIEW_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        views = await async_postgres_backend_single.introspector.list_views()

        assert isinstance(views, list)
        assert len(views) > 0

        # Cleanup
        for sql in _CLEANUP_VIEW_SQL:
            await async_postgres_backend_single.execute(sql)

    @pytest.mark.asyncio
    async def test_async_get_view_info(self, async_postgres_backend_single):
        """Test async get_view_info for existing view."""
        for sql in _VIEW_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        view = await async_postgres_backend_single.introspector.get_view_info("user_summary")

        assert view is not None
        assert view.name == "user_summary"

        # Cleanup
        for sql in _CLEANUP_VIEW_SQL:
            await async_postgres_backend_single.execute(sql)
