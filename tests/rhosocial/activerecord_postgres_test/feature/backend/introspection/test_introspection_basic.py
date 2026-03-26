# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_basic.py
"""
Tests for basic PostgreSQL introspection functionality.

This module tests the core introspection methods:
- introspector availability
- get_database_info
"""

import pytest

from rhosocial.activerecord.backend.introspection.types import DatabaseInfo
from rhosocial.activerecord.backend.introspection.backend_mixin import IntrospectorBackendMixin
from rhosocial.activerecord.backend.introspection.base import AbstractIntrospector


class TestIntrospectionSupport:
    """Tests for introspection capability detection."""

    def test_has_introspector(self, postgres_backend_single):
        """Test that backend has an introspector attribute."""
        assert hasattr(postgres_backend_single, "introspector")
        assert postgres_backend_single.introspector is not None

    def test_introspector_is_abstract_introspector(self, postgres_backend_single):
        """Test that introspector is an AbstractIntrospector instance."""
        assert isinstance(postgres_backend_single.introspector, AbstractIntrospector)

    def test_backend_is_introspector_mixin(self, postgres_backend_single):
        """Test that backend uses IntrospectorBackendMixin."""
        assert isinstance(postgres_backend_single, IntrospectorBackendMixin)

    def test_introspector_has_list_tables(self, postgres_backend_single):
        """Test that introspector supports table introspection."""
        assert hasattr(postgres_backend_single.introspector, "list_tables")
        assert callable(postgres_backend_single.introspector.list_tables)

    def test_introspector_has_list_columns(self, postgres_backend_single):
        """Test that introspector supports column introspection."""
        assert hasattr(postgres_backend_single.introspector, "list_columns")
        assert callable(postgres_backend_single.introspector.list_columns)

    def test_introspector_has_list_indexes(self, postgres_backend_single):
        """Test that introspector supports index introspection."""
        assert hasattr(postgres_backend_single.introspector, "list_indexes")
        assert callable(postgres_backend_single.introspector.list_indexes)

    def test_introspector_has_list_foreign_keys(self, postgres_backend_single):
        """Test that introspector supports foreign key introspection."""
        assert hasattr(postgres_backend_single.introspector, "list_foreign_keys")
        assert callable(postgres_backend_single.introspector.list_foreign_keys)

    def test_introspector_has_list_views(self, postgres_backend_single):
        """Test that introspector supports view introspection."""
        assert hasattr(postgres_backend_single.introspector, "list_views")
        assert callable(postgres_backend_single.introspector.list_views)


class TestGetDatabaseInfo:
    """Tests for get_database_info method."""

    def test_get_database_info_returns_info(self, postgres_backend_single):
        """Test that get_database_info returns DatabaseInfo."""
        info = postgres_backend_single.introspector.get_database_info()

        assert isinstance(info, DatabaseInfo)
        assert info.name is not None
        assert info.version is not None
        assert info.vendor == "PostgreSQL"

    def test_get_database_info_has_version_tuple(self, postgres_backend_single):
        """Test that database info has version tuple."""
        info = postgres_backend_single.introspector.get_database_info()

        assert info.version_tuple is not None
        assert isinstance(info.version_tuple, tuple)
        assert len(info.version_tuple) >= 2

    def test_get_database_info_caching(self, postgres_backend_single):
        """Test that database info is cached."""
        info1 = postgres_backend_single.introspector.get_database_info()
        info2 = postgres_backend_single.introspector.get_database_info()

        # Should return the same cached object
        assert info1 is info2
