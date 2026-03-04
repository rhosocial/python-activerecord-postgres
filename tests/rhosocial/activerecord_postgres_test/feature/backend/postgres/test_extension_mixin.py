# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_extension_mixin.py
"""
Integration tests for PostgreSQL extension-related mixins.

These tests verify the extension detection and feature support functionality
using a real PostgreSQL database connection.
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.mixins import (
    PostgresExtensionMixin,
    PostgresMaterializedViewMixin,
    PostgresTableMixin,
)
from rhosocial.activerecord.backend.impl.postgres.protocols import PostgresExtensionInfo


class TestPostgresExtensionMixinKnownExtensions:
    """Tests for KNOWN_EXTENSIONS constant."""

    def test_known_extensions_is_dict(self):
        """Test that KNOWN_EXTENSIONS is a dictionary."""
        assert isinstance(PostgresExtensionMixin.KNOWN_EXTENSIONS, dict)

    def test_known_extensions_contains_postgis(self):
        """Test that postgis is in known extensions."""
        assert 'postgis' in PostgresExtensionMixin.KNOWN_EXTENSIONS
        assert 'min_version' in PostgresExtensionMixin.KNOWN_EXTENSIONS['postgis']
        assert 'description' in PostgresExtensionMixin.KNOWN_EXTENSIONS['postgis']
        assert PostgresExtensionMixin.KNOWN_EXTENSIONS['postgis']['category'] == 'spatial'

    def test_known_extensions_contains_vector(self):
        """Test that vector (pgvector) is in known extensions."""
        assert 'vector' in PostgresExtensionMixin.KNOWN_EXTENSIONS
        assert 'min_version' in PostgresExtensionMixin.KNOWN_EXTENSIONS['vector']
        assert 'description' in PostgresExtensionMixin.KNOWN_EXTENSIONS['vector']
        assert PostgresExtensionMixin.KNOWN_EXTENSIONS['vector']['category'] == 'vector'

    def test_known_extensions_contains_pg_trgm(self):
        """Test that pg_trgm is in known extensions."""
        assert 'pg_trgm' in PostgresExtensionMixin.KNOWN_EXTENSIONS
        assert 'min_version' in PostgresExtensionMixin.KNOWN_EXTENSIONS['pg_trgm']
        assert PostgresExtensionMixin.KNOWN_EXTENSIONS['pg_trgm']['category'] == 'text'

    def test_known_extensions_contains_hstore(self):
        """Test that hstore is in known extensions."""
        assert 'hstore' in PostgresExtensionMixin.KNOWN_EXTENSIONS
        assert PostgresExtensionMixin.KNOWN_EXTENSIONS['hstore']['category'] == 'data'

    def test_known_extensions_contains_uuid_ossp(self):
        """Test that uuid-ossp is in known extensions."""
        assert 'uuid-ossp' in PostgresExtensionMixin.KNOWN_EXTENSIONS
        assert PostgresExtensionMixin.KNOWN_EXTENSIONS['uuid-ossp']['category'] == 'utility'

    def test_known_extensions_contains_pgcrypto(self):
        """Test that pgcrypto is in known extensions."""
        assert 'pgcrypto' in PostgresExtensionMixin.KNOWN_EXTENSIONS
        assert PostgresExtensionMixin.KNOWN_EXTENSIONS['pgcrypto']['category'] == 'security'

    def test_all_extensions_have_required_fields(self):
        """Test that all known extensions have required fields."""
        required_fields = ['min_version', 'description', 'category']
        for ext_name, ext_info in PostgresExtensionMixin.KNOWN_EXTENSIONS.items():
            for field in required_fields:
                assert field in ext_info, f"Extension {ext_name} missing {field}"


class TestPostgresMaterializedViewMixin:
    """Tests for PostgresMaterializedViewMixin version detection."""

    def test_concurrent_refresh_not_supported_before_9_4(self):
        """Test that CONCURRENTLY is not supported before PostgreSQL 9.4."""
        mixin = PostgresMaterializedViewMixin()
        mixin.version = (9, 3, 0)
        assert mixin.supports_materialized_view_concurrent_refresh() is False

    def test_concurrent_refresh_supported_at_9_4(self):
        """Test that CONCURRENTLY is supported at PostgreSQL 9.4."""
        mixin = PostgresMaterializedViewMixin()
        mixin.version = (9, 4, 0)
        assert mixin.supports_materialized_view_concurrent_refresh() is True

    def test_concurrent_refresh_supported_after_9_4(self):
        """Test that CONCURRENTLY is supported after PostgreSQL 9.4."""
        for version in [(9, 5, 0), (10, 0, 0), (11, 0, 0), (12, 0, 0), (13, 0, 0), (14, 0, 0)]:
            mixin = PostgresMaterializedViewMixin()
            mixin.version = version
            assert mixin.supports_materialized_view_concurrent_refresh() is True, \
                f"CONCURRENTLY should be supported for version {version}"


class TestPostgresTableMixin:
    """Tests for PostgresTableMixin."""

    def test_table_inheritance_always_supported(self):
        """Test that PostgreSQL always supports table inheritance."""
        mixin = PostgresTableMixin()
        assert mixin.supports_table_inheritance() is True


class TestPostgresExtensionMixinVersionCompare:
    """Tests for _compare_versions method."""

    def test_equal_versions(self):
        """Test comparison of equal versions."""
        mixin = PostgresExtensionMixin()
        assert mixin._compare_versions('1.0.0', '1.0.0') == 0

    def test_greater_version(self):
        """Test when first version is greater."""
        mixin = PostgresExtensionMixin()
        assert mixin._compare_versions('2.0.0', '1.0.0') > 0
        assert mixin._compare_versions('1.2.0', '1.1.0') > 0
        assert mixin._compare_versions('1.1.2', '1.1.1') > 0

    def test_lesser_version(self):
        """Test when first version is lesser."""
        mixin = PostgresExtensionMixin()
        assert mixin._compare_versions('1.0.0', '2.0.0') < 0
        assert mixin._compare_versions('1.1.0', '1.2.0') < 0
        assert mixin._compare_versions('1.1.1', '1.1.2') < 0

    def test_different_length_versions(self):
        """Test versions with different number of components."""
        mixin = PostgresExtensionMixin()
        assert mixin._compare_versions('1.0', '1.0.0') == 0
        assert mixin._compare_versions('1', '1.0.0') == 0
        assert mixin._compare_versions('1.0.1', '1.0') > 0

    def test_development_versions(self):
        """Test versions with development suffix."""
        mixin = PostgresExtensionMixin()
        # Development versions should be comparable
        result = mixin._compare_versions('0.5.0.dev', '0.5.0')
        assert isinstance(result, int)

    def test_pgvector_version_comparisons(self):
        """Test realistic pgvector version comparisons."""
        mixin = PostgresExtensionMixin()
        assert mixin._compare_versions('0.4.4', '0.5.0') < 0
        assert mixin._compare_versions('0.5.0', '0.5.0') == 0
        assert mixin._compare_versions('0.5.1', '0.5.0') > 0

    def test_postgresql_major_versions(self):
        """Test PostgreSQL major version comparisons."""
        mixin = PostgresExtensionMixin()
        assert mixin._compare_versions('9.6', '10.0') < 0
        assert mixin._compare_versions('10.0', '11.0') < 0
        assert mixin._compare_versions('14.0', '13.0') > 0


class TestPostgresExtensionInfo:
    """Tests for PostgresExtensionInfo dataclass."""

    def test_extension_info_creation(self):
        """Test creating PostgresExtensionInfo."""
        info = PostgresExtensionInfo(
            name='vector',
            installed=True,
            version='0.5.0',
            schema='public'
        )
        assert info.name == 'vector'
        assert info.installed is True
        assert info.version == '0.5.0'
        assert info.schema == 'public'

    def test_extension_info_defaults(self):
        """Test PostgresExtensionInfo default values."""
        info = PostgresExtensionInfo(name='test')
        assert info.name == 'test'
        assert info.installed is False
        assert info.version is None
        assert info.schema is None


class TestPostgresExtensionMixinWithDatabase:
    """Tests that require database connection."""

    def test_is_extension_installed_returns_bool(self, postgres_backend):
        """Test that is_extension_installed returns boolean."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.is_extension_installed('nonexistent_extension_xyz')
        assert isinstance(result, bool)
        assert result is False

    def test_get_extension_version_returns_none_for_unknown(self, postgres_backend):
        """Test get_extension_version returns None for unknown extension."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.get_extension_version('nonexistent_extension_xyz')
        assert result is None

    def test_get_extension_info_returns_none_for_unknown(self, postgres_backend):
        """Test get_extension_info returns None for unknown extension."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.get_extension_info('nonexistent_extension_xyz')
        assert result is None

    def test_dialect_has_extension_methods(self, postgres_backend):
        """Test that dialect has extension-related methods."""
        backend = postgres_backend

        assert hasattr(backend.dialect, 'is_extension_installed')
        assert hasattr(backend.dialect, 'get_extension_version')
        assert hasattr(backend.dialect, 'get_extension_info')

    def test_materialized_view_concurrent_refresh_detection(self, postgres_backend):
        """Test materialized view concurrent refresh detection."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        supports = backend.dialect.supports_materialized_view_concurrent_refresh()
        assert isinstance(supports, bool)

    def test_table_inheritance_detection(self, postgres_backend):
        """Test table inheritance detection."""
        backend = postgres_backend

        # Table inheritance is always supported in PostgreSQL
        assert backend.dialect.supports_table_inheritance() is True


class TestPostgresExtensionMixinAsync:
    """Async tests for extension mixin functionality."""

    async def test_is_extension_installed_async(self, async_postgres_backend):
        """Test is_extension_installed in async context."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        result = backend.dialect.is_extension_installed('nonexistent_extension_xyz')
        assert result is False

    async def test_get_extension_version_async(self, async_postgres_backend):
        """Test get_extension_version in async context."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        result = backend.dialect.get_extension_version('nonexistent_extension_xyz')
        assert result is None

    async def test_materialized_view_concurrent_refresh_async(self, async_postgres_backend):
        """Test materialized view concurrent refresh in async context."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        supports = backend.dialect.supports_materialized_view_concurrent_refresh()
        assert isinstance(supports, bool)

    async def test_table_inheritance_async(self, async_postgres_backend):
        """Test table inheritance in async context."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        assert backend.dialect.supports_table_inheritance() is True
