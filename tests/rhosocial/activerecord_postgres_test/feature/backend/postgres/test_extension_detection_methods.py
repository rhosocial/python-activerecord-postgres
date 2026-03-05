# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_extension_detection_methods.py
"""
Integration tests for PostgresExtensionMixin methods:
- detect_extensions()
- check_extension_feature()
- get_extension_min_version_for_feature()

These tests use real database connections to verify extension detection logic.
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.impl.postgres.mixins import PostgresExtensionMixin
from rhosocial.activerecord.backend.impl.postgres.protocols import PostgresExtensionInfo


class TestDetectExtensionsWithDatabase:
    """Tests for detect_extensions method with real database."""

    def test_detect_extensions_returns_dict(self, postgres_backend):
        """Test that detect_extensions returns a dictionary."""
        backend = postgres_backend

        extensions = backend.dialect.detect_extensions(backend._connection)

        assert isinstance(extensions, dict)

    def test_detect_extensions_includes_known_extensions(self, postgres_backend):
        """Test that all known extensions are included in result."""
        backend = postgres_backend

        extensions = backend.dialect.detect_extensions(backend._connection)

        for ext_name in PostgresExtensionMixin.KNOWN_EXTENSIONS:
            assert ext_name in extensions
            assert isinstance(extensions[ext_name], PostgresExtensionInfo)
            assert extensions[ext_name].name == ext_name

    def test_detect_extensions_plpgsql_typically_installed(self, postgres_backend):
        """Test that plpgsql extension is typically installed in PostgreSQL."""
        backend = postgres_backend

        extensions = backend.dialect.detect_extensions(backend._connection)

        assert 'plpgsql' in extensions
        assert isinstance(extensions['plpgsql'], PostgresExtensionInfo)

    def test_detect_extensions_known_not_installed_marked_correctly(self, postgres_backend):
        """Test that known but not installed extensions are marked correctly."""
        backend = postgres_backend

        extensions = backend.dialect.detect_extensions(backend._connection)

        for ext_name, ext_info in extensions.items():
            assert hasattr(ext_info, 'installed')
            assert hasattr(ext_info, 'version')
            assert hasattr(ext_info, 'schema')
            assert isinstance(ext_info.installed, bool)
            if not ext_info.installed:
                assert ext_info.version is None
                assert ext_info.schema is None

    def test_detect_extensions_after_introspect(self, postgres_backend):
        """Test that detect_extensions is called during introspect_and_adapt."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        assert hasattr(backend.dialect, '_extensions')
        assert isinstance(backend.dialect._extensions, dict)

    def test_detect_extensions_consistency(self, postgres_backend):
        """Test that multiple calls to detect_extensions return consistent results."""
        backend = postgres_backend

        extensions1 = backend.dialect.detect_extensions(backend._connection)
        extensions2 = backend.dialect.detect_extensions(backend._connection)

        assert set(extensions1.keys()) == set(extensions2.keys())
        for ext_name in extensions1:
            assert extensions1[ext_name].installed == extensions2[ext_name].installed


class TestCheckExtensionFeatureWithDatabase:
    """Tests for check_extension_feature method with real database."""

    def test_check_feature_for_known_installed_extension(self, postgres_backend):
        """Test check_extension_feature for known installed extensions."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('plpgsql', 'any_feature')

        assert isinstance(result, bool)

    def test_check_feature_for_known_not_installed_extension(self, postgres_backend):
        """Test check_extension_feature for known but not installed extensions."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        for ext_name in ['postgis', 'vector', 'pg_trgm', 'hstore']:
            if not backend.dialect.is_extension_installed(ext_name):
                result = backend.dialect.check_extension_feature(ext_name, 'type')
                assert result is False

    def test_check_feature_vector_type_when_installed(self, postgres_backend):
        """Test check_extension_feature for vector type when installed."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('vector', 'type')

        assert isinstance(result, bool)

    def test_check_feature_vector_hnsw_when_installed(self, postgres_backend):
        """Test check_extension_feature for vector HNSW index when installed."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('vector', 'hnsw_index')

        assert isinstance(result, bool)

    def test_check_feature_postgis_geometry_when_installed(self, postgres_backend):
        """Test check_extension_feature for PostGIS geometry when installed."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('postgis', 'geometry_type')

        assert isinstance(result, bool)

    def test_check_feature_pg_trgm_similarity_when_installed(self, postgres_backend):
        """Test check_extension_feature for pg_trgm similarity when installed."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('pg_trgm', 'similarity')

        assert isinstance(result, bool)

    def test_check_feature_hstore_type_when_installed(self, postgres_backend):
        """Test check_extension_feature for hstore type when installed."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('hstore', 'type')

        assert isinstance(result, bool)

    def test_check_feature_unknown_extension(self, postgres_backend):
        """Test check_extension_feature for unknown extension."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('unknown_extension', 'some_feature')

        assert isinstance(result, bool)

    def test_check_feature_unknown_feature(self, postgres_backend):
        """Test check_extension_feature for unknown feature of known extension."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('plpgsql', 'unknown_feature')

        assert result is True

    def test_check_feature_without_introspect(self, postgres_backend):
        """Test check_extension_feature before introspect_and_adapt is called."""
        backend = postgres_backend

        result = backend.dialect.check_extension_feature('vector', 'type')

        assert result is False


class TestGetExtensionMinVersionForFeatureWithDatabase:
    """Tests for get_extension_min_version_for_feature method with real database."""

    def test_get_min_version_vector_hnsw(self, postgres_backend):
        """Test minimum version for vector hnsw_index feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('vector', 'hnsw_index')

        assert result == '0.5.0'

    def test_get_min_version_vector_type(self, postgres_backend):
        """Test minimum version for vector type feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('vector', 'type')

        assert result == '0.1'

    def test_get_min_version_vector_similarity_search(self, postgres_backend):
        """Test minimum version for vector similarity_search feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('vector', 'similarity_search')

        assert result == '0.1'

    def test_get_min_version_vector_ivfflat_index(self, postgres_backend):
        """Test minimum version for vector ivfflat_index feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('vector', 'ivfflat_index')

        assert result == '0.1'

    def test_get_min_version_postgis_geometry_type(self, postgres_backend):
        """Test minimum version for PostGIS geometry_type feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('postgis', 'geometry_type')

        assert result == '2.0'

    def test_get_min_version_postgis_geography_type(self, postgres_backend):
        """Test minimum version for PostGIS geography_type feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('postgis', 'geography_type')

        assert result == '2.0'

    def test_get_min_version_postgis_spatial_index(self, postgres_backend):
        """Test minimum version for PostGIS spatial_index feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('postgis', 'spatial_index')

        assert result == '2.0'

    def test_get_min_version_postgis_spatial_functions(self, postgres_backend):
        """Test minimum version for PostGIS spatial_functions feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('postgis', 'spatial_functions')

        assert result == '2.0'

    def test_get_min_version_pg_trgm_similarity(self, postgres_backend):
        """Test minimum version for pg_trgm similarity feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('pg_trgm', 'similarity')

        assert result == '1.0'

    def test_get_min_version_pg_trgm_index(self, postgres_backend):
        """Test minimum version for pg_trgm index feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('pg_trgm', 'index')

        assert result == '1.0'

    def test_get_min_version_hstore_type(self, postgres_backend):
        """Test minimum version for hstore type feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('hstore', 'type')

        assert result == '1.0'

    def test_get_min_version_hstore_operators(self, postgres_backend):
        """Test minimum version for hstore operators feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('hstore', 'operators')

        assert result == '1.0'

    def test_get_min_version_unknown_extension(self, postgres_backend):
        """Test minimum version for unknown extension."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('unknown_ext', 'feature')

        assert result is None

    def test_get_min_version_unknown_feature(self, postgres_backend):
        """Test minimum version for unknown feature."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('vector', 'unknown_feature')

        assert result is None

    def test_get_min_version_extension_no_features(self, postgres_backend):
        """Test minimum version for extension with no features defined."""
        backend = postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('uuid-ossp', 'any_feature')

        assert result is None

    def test_get_min_version_ltree_features(self, postgres_backend):
        """Test minimum version for ltree features."""
        backend = postgres_backend

        assert backend.dialect.get_extension_min_version_for_feature('ltree', 'type') == '1.0'
        assert backend.dialect.get_extension_min_version_for_feature('ltree', 'operators') == '1.0'
        assert backend.dialect.get_extension_min_version_for_feature('ltree', 'index') == '1.0'

    def test_get_min_version_intarray_features(self, postgres_backend):
        """Test minimum version for intarray features."""
        backend = postgres_backend

        assert backend.dialect.get_extension_min_version_for_feature('intarray', 'operators') == '1.0'
        assert backend.dialect.get_extension_min_version_for_feature('intarray', 'functions') == '1.0'
        assert backend.dialect.get_extension_min_version_for_feature('intarray', 'index') == '1.0'


class TestExtensionFeatureIntegrationWithDatabase:
    """Integration tests combining multiple methods with real database."""

    def test_detect_check_min_version_workflow(self, postgres_backend):
        """Test typical workflow: detect, check, and get min version."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        for ext_name in ['vector', 'postgis', 'pg_trgm', 'hstore']:
            min_version = backend.dialect.get_extension_min_version_for_feature(ext_name, 'type')
            if min_version:
                is_installed = backend.dialect.is_extension_installed(ext_name)
                check_result = backend.dialect.check_extension_feature(ext_name, 'type')

                assert isinstance(is_installed, bool)
                assert isinstance(check_result, bool)

    def test_extension_detection_consistency(self, postgres_backend):
        """Test that extension detection results are consistent."""
        backend = postgres_backend

        extensions1 = backend.dialect.detect_extensions(backend._connection)
        backend.introspect_and_adapt()

        for ext_name in extensions1:
            is_installed = backend.dialect.is_extension_installed(ext_name)
            ext_info = backend.dialect.get_extension_info(ext_name)

            if ext_info:
                assert ext_info.installed == is_installed
                if is_installed:
                    assert ext_info.version is not None
                    assert ext_info.schema is not None

    def test_all_known_extensions_have_methods(self, postgres_backend):
        """Test that dialect has methods for all known extensions."""
        backend = postgres_backend
        backend.introspect_and_adapt()

        for ext_name in PostgresExtensionMixin.KNOWN_EXTENSIONS:
            assert backend.dialect.is_extension_installed(ext_name) is not None
            assert backend.dialect.get_extension_info(ext_name) is not None

    def test_extension_version_string_format(self, postgres_backend):
        """Test that extension version strings are properly formatted."""
        backend = postgres_backend
        extensions = backend.dialect.detect_extensions(backend._connection)

        for ext_name, ext_info in extensions.items():
            if ext_info.installed and ext_info.version:
                assert isinstance(ext_info.version, str)
                assert len(ext_info.version) > 0


class TestExtensionDetectionMethodsAsync:
    """Async tests for extension detection methods."""

    async def test_extension_detection_after_introspect_async(self, async_postgres_backend):
        """Test that extensions are detected after introspect_and_adapt in async context."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        assert hasattr(backend.dialect, '_extensions')
        assert isinstance(backend.dialect._extensions, dict)

    async def test_check_extension_feature_async(self, async_postgres_backend):
        """Test check_extension_feature in async context."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        result = backend.dialect.check_extension_feature('vector', 'type')

        assert isinstance(result, bool)

    async def test_get_min_version_async(self, async_postgres_backend):
        """Test get_extension_min_version_for_feature in async context."""
        backend = async_postgres_backend

        result = backend.dialect.get_extension_min_version_for_feature('vector', 'hnsw_index')

        assert result == '0.5.0'

    async def test_extension_workflow_async(self, async_postgres_backend):
        """Test full extension detection workflow in async context."""
        backend = async_postgres_backend
        await backend.introspect_and_adapt()

        for ext_name in PostgresExtensionMixin.KNOWN_EXTENSIONS:
            is_installed = backend.dialect.is_extension_installed(ext_name)
            ext_info = backend.dialect.get_extension_info(ext_name)

            assert isinstance(is_installed, bool)
            assert ext_info is not None
