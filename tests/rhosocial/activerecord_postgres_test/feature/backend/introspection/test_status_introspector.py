# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_status_introspector.py
"""
Tests for PostgreSQL status introspector.

This module tests the SyncPostgreSQLStatusIntrospector functionality
for retrieving server status information via pg_settings, pg_stat_activity,
and other system views.
"""

import pytest

from rhosocial.activerecord.backend.introspection.status import (
    StatusItem,
    StatusCategory,
    ServerOverview,
    DatabaseBriefInfo,
    UserInfo,
    ConnectionInfo,
    StorageInfo,
    WALInfo,
    ReplicationInfo,
    ArchiveInfo,
    SecurityInfo,
    ExtensionInfo,
)


class TestSyncPostgreSQLStatusIntrospector:
    """Tests for synchronous PostgreSQL status introspector."""

    def test_get_overview(self, postgres_backend_single):
        """Test get_overview returns valid ServerOverview."""
        status = postgres_backend_single.introspector.status

        overview = status.get_overview()

        assert isinstance(overview, ServerOverview)
        assert overview.server_vendor == "PostgreSQL"
        assert overview.server_version is not None
        assert isinstance(overview.configuration, list)
        assert isinstance(overview.performance, list)
        assert isinstance(overview.storage, StorageInfo)
        assert isinstance(overview.databases, list)
        # PostgreSQL has users
        assert isinstance(overview.users, list)

    def test_get_overview_version_matches_dialect(self, postgres_backend_single):
        """Test that overview version matches dialect version."""
        status = postgres_backend_single.introspector.status
        overview = status.get_overview()

        expected_version = ".".join(map(str, postgres_backend_single.dialect.version))
        assert overview.server_version == expected_version

    def test_get_overview_contains_postgresql_version_info(self, postgres_backend_single):
        """Test that overview contains PostgreSQL version info in extra."""
        status = postgres_backend_single.introspector.status
        overview = status.get_overview()

        # PostgreSQL should have version info
        assert overview.server_version is not None

    def test_list_configuration(self, postgres_backend_single):
        """Test list_configuration returns configuration items."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration()

        assert isinstance(items, list)
        assert len(items) > 0

        # Check that all items are StatusItem instances
        for item in items:
            assert isinstance(item, StatusItem)
            assert item.name is not None
            assert item.value is not None

    def test_list_configuration_with_category_filter(self, postgres_backend_single):
        """Test list_configuration with category filter."""
        status = postgres_backend_single.introspector.status

        config_items = status.list_configuration(category=StatusCategory.CONFIGURATION)

        for item in config_items:
            assert item.category == StatusCategory.CONFIGURATION

    def test_list_configuration_contains_expected_items(self, postgres_backend_single):
        """Test that configuration contains expected PostgreSQL settings."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration()
        item_names = [item.name for item in items]

        # Check for some common PostgreSQL settings
        assert "port" in item_names
        assert "max_connections" in item_names

    def test_list_configuration_values_are_parsed(self, postgres_backend_single):
        """Test that configuration values are returned."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration()

        # port should exist and have a value
        port_item = next((i for i in items if i.name == "port"), None)
        if port_item:
            # PostgreSQL's _parse_setting_value preserves string values
            assert port_item.value is not None

    def test_list_performance_metrics(self, postgres_backend_single):
        """Test list_performance_metrics returns performance items."""
        status = postgres_backend_single.introspector.status

        items = status.list_performance_metrics()

        assert isinstance(items, list)

        # All items should be PERFORMANCE category
        for item in items:
            assert item.category == StatusCategory.PERFORMANCE

    def test_get_connection_info(self, postgres_backend_single):
        """Test get_connection_info returns ConnectionInfo."""
        status = postgres_backend_single.introspector.status

        conn_info = status.get_connection_info()

        assert isinstance(conn_info, ConnectionInfo)
        # PostgreSQL has connection info
        assert conn_info.active_count is not None or conn_info.max_connections is not None

    def test_get_storage_info(self, postgres_backend_single):
        """Test get_storage_info returns StorageInfo."""
        status = postgres_backend_single.introspector.status

        storage = status.get_storage_info()

        assert isinstance(storage, StorageInfo)
        # PostgreSQL should have storage info
        assert storage.extra is not None

    def test_list_databases(self, postgres_backend_single):
        """Test list_databases returns database list."""
        status = postgres_backend_single.introspector.status

        databases = status.list_databases()

        assert isinstance(databases, list)
        assert len(databases) >= 1

        # All items should be DatabaseBriefInfo instances
        for db in databases:
            assert isinstance(db, DatabaseBriefInfo)
            assert db.name is not None

    def test_list_databases_with_tables(self, backend_with_tables):
        """Test list_databases includes table count."""
        status = backend_with_tables.introspector.status

        databases = status.list_databases()

        # At least one database should have tables
        assert len(databases) >= 1

    def test_list_users(self, postgres_backend_single):
        """Test list_users returns user list."""
        status = postgres_backend_single.introspector.status

        users = status.list_users()

        assert isinstance(users, list)

        # PostgreSQL typically has at least one user
        for user in users:
            assert isinstance(user, UserInfo)
            assert user.name is not None

    def test_get_wal_info(self, postgres_backend_single):
        """Test get_wal_info returns WALInfo."""
        status = postgres_backend_single.introspector.status

        wal_info = status.get_wal_info()

        assert isinstance(wal_info, WALInfo)
        # PostgreSQL should have WAL info
        assert wal_info.extra is not None

    def test_get_replication_info(self, postgres_backend_single):
        """Test get_replication_info returns ReplicationInfo."""
        status = postgres_backend_single.introspector.status

        repl_info = status.get_replication_info()

        assert isinstance(repl_info, ReplicationInfo)
        # Replication may or may not be configured

    def test_get_archive_info(self, postgres_backend_single):
        """Test get_archive_info returns ArchiveInfo."""
        status = postgres_backend_single.introspector.status

        archive_info = status.get_archive_info()

        assert isinstance(archive_info, ArchiveInfo)
        # Archive info should be available

    def test_get_security_info(self, postgres_backend_single):
        """Test get_security_info returns SecurityInfo."""
        status = postgres_backend_single.introspector.status

        security_info = status.get_security_info()

        assert isinstance(security_info, SecurityInfo)
        # Security info should be available

    def test_list_extensions(self, postgres_backend_single):
        """Test list_extensions returns extension list."""
        status = postgres_backend_single.introspector.status

        extensions = status.list_extensions()

        assert isinstance(extensions, list)

        # All items should be ExtensionInfo instances
        for ext in extensions:
            assert isinstance(ext, ExtensionInfo)
            assert ext.name is not None

    def test_status_item_has_description(self, postgres_backend_single):
        """Test that status items have descriptions."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration()

        # Check that items have descriptions
        for item in items:
            assert item.description is not None

    def test_status_item_readonly_flag(self, postgres_backend_single):
        """Test that readonly items are marked correctly."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration()

        # port should be readonly (requires restart)
        port_item = next((i for i in items if i.name == "port"), None)
        if port_item:
            assert port_item.is_readonly is True


class TestPostgreSQLStatusIntrospectorMixin:
    """Tests for PostgreSQLStatusIntrospectorMixin helper methods."""

    def test_parse_setting_value_int(self, postgres_backend_single):
        """Test _parse_setting_value handles integers."""
        status = postgres_backend_single.introspector.status

        # _parse_setting_value returns int if input is int, otherwise str
        result = status._parse_setting_value(42)
        assert result == 42
        assert isinstance(result, int)

        # String "42" is not converted to int
        result_str = status._parse_setting_value("42")
        assert result_str == "42"
        assert isinstance(result_str, str)

    def test_parse_setting_value_str(self, postgres_backend_single):
        """Test _parse_setting_value preserves non-integer strings."""
        status = postgres_backend_single.introspector.status

        result = status._parse_setting_value("utf8")
        assert result == "utf8"
        assert isinstance(result, str)

    def test_parse_setting_value_on_off(self, postgres_backend_single):
        """Test _parse_setting_value handles on/off values."""
        status = postgres_backend_single.introspector.status

        result_on = status._parse_setting_value("on")
        assert result_on is True

        result_off = status._parse_setting_value("off")
        assert result_off is False

    def test_create_status_item(self, postgres_backend_single):
        """Test _create_status_item creates proper StatusItem."""
        status = postgres_backend_single.introspector.status

        item = status._create_status_item(
            name="test_param",
            value="42",
            category=StatusCategory.CONFIGURATION,
            description="Test parameter",
            unit="ms",
            is_readonly=False,
        )

        assert isinstance(item, StatusItem)
        assert item.name == "test_param"
        # PostgreSQL's _parse_setting_value preserves string "42"
        assert item.value == "42"
        assert item.category == StatusCategory.CONFIGURATION
        assert item.description == "Test parameter"
        assert item.unit == "ms"
        assert item.is_readonly is False

    def test_get_vendor_name(self, postgres_backend_single):
        """Test _get_vendor_name returns PostgreSQL."""
        status = postgres_backend_single.introspector.status

        vendor = status._get_vendor_name()
        assert vendor == "PostgreSQL"


class TestStatusIntrospectorCategories:
    """Tests for different status categories."""

    def test_configuration_category_items(self, postgres_backend_single):
        """Test items in CONFIGURATION category."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration(category=StatusCategory.CONFIGURATION)

        for item in items:
            assert item.category == StatusCategory.CONFIGURATION

    def test_performance_category_items(self, postgres_backend_single):
        """Test items in PERFORMANCE category."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration(category=StatusCategory.PERFORMANCE)

        for item in items:
            assert item.category == StatusCategory.PERFORMANCE

    def test_storage_category_items(self, postgres_backend_single):
        """Test items in STORAGE category."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration(category=StatusCategory.STORAGE)

        for item in items:
            assert item.category == StatusCategory.STORAGE

    def test_security_category_items(self, postgres_backend_single):
        """Test items in SECURITY category."""
        status = postgres_backend_single.introspector.status

        items = status.list_configuration(category=StatusCategory.SECURITY)

        for item in items:
            assert item.category == StatusCategory.SECURITY


class TestAsyncPostgreSQLStatusIntrospector:
    """Tests for asynchronous PostgreSQL status introspector."""

    @pytest.mark.asyncio
    async def test_get_overview(self, async_postgres_backend_single):
        """Test async get_overview returns valid ServerOverview."""
        status = async_postgres_backend_single.introspector.status

        overview = await status.get_overview()

        assert isinstance(overview, ServerOverview)
        assert overview.server_vendor == "PostgreSQL"
        assert overview.server_version is not None
        assert isinstance(overview.configuration, list)
        assert isinstance(overview.performance, list)
        assert isinstance(overview.storage, StorageInfo)
        assert isinstance(overview.databases, list)

    @pytest.mark.asyncio
    async def test_get_overview_version_matches_dialect(self, async_postgres_backend_single):
        """Test that async overview version matches dialect version."""
        status = async_postgres_backend_single.introspector.status
        overview = await status.get_overview()

        expected_version = ".".join(map(str, async_postgres_backend_single.dialect.version))
        assert overview.server_version == expected_version

    @pytest.mark.asyncio
    async def test_list_configuration(self, async_postgres_backend_single):
        """Test async list_configuration returns configuration items."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration()

        assert isinstance(items, list)
        assert len(items) > 0

        # Check that all items are StatusItem instances
        for item in items:
            assert isinstance(item, StatusItem)
            assert item.name is not None
            assert item.value is not None

    @pytest.mark.asyncio
    async def test_list_configuration_with_category_filter(self, async_postgres_backend_single):
        """Test async list_configuration with category filter."""
        status = async_postgres_backend_single.introspector.status

        config_items = await status.list_configuration(category=StatusCategory.CONFIGURATION)

        for item in config_items:
            assert item.category == StatusCategory.CONFIGURATION

    @pytest.mark.asyncio
    async def test_list_configuration_contains_expected_items(self, async_postgres_backend_single):
        """Test that async configuration contains expected PostgreSQL settings."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration()
        item_names = [item.name for item in items]

        # Check for some common PostgreSQL settings
        assert "port" in item_names
        assert "max_connections" in item_names

    @pytest.mark.asyncio
    async def test_list_performance_metrics(self, async_postgres_backend_single):
        """Test async list_performance_metrics returns performance items."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_performance_metrics()

        assert isinstance(items, list)

        # All items should be PERFORMANCE category
        for item in items:
            assert item.category == StatusCategory.PERFORMANCE

    @pytest.mark.asyncio
    async def test_get_connection_info(self, async_postgres_backend_single):
        """Test async get_connection_info returns ConnectionInfo."""
        status = async_postgres_backend_single.introspector.status

        conn_info = await status.get_connection_info()

        assert isinstance(conn_info, ConnectionInfo)
        assert conn_info.active_count is not None or conn_info.max_connections is not None

    @pytest.mark.asyncio
    async def test_get_storage_info(self, async_postgres_backend_single):
        """Test async get_storage_info returns StorageInfo."""
        status = async_postgres_backend_single.introspector.status

        storage = await status.get_storage_info()

        assert isinstance(storage, StorageInfo)

    @pytest.mark.asyncio
    async def test_list_databases(self, async_postgres_backend_single):
        """Test async list_databases returns database list."""
        status = async_postgres_backend_single.introspector.status

        databases = await status.list_databases()

        assert isinstance(databases, list)
        assert len(databases) >= 1

        for db in databases:
            assert isinstance(db, DatabaseBriefInfo)

    @pytest.mark.asyncio
    async def test_list_users(self, async_postgres_backend_single):
        """Test async list_users returns user list."""
        status = async_postgres_backend_single.introspector.status

        users = await status.list_users()

        assert isinstance(users, list)

        for user in users:
            assert isinstance(user, UserInfo)

    @pytest.mark.asyncio
    async def test_get_wal_info(self, async_postgres_backend_single):
        """Test async get_wal_info returns WALInfo."""
        status = async_postgres_backend_single.introspector.status

        wal_info = await status.get_wal_info()

        assert isinstance(wal_info, WALInfo)

    @pytest.mark.asyncio
    async def test_get_replication_info(self, async_postgres_backend_single):
        """Test async get_replication_info returns ReplicationInfo."""
        status = async_postgres_backend_single.introspector.status

        repl_info = await status.get_replication_info()

        assert isinstance(repl_info, ReplicationInfo)

    @pytest.mark.asyncio
    async def test_get_archive_info(self, async_postgres_backend_single):
        """Test async get_archive_info returns ArchiveInfo."""
        status = async_postgres_backend_single.introspector.status

        archive_info = await status.get_archive_info()

        assert isinstance(archive_info, ArchiveInfo)

    @pytest.mark.asyncio
    async def test_get_security_info(self, async_postgres_backend_single):
        """Test async get_security_info returns SecurityInfo."""
        status = async_postgres_backend_single.introspector.status

        security_info = await status.get_security_info()

        assert isinstance(security_info, SecurityInfo)

    @pytest.mark.asyncio
    async def test_list_extensions(self, async_postgres_backend_single):
        """Test async list_extensions returns extension list."""
        status = async_postgres_backend_single.introspector.status

        extensions = await status.list_extensions()

        assert isinstance(extensions, list)

        for ext in extensions:
            assert isinstance(ext, ExtensionInfo)

    @pytest.mark.asyncio
    async def test_status_item_has_description(self, async_postgres_backend_single):
        """Test that async status items have descriptions."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration()

        for item in items:
            assert item.description is not None

    @pytest.mark.asyncio
    async def test_status_item_readonly_flag(self, async_postgres_backend_single):
        """Test that async readonly items are marked correctly."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration()

        port_item = next((i for i in items if i.name == "port"), None)
        if port_item:
            assert port_item.is_readonly is True


class TestAsyncPostgreSQLStatusIntrospectorMixin:
    """Tests for async PostgreSQLStatusIntrospectorMixin helper methods."""

    @pytest.mark.asyncio
    async def test_parse_setting_value_int(self, async_postgres_backend_single):
        """Test _parse_setting_value handles integers."""
        status = async_postgres_backend_single.introspector.status

        # _parse_setting_value returns int if input is int, otherwise str
        result = status._parse_setting_value(42)
        assert result == 42
        assert isinstance(result, int)

        # String "42" is not converted to int
        result_str = status._parse_setting_value("42")
        assert result_str == "42"
        assert isinstance(result_str, str)

    @pytest.mark.asyncio
    async def test_parse_setting_value_str(self, async_postgres_backend_single):
        """Test _parse_setting_value preserves non-integer strings."""
        status = async_postgres_backend_single.introspector.status

        result = status._parse_setting_value("utf8")
        assert result == "utf8"
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_get_vendor_name(self, async_postgres_backend_single):
        """Test _get_vendor_name returns PostgreSQL."""
        status = async_postgres_backend_single.introspector.status

        vendor = status._get_vendor_name()
        assert vendor == "PostgreSQL"


class TestAsyncStatusIntrospectorCategories:
    """Tests for different async status categories."""

    @pytest.mark.asyncio
    async def test_configuration_category_items(self, async_postgres_backend_single):
        """Test async items in CONFIGURATION category."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration(category=StatusCategory.CONFIGURATION)

        for item in items:
            assert item.category == StatusCategory.CONFIGURATION

    @pytest.mark.asyncio
    async def test_performance_category_items(self, async_postgres_backend_single):
        """Test async items in PERFORMANCE category."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration(category=StatusCategory.PERFORMANCE)

        for item in items:
            assert item.category == StatusCategory.PERFORMANCE

    @pytest.mark.asyncio
    async def test_storage_category_items(self, async_postgres_backend_single):
        """Test async items in STORAGE category."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration(category=StatusCategory.STORAGE)

        for item in items:
            assert item.category == StatusCategory.STORAGE

    @pytest.mark.asyncio
    async def test_security_category_items(self, async_postgres_backend_single):
        """Test async items in SECURITY category."""
        status = async_postgres_backend_single.introspector.status

        items = await status.list_configuration(category=StatusCategory.SECURITY)

        for item in items:
            assert item.category == StatusCategory.SECURITY
