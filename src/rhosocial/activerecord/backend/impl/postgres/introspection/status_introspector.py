# src/rhosocial/activerecord/backend/impl/postgres/introspection/status_introspector.py
"""
PostgreSQL server status introspector.

Provides server status information by querying PostgreSQL's pg_settings,
pg_stat_activity, and other system views.

Design principle: Sync and Async are separate and cannot coexist.
- SyncPostgreSQLStatusIntrospector: for synchronous backends
- AsyncPostgreSQLStatusIntrospector: for asynchronous backends
"""

from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.introspection.status import (
    StatusItem,
    StatusCategory,
    ServerOverview,
    DatabaseBriefInfo,
    UserInfo,
    ConnectionInfo,
    StorageInfo,
    SessionInfo,
    WALInfo,
    ReplicationInfo,
    ArchiveInfo,
    SecurityInfo,
    ExtensionInfo,
    SyncAbstractStatusIntrospector,
    AsyncAbstractStatusIntrospector,
)
from ..expression.status import (
    PostgresSettingsQuery,
    PostgresActivityCountQuery,
    PostgresShowStatement,
    PostgresDatabaseListQuery,
    PostgresSchemaStatsQuery,
    PostgresCurrentDatabaseQuery,
    PostgresRoleListQuery,
    PostgresCurrentUserQuery,
    PostgresCurrentSchemaQuery,
    PostgresClientAddressQuery,
    PostgresSslSessionQuery,
    PostgresWalPositionQuery,
    PostgresWalFileCountQuery,
    PostgresCheckpointCountQuery,
    PostgresCheckpointLsnQuery,
    PostgresRecoveryStateQuery,
    PostgresReplicationSlotCountQuery,
    PostgresActiveReplicationCountQuery,
    PostgresReplicationLagQuery,
    PostgresArchiveStatsQuery,
    PostgresExtensionListQuery,
)


# PostgreSQL settings to include in status overview
# Format: (setting_name, category, description, unit, is_readonly)
POSTGRES_CONFIG_VARIABLES = [
    # Configuration
    ("listen_addresses", StatusCategory.CONFIGURATION, "Server listen addresses", None, False),
    ("port", StatusCategory.CONFIGURATION, "Server port", None, True),
    ("max_connections", StatusCategory.CONFIGURATION, "Maximum connections", None, True),
    ("shared_buffers", StatusCategory.CONFIGURATION, "Shared buffers size", "bytes", True),
    ("work_mem", StatusCategory.CONFIGURATION, "Work memory size", "bytes", True),
    ("maintenance_work_mem", StatusCategory.CONFIGURATION, "Maintenance work memory", "bytes", True),
    ("dynamic_shared_memory", StatusCategory.CONFIGURATION, "Dynamic shared memory", None, False),
    ("wal_level", StatusCategory.CONFIGURATION, "WAL level", None, False),
    ("wal_keep_size", StatusCategory.CONFIGURATION, "WAL keep size", "bytes", False),  # Changed from wal_keep_segments
    ("synchronous_commit", StatusCategory.PERFORMANCE, "Synchronous commit", None, False),
    ("checkpoint_timeout", StatusCategory.PERFORMANCE, "Checkpoint timeout", "seconds", False),
    ("checkpoint_completion_target", StatusCategory.PERFORMANCE, "Checkpoint completion target", "seconds", False),
    ("archive_timeout", StatusCategory.PERFORMANCE, "Archive timeout", "seconds", False),
    ("archive_command", StatusCategory.PERFORMANCE, "Archive command", None, False),
    ("log_timezone", StatusCategory.CONFIGURATION, "Log timezone", None, False),
    ("log_destination", StatusCategory.CONFIGURATION, "Log destination", None, False),
    ("logging_collector", StatusCategory.CONFIGURATION, "Logging collector", None, False),
    ("cluster_name", StatusCategory.CONFIGURATION, "Cluster name", None, False),

    # Security
    ("authentication_timeout", StatusCategory.SECURITY, "Authentication timeout", "seconds", False),
    ("password_encryption", StatusCategory.SECURITY, "Password encryption", None, True),
    ("ssl", StatusCategory.SECURITY, "SSL enabled", None, False),
    ("krb_server_keyfile", StatusCategory.SECURITY, "Kerberos keyfile", None, False),

    # Replication
    ("max_wal_senders", StatusCategory.REPLICATION, "Maximum WAL senders", None, False),
    ("max_replication_slots", StatusCategory.REPLICATION, "Maximum replication slots", None, False),
    ("hot_standby", StatusCategory.REPLICATION, "Hot standby mode", None, False),
]


# Performance metrics from pg_stat_bgwriter and other views
# Format: (variable_name, category, description, unit)
POSTGRES_STATUS_VARIABLES = [
    # Checkpoint statistics (available in PostgreSQL 10-16, removed in 17+)
    ("checkpoints_timed", StatusCategory.PERFORMANCE, "Scheduled checkpoints", "checkpoints"),
    ("checkpoints_req", StatusCategory.PERFORMANCE, "Requested checkpoints", "checkpoints"),

    # Buffer pool statistics
    ("buffers_alloc", StatusCategory.PERFORMANCE, "Buffer allocations", None),
    ("buffers_backend_fsync", StatusCategory.PERFORMANCE, "Buffer backend fsync", None),
    ("buffers_checkpoint", StatusCategory.PERFORMANCE, "Buffers written by checkpoint", None),

    # Connection statistics (from pg_stat_activity)
    ("numbackends", StatusCategory.CONNECTION, "Number of backend connections", None),
    ("sessions", StatusCategory.CONNECTION, "Number of sessions", None),
    ("active", StatusCategory.CONNECTION, "Number of active sessions", None),
    ("idle", StatusCategory.CONNECTION, "Number of idle sessions", None),
    ("waiting", StatusCategory.CONNECTION, "Number of waiting sessions", None),
    ("total", StatusCategory.CONNECTION, "Total connections", None),
    ("max_conn", StatusCategory.CONNECTION, "Maximum connections", None),
]


class PostgreSQLStatusIntrospectorMixin:
    """Mixin providing shared PostgreSQL status introspection logic.

    Both SyncPostgreSQLStatusIntrospector and AsyncPostgreSQLStatusIntrospector
    inherit from this mixin to share:
    - Query execution helpers
    - parse_* implementations
    """

    def _get_vendor_name(self) -> str:
        """Get PostgreSQL vendor name."""
        return "PostgreSQL"

    def _show(self, parameter: str):
        """The node for a ``SHOW`` of one run-time parameter."""
        return PostgresShowStatement(self._backend.dialect, parameter)
    def parse_setting_value(self, value: Any) -> Any:
        """Parse PostgreSQL setting value to appropriate Python type.

        PostgreSQL settings can be:
        - bool: 'on', 'off', 'true', 'false', 'yes', 'no'
        - int: numeric values
        - str: string values
        - bytes: bytes values with unit
        """
        if isinstance(value, bytes):
            return value.decode('utf-8')
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value in ('on', 'true', 'yes'):
                return True
            if value in ('off', 'false', 'no'):
                return False
        if isinstance(value, int):
            return value
        return str(value)

    def _get_version_string(self) -> str:
        """Get PostgreSQL version string."""
        # Try to get version from backend's _server_version_cache attribute first
        # (set by introspect_and_adapt() during backend initialization)
        version = getattr(self._backend, '_server_version_cache', None)
        if version:
            return ".".join(str(v) for v in version)

        # Fall back to _version attribute
        version = getattr(self._backend, '_version', None)
        if version:
            return ".".join(str(v) for v in version)

        # Try dialect version
        if hasattr(self._backend, '_dialect') and hasattr(self._backend._dialect, 'version'):
            version = self._backend._dialect.version
            if version:
                return ".".join(str(v) for v in version)

        # Last resort: query server directly
        try:
            version = self._backend.get_server_version()
            if version:
                return ".".join(str(v) for v in version)
        except Exception:
            pass

        return "Unknown"

    def _get_version_tuple(self) -> tuple:
        """Get PostgreSQL version as tuple for comparison.

        Returns tuple like (18, 3, 0) or (9, 6, 24).
        """
        # Try to get version from backend's _server_version_cache attribute first
        version = getattr(self._backend, '_server_version_cache', None)
        if version:
            return tuple(version)

        # Fall back to _version attribute
        version = getattr(self._backend, '_version', None)
        if version:
            return tuple(version)

        # Try dialect version
        if hasattr(self._backend, '_dialect') and hasattr(self._backend._dialect, 'version'):
            version = self._backend._dialect.version
            if version:
                return tuple(version)

        # Last resort: query server directly
        try:
            version = self._backend.get_server_version()
            if version:
                return tuple(version)
        except Exception:
            pass

        return (0, 0, 0)  # Unknown version

    def _create_status_item(
        self,
        name: str,
        value: Any,
        category: StatusCategory,
        description: Optional[str] = None,
        unit: Optional[str] = None,
        is_readonly: bool = False,
    ) -> StatusItem:
        """Create a StatusItem with parsed value."""
        return StatusItem(
            name=name,
            value=self.parse_setting_value(value),
            category=category,
            description=description,
            unit=unit,
            is_readonly=is_readonly,
        )

    def _build_server_overview(
        self,
        configuration: List[StatusItem],
        performance: List[StatusItem],
        connections: ConnectionInfo,
        storage: StorageInfo,
        databases: List[DatabaseBriefInfo],
        users: List[UserInfo],
        session: Optional[SessionInfo] = None,
        wal: Optional[WALInfo] = None,
        replication: Optional[ReplicationInfo] = None,
        archive: Optional[ArchiveInfo] = None,
        security: Optional[SecurityInfo] = None,
        extensions: Optional[List[ExtensionInfo]] = None,
    ) -> ServerOverview:
        """Build ServerOverview from collected data."""
        return ServerOverview(
            server_version=self._get_version_string(),
            server_vendor=self._get_vendor_name(),
            session=session,
            configuration=configuration,
            performance=performance,
            connections=connections,
            storage=storage,
            databases=databases,
            users=users,
            wal=wal,
            replication=replication,
            archive=archive,
            security=security,
            extensions=extensions or [],
        )


class SyncPostgreSQLStatusIntrospector(
    PostgreSQLStatusIntrospectorMixin, SyncAbstractStatusIntrospector
):
    """Synchronous PostgreSQL status introspector.

    Usage::

        backend = PostgresBackend(connection_config=config)
        backend.connect()
        status = backend.introspector.status.get_overview()
        print(status.server_version)
    """

    def __init__(self, backend: Any) -> None:
        super().__init__(backend)

    def _exec_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query synchronously."""
        return self._backend.execute(sql, params).data or []

    def get_overview(self) -> ServerOverview:
        """Get complete PostgreSQL status overview."""
        configuration = self.list_configuration()
        performance = self.list_performance_metrics()
        connections = self.get_connection_info()
        storage = self.get_storage_info()
        databases = self.list_databases()
        users = self.list_users()
        session = self.get_session_info()
        wal = self.get_wal_info()
        replication = self.get_replication_info()
        archive = self.get_archive_info()
        security = self.get_security_info()
        extensions = self.list_extensions()

        return self._build_server_overview(
            configuration=configuration,
            performance=performance,
            connections=connections,
            storage=storage,
            databases=databases,
            users=users,
            session=session,
            wal=wal,
            replication=replication,
            archive=archive,
            security=security,
            extensions=extensions,
        )

    def list_configuration(
        self, category: Optional[StatusCategory] = None
    ) -> List[StatusItem]:
        """List PostgreSQL configuration parameters from pg_settings."""
        items = []

        # Query pg_settings
        sql, params = PostgresSettingsQuery(self._backend.dialect).to_sql()
        rows = self._exec_query(sql, params)

        settings_dict = {row['name']: row for row in rows}

        for name, cat, desc, unit, is_readonly in POSTGRES_CONFIG_VARIABLES:
            if category and cat != category:
                continue

            # Get setting value from pg_settings
            row = settings_dict.get(name)
            if row:
                item = self._create_status_item(
                    name=name,
                    value=row.get('setting'),
                    category=cat,
                    description=desc,
                    unit=row.get('unit') if row.get('unit') else unit,
                    is_readonly=is_readonly,
                )
                items.append(item)

        return items

    def list_performance_metrics(
        self, category: Optional[StatusCategory] = None
    ) -> List[StatusItem]:
        """List PostgreSQL performance metrics from pg_stat_activity."""
        items = []

        # Query pg_stat_activity for performance-related settings
        sql, params = PostgresSettingsQuery(
            self._backend.dialect,
            [
                'checkpoint_timeout', 'checkpoint_completion_target',
                'archive_timeout', 'archive_command',
            ],
        ).to_sql()
        rows = self._exec_query(sql, params)

        for row in rows:
            name = row['name']
            for var_name, var_cat, desc, unit, is_readonly in POSTGRES_CONFIG_VARIABLES:
                if name == var_name:
                    item = self._create_status_item(
                        name=name,
                        value=row.get('setting'),
                        category=var_cat,
                        description=desc,
                        unit=row.get('unit') if row.get('unit') else unit,
                        is_readonly=is_readonly,
                    )
                    items.append(item)
                    break

        return items

    def get_connection_info(self) -> ConnectionInfo:
        """Get connection information from pg_stat_activity."""
        sql, params = PostgresActivityCountQuery(self._backend.dialect).to_sql()
        rows = self._exec_query(sql, params)
        total = rows[0].get('total_connections', 0) if rows else 0

        max_conn = None
        try:
            sql, params = PostgresShowStatement(
                self._backend.dialect, 'max_connections'
            ).to_sql()
            result = self._exec_query(sql, params)
            if result:
                max_conn = int(list(result[0].values())[0])
        except Exception:
            pass

        return ConnectionInfo(
            active_count=total,
            max_connections=max_conn,
        )

    def get_storage_info(self) -> StorageInfo:
        """Get storage information."""
        db_name = getattr(self._backend.config, 'database', 'postgres')

        # Get database size: the node carries the database name and the
        # dialect renders the statement, so no SQL text is assembled here.
        from ..expression.introspection import PostgresDatabaseSizeQueryStatement

        sql, params = PostgresDatabaseSizeQueryStatement(
            self.dialect, database_name=db_name
        ).to_sql()
        rows = self._exec_query(sql, params)
        size_bytes = rows[0].get('size_bytes') if rows else None

        return StorageInfo(
            total_size_bytes=size_bytes,
        )

    def list_databases(self) -> List[DatabaseBriefInfo]:
        """List databases from pg_database with schema info for current database.

        For the current database, includes schema information with table/view counts
        in the extra field.
        """
        # Get list of databases
        sql, params = PostgresDatabaseListQuery(self._backend.dialect).to_sql()
        rows = self._exec_query(sql, params)

        # Get current database name
        current_db = None
        try:
            result = self._exec_query(
    *PostgresCurrentDatabaseQuery(self._backend.dialect).to_sql()
)
            if result:
                current_db = result[0].get('current_database')
        except Exception:
            pass

        # Get schema info for current database
        schema_info: Dict[str, Dict[str, int]] = {}
        if current_db:
            try:
                # Query schemata with table/view counts
                sql, params = PostgresSchemaStatsQuery(self._backend.dialect).to_sql()
                schema_rows = self._exec_query(sql, params)
                for row in schema_rows:
                    schema_name = row.get('schema_name')
                    if schema_name:
                        schema_info[schema_name] = {
                            'tables': row.get('table_count', 0) or 0,
                            'views': row.get('view_count', 0) or 0,
                        }
            except Exception:
                pass

        databases = []
        for row in rows:
            db_name = row['name']
            db_info = DatabaseBriefInfo(
                name=db_name,
                size_bytes=row.get('size_bytes'),
            )

            # Add schema info for current database
            if db_name == current_db and schema_info:
                db_info.extra['schemas'] = schema_info
                # Also set total table/view counts
                db_info.table_count = sum(s['tables'] for s in schema_info.values())
                db_info.view_count = sum(s['views'] for s in schema_info.values())

            databases.append(db_info)

        return databases

    def list_users(self) -> List[UserInfo]:
        """List users from pg_roles."""
        sql, params = PostgresRoleListQuery(self._backend.dialect).to_sql()
        rows = self._exec_query(sql, params)

        return [
            UserInfo(
                name=row['name'],
                is_superuser=row['is_superuser'],
                extra={'owner': row.get('owner')} if row.get('owner') else {},
            )
            for row in rows
        ]

    def get_session_info(self) -> SessionInfo:
        """Get current session/connection information."""
        session = SessionInfo()

        # Get current user. ``CURRENT_USER`` is niladic: the standard writes it
        # without parentheses and PostgreSQL refuses the call form.
        try:
            rows = self._exec_query(*PostgresCurrentUserQuery(self._backend.dialect).to_sql())
            if rows:
                session.user = rows[0].get('current_user')
        except Exception:
            pass

        # Get current database
        try:
            rows = self._exec_query(*PostgresCurrentDatabaseQuery(self._backend.dialect).to_sql())
            if rows:
                session.database = rows[0].get('current_database')
        except Exception:
            pass

        # Get current schema
        try:
            rows = self._exec_query(*PostgresCurrentSchemaQuery(self._backend.dialect).to_sql())
            if rows:
                session.schema = rows[0].get('current_schema')
        except Exception:
            pass

        # Get client host address
        try:
            rows = self._exec_query(*PostgresClientAddressQuery(self._backend.dialect).to_sql())
            if rows:
                host = rows[0].get('inet_client_addr')
                if host:
                    session.host = str(host)
        except Exception:
            pass

        # Get SSL status from pg_stat_ssl
        try:
            rows = self._exec_query(*PostgresSslSessionQuery(self._backend.dialect).to_sql())
            if rows and rows[0]:
                ssl_info = rows[0]
                ssl_value = ssl_info.get('ssl')
                # PostgreSQL returns boolean or 't'/'f'
                session.ssl_enabled = ssl_value is True or ssl_value == 't' or ssl_value == 'true'
                session.ssl_version = ssl_info.get('version')
                session.ssl_cipher = ssl_info.get('cipher')
        except Exception:
            pass

        # Check if password was used for authentication
        session.password_used = bool(getattr(self._backend.config, 'password', None))

        return session

    def get_wal_info(self) -> WALInfo:
        """Get WAL (Write-Ahead Logging) information."""
        wal_info = WALInfo()
        version = self._get_version_tuple()

        # Get WAL level
        try:
            rows = self._exec_query(*self._show('wal_level').to_sql())
            if rows:
                wal_info.wal_level = list(rows[0].values())[0]
        except Exception:
            pass

        # Get WAL position/size. The node names the question; the formatter
        # spells the function pair this server version has (wal/xlog).
        try:
            rows = self._exec_query(*PostgresWalPositionQuery(self._backend.dialect).to_sql())
            if rows and rows[0].get('wal_bytes'):
                wal_info.wal_size_bytes = int(rows[0]['wal_bytes'])
        except Exception:
            pass

        # Get WAL files count
        # pg_ls_waldir() only exists in PostgreSQL 10+; the node refuses below.
        if version >= (10, 0, 0):
            try:
                rows = self._exec_query(*PostgresWalFileCountQuery(self._backend.dialect).to_sql())
                if rows:
                    wal_info.wal_files = int(rows[0].get('wal_files', 0))
            except Exception:
                pass

        # Get checkpoint statistics
        # PostgreSQL 10-16: pg_stat_bgwriter has checkpoints_timed and checkpoints_req
        # PostgreSQL 17+: these columns moved to pg_stat_checkpointer
        try:
            rows = self._exec_query(*PostgresCheckpointCountQuery(self._backend.dialect).to_sql())
            if rows:
                wal_info.checkpoint_count = int(rows[0].get('checkpoint_count', 0))
        except Exception:
            pass

        # Get last checkpoint info
        # pg_control_checkpoint() exists in PostgreSQL 9.6+
        # Returns a record with checkpoint LSN as the first column
        if version >= (10, 0, 0):
            try:
                rows = self._exec_query(*PostgresCheckpointLsnQuery(self._backend.dialect).to_sql())
                if rows and rows[0].get('checkpoint_info'):
                    wal_info.checkpoint_time = rows[0]['checkpoint_info']
            except Exception:
                # Some PostgreSQL versions may not support this syntax
                pass

        # Get WAL keep size/segments
        # PostgreSQL 13+: wal_keep_size
        # PostgreSQL 12 and below: wal_keep_segments
        parameter = 'wal_keep_size' if version >= (13, 0, 0) else 'wal_keep_segments'
        try:
            rows = self._exec_query(*self._show(parameter).to_sql())
            if rows:
                wal_info.wal_segments = list(rows[0].values())[0]
        except Exception:
            pass

        return wal_info

    def get_replication_info(self) -> ReplicationInfo:
        """Get replication status information."""
        replication_info = ReplicationInfo()

        # Check if primary or standby
        try:
            rows = self._exec_query(*PostgresRecoveryStateQuery(self._backend.dialect).to_sql())
            if rows:
                is_in_recovery = list(rows[0].values())[0]
                # pg_is_in_recovery returns False for primary, True for standby
                replication_info.is_primary = not (
                    is_in_recovery is True or is_in_recovery == 't' or is_in_recovery == 'true'
                )
                replication_info.is_standby = (
                    is_in_recovery is True or is_in_recovery == 't' or is_in_recovery == 'true'
                )
        except Exception:
            pass

        # Get replication slots count
        try:
            rows = self._exec_query(
                *PostgresReplicationSlotCountQuery(self._backend.dialect).to_sql()
            )
            if rows:
                replication_info.replication_slots = int(rows[0].get('slot_count', 0))
        except Exception:
            pass

        # Get active replications count
        try:
            rows = self._exec_query(
                *PostgresActiveReplicationCountQuery(self._backend.dialect).to_sql()
            )
            if rows:
                replication_info.active_replications = int(rows[0].get('active_count', 0))
                replication_info.streaming = replication_info.active_replications > 0
        except Exception:
            pass

        # Get replication lag (for standby)
        if replication_info.is_standby:
            try:
                rows = self._exec_query(
                    *PostgresReplicationLagQuery(self._backend.dialect).to_sql()
                )
                if rows and rows[0].get('lag_bytes') is not None:
                    replication_info.lag_bytes = int(rows[0]['lag_bytes'])
            except Exception:
                pass

        return replication_info

    def get_archive_info(self) -> ArchiveInfo:
        """Get archive status information."""
        archive_info = ArchiveInfo()

        # Get archive mode
        try:
            rows = self._exec_query(*self._show('archive_mode').to_sql())
            if rows:
                archive_info.archive_mode = list(rows[0].values())[0]
        except Exception:
            pass

        # Get archive command
        try:
            rows = self._exec_query(*self._show('archive_command').to_sql())
            if rows:
                archive_info.archive_command = list(rows[0].values())[0]
        except Exception:
            pass

        # Get archive timeout
        try:
            rows = self._exec_query(*self._show('archive_timeout').to_sql())
            if rows:
                timeout_str = list(rows[0].values())[0]
                # Parse timeout (could be like '5min' or '300s')
                if timeout_str and timeout_str != '0':
                    # Try to parse as integer (seconds)
                    try:
                        archive_info.archive_timeout = int(timeout_str)
                    except ValueError:
                        pass
        except Exception:
            pass

        # Get archive statistics
        try:
            rows = self._exec_query(*PostgresArchiveStatsQuery(self._backend.dialect).to_sql())
            if rows:
                row = rows[0]
                archive_info.archived_count = int(row.get('archived_count', 0) or 0)
                archive_info.failed_count = int(row.get('failed_count', 0) or 0)
                archive_info.last_archived = row.get('last_archived')
                archive_info.last_failed = row.get('last_failed')
        except Exception:
            pass

        return archive_info

    def get_security_info(self) -> SecurityInfo:
        """Get security-related status information."""
        security_info = SecurityInfo()

        # Get SSL status
        try:
            rows = self._exec_query(*self._show('ssl').to_sql())
            if rows:
                ssl_value = list(rows[0].values())[0]
                security_info.ssl_enabled = ssl_value == 'on' or ssl_value is True
        except Exception:
            pass

        # Get SSL certificate file
        try:
            rows = self._exec_query(*self._show('ssl_cert_file').to_sql())
            if rows:
                security_info.ssl_cert_file = list(rows[0].values())[0]
        except Exception:
            pass

        # Get SSL key file
        try:
            rows = self._exec_query(*self._show('ssl_key_file').to_sql())
            if rows:
                security_info.ssl_key_file = list(rows[0].values())[0]
        except Exception:
            pass

        # Get SSL CA file
        try:
            rows = self._exec_query(*self._show('ssl_ca_file').to_sql())
            if rows:
                security_info.ssl_ca_file = list(rows[0].values())[0]
        except Exception:
            pass

        # Get password encryption
        try:
            rows = self._exec_query(*self._show('password_encryption').to_sql())
            if rows:
                security_info.password_encryption = list(rows[0].values())[0]
        except Exception:
            pass

        # Check row-level security
        try:
            rows = self._exec_query(*self._show('row_security').to_sql())
            if rows:
                rls_value = list(rows[0].values())[0]
                security_info.row_security = rls_value == 'on' or rls_value is True
        except Exception:
            pass

        return security_info

    def list_extensions(self) -> List[ExtensionInfo]:
        """List installed extensions."""
        extensions = []

        try:
            rows = self._exec_query(*PostgresExtensionListQuery(self._backend.dialect).to_sql())

            for row in rows:
                ext = ExtensionInfo(
                    name=row['name'],
                    version=row.get('version'),
                    schema=row.get('schema'),
                    description=row.get('description'),
                )
                extensions.append(ext)
        except Exception:
            pass

        return extensions


class AsyncPostgreSQLStatusIntrospector(
    PostgreSQLStatusIntrospectorMixin, AsyncAbstractStatusIntrospector
):
    """Asynchronous PostgreSQL status introspector.

    Usage::

        backend = AsyncPostgresBackend(connection_config=config)
        await backend.connect()
        status = await backend.introspector.status.get_overview()
        print(status.server_version)
    """

    def __init__(self, backend: Any) -> None:
        super().__init__(backend)

    async def _exec_query_async(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query asynchronously."""
        result = await self._backend.execute(sql, params)
        return result.data if result else []

    async def get_overview(self) -> ServerOverview:
        """Get complete PostgreSQL status overview."""
        configuration = await self.list_configuration()
        performance = await self.list_performance_metrics()
        connections = await self.get_connection_info()
        storage = await self.get_storage_info()
        databases = await self.list_databases()
        users = await self.list_users()
        session = await self.get_session_info()
        wal = await self.get_wal_info()
        replication = await self.get_replication_info()
        archive = await self.get_archive_info()
        security = await self.get_security_info()
        extensions = await self.list_extensions()

        return self._build_server_overview(
            configuration=configuration,
            performance=performance,
            connections=connections,
            storage=storage,
            databases=databases,
            users=users,
            session=session,
            wal=wal,
            replication=replication,
            archive=archive,
            security=security,
            extensions=extensions,
        )

    async def list_configuration(
        self, category: Optional[StatusCategory] = None
    ) -> List[StatusItem]:
        """List PostgreSQL configuration parameters from pg_settings."""
        items = []

        # Query pg_settings
        sql, params = PostgresSettingsQuery(self._backend.dialect).to_sql()
        rows = await self._exec_query_async(sql, params)

        settings_dict = {row['name']: row for row in rows}

        for name, cat, desc, unit, is_readonly in POSTGRES_CONFIG_VARIABLES:
            if category and cat != category:
                continue

            # Get setting value from pg_settings
            row = settings_dict.get(name)
            if row:
                item = self._create_status_item(
                    name=name,
                    value=row.get('setting'),
                    category=cat,
                    description=desc,
                    unit=row.get('unit') if row.get('unit') else unit,
                    is_readonly=is_readonly,
                )
                items.append(item)

        return items

    async def list_performance_metrics(
        self, category: Optional[StatusCategory] = None
    ) -> List[StatusItem]:
        """List PostgreSQL performance metrics from pg_stat_activity."""
        items = []

        # Query pg_stat_activity for performance-related settings
        sql, params = PostgresSettingsQuery(
            self._backend.dialect,
            [
                'checkpoint_timeout', 'checkpoint_completion_target',
                'archive_timeout', 'archive_command',
            ],
        ).to_sql()
        rows = await self._exec_query_async(sql, params)

        for row in rows:
            name = row['name']
            for var_name, var_cat, desc, unit, is_readonly in POSTGRES_CONFIG_VARIABLES:
                if name == var_name:
                    item = self._create_status_item(
                        name=name,
                        value=row.get('setting'),
                        category=var_cat,
                        description=desc,
                        unit=row.get('unit') if row.get('unit') else unit,
                        is_readonly=is_readonly,
                    )
                    items.append(item)
                    break

        return items

    async def get_connection_info(self) -> ConnectionInfo:
        """Get connection information from pg_stat_activity."""
        sql, params = PostgresActivityCountQuery(self._backend.dialect).to_sql()
        rows = await self._exec_query_async(sql, params)
        total = rows[0].get('total_connections', 1) if rows else 0

        max_conn = None
        try:
            sql, params = PostgresShowStatement(
                self._backend.dialect, 'max_connections'
            ).to_sql()
            result = await self._exec_query_async(sql, params)
            if result:
                max_conn = int(list(result[0].values())[0])
        except Exception:
            pass

        return ConnectionInfo(
            active_count=total,
            max_connections=max_conn,
        )

    async def get_storage_info(self) -> StorageInfo:
        """Get storage information."""
        db_name = getattr(self._backend.config, 'database', 'postgres')

        # Get database size: the node carries the database name and the
        # dialect renders the statement, so no SQL text is assembled here.
        from ..expression.introspection import PostgresDatabaseSizeQueryStatement

        sql, params = PostgresDatabaseSizeQueryStatement(
            self.dialect, database_name=db_name
        ).to_sql()
        rows = await self._exec_query_async(sql, params)
        size_bytes = rows[0].get('size_bytes') if rows else None

        return StorageInfo(
            total_size_bytes=size_bytes,
        )

    async def list_databases(self) -> List[DatabaseBriefInfo]:
        """List databases from pg_database with schema info for current database.

        For the current database, includes schema information with table/view counts
        in the extra field.
        """
        # Get list of databases
        sql, params = PostgresDatabaseListQuery(self._backend.dialect).to_sql()
        rows = await self._exec_query_async(sql, params)

        # Get current database name
        current_db = None
        try:
            result = await self._exec_query_async(
                *PostgresCurrentDatabaseQuery(self._backend.dialect).to_sql()
            )
            if result:
                current_db = result[0].get('current_database')
        except Exception:
            pass

        # Get schema info for current database
        schema_info: Dict[str, Dict[str, int]] = {}
        if current_db:
            try:
                # Query schemata with table/view counts
                sql, params = PostgresSchemaStatsQuery(self._backend.dialect).to_sql()
                schema_rows = await self._exec_query_async(sql, params)
                for row in schema_rows:
                    schema_name = row.get('schema_name')
                    if schema_name:
                        schema_info[schema_name] = {
                            'tables': row.get('table_count', 0) or 0,
                            'views': row.get('view_count', 0) or 0,
                        }
            except Exception:
                pass

        databases = []
        for row in rows:
            db_name = row['name']
            db_info = DatabaseBriefInfo(
                name=db_name,
                size_bytes=row.get('size_bytes'),
            )

            # Add schema info for current database
            if db_name == current_db and schema_info:
                db_info.extra['schemas'] = schema_info
                # Also set total table/view counts
                db_info.table_count = sum(s['tables'] for s in schema_info.values())
                db_info.view_count = sum(s['views'] for s in schema_info.values())

            databases.append(db_info)

        return databases

    async def list_users(self) -> List[UserInfo]:
        """List users from pg_roles."""
        sql, params = PostgresRoleListQuery(self._backend.dialect).to_sql()
        rows = await self._exec_query_async(sql, params)

        return [
            UserInfo(
                name=row['name'],
                is_superuser=row['is_superuser'],
                extra={'owner': row.get('owner')} if row.get('owner') else {},
            )
            for row in rows
        ]

    async def get_session_info(self) -> SessionInfo:
        """Get current session/connection information."""
        session = SessionInfo()

        # Get current user. ``CURRENT_USER`` is niladic: the standard writes it
        # without parentheses and PostgreSQL refuses the call form.
        try:
            rows = await self._exec_query_async(
                *PostgresCurrentUserQuery(self._backend.dialect).to_sql()
            )
            if rows:
                session.user = rows[0].get('current_user')
        except Exception:
            pass

        # Get current database
        try:
            rows = await self._exec_query_async(
                *PostgresCurrentDatabaseQuery(self._backend.dialect).to_sql()
            )
            if rows:
                session.database = rows[0].get('current_database')
        except Exception:
            pass

        # Get current schema
        try:
            rows = await self._exec_query_async(
                *PostgresCurrentSchemaQuery(self._backend.dialect).to_sql()
            )
            if rows:
                session.schema = rows[0].get('current_schema')
        except Exception:
            pass

        # Get client host address
        try:
            rows = await self._exec_query_async(
                *PostgresClientAddressQuery(self._backend.dialect).to_sql()
            )
            if rows:
                host = rows[0].get('inet_client_addr')
                if host:
                    session.host = str(host)
        except Exception:
            pass

        # Get SSL status from the session's own pg_stat_ssl row. The sync path
        # reads the same shape; there is no ssl_is_used() function (measured
        # 17.11: function does not exist), which is why both paths read the view.
        try:
            rows = await self._exec_query_async(
                *PostgresSslSessionQuery(self._backend.dialect).to_sql()
            )
            if rows and rows[0]:
                ssl_info = rows[0]
                ssl_value = ssl_info.get('ssl')
                session.ssl_enabled = ssl_value is True or ssl_value == 't' or ssl_value == 'true'
                session.ssl_version = ssl_info.get('version')
                session.ssl_cipher = ssl_info.get('cipher')
        except Exception:
            pass

        # Check if password was used for authentication
        session.password_used = bool(getattr(self._backend.config, 'password', None))

        return session

    async def get_wal_info(self) -> WALInfo:
        """Get WAL (Write-Ahead Logging) information."""
        wal_info = WALInfo()
        version = self._get_version_tuple()

        # Get WAL level
        try:
            rows = await self._exec_query_async(*self._show('wal_level').to_sql())
            if rows:
                wal_info.wal_level = list(rows[0].values())[0]
        except Exception:
            pass

        # Get WAL position/size. The node names the question; the formatter
        # spells the function pair this server version has (wal/xlog).
        try:
            rows = await self._exec_query_async(
                *PostgresWalPositionQuery(self._backend.dialect).to_sql()
            )
            if rows and rows[0].get('wal_bytes'):
                wal_info.wal_size_bytes = int(rows[0]['wal_bytes'])
        except Exception:
            pass

        # Get WAL files count
        # pg_ls_waldir() only exists in PostgreSQL 10+; the node refuses below.
        if version >= (10, 0, 0):
            try:
                rows = await self._exec_query_async(
                    *PostgresWalFileCountQuery(self._backend.dialect).to_sql()
                )
                if rows:
                    wal_info.wal_files = int(rows[0].get('wal_files', 0))
            except Exception:
                pass

        # Get checkpoint statistics
        # PostgreSQL 10-16: pg_stat_bgwriter has checkpoints_timed and checkpoints_req
        # PostgreSQL 17+: these columns moved to pg_stat_checkpointer
        try:
            rows = await self._exec_query_async(
                *PostgresCheckpointCountQuery(self._backend.dialect).to_sql()
            )
            if rows:
                wal_info.checkpoint_count = int(rows[0].get('checkpoint_count', 0))
        except Exception:
            pass

        # Get last checkpoint info
        # pg_control_checkpoint() exists in PostgreSQL 9.6+
        if version >= (10, 0, 0):
            try:
                rows = await self._exec_query_async(
                    *PostgresCheckpointLsnQuery(self._backend.dialect).to_sql()
                )
                if rows and rows[0].get('checkpoint_info'):
                    wal_info.checkpoint_time = rows[0]['checkpoint_info']
            except Exception:
                # Some PostgreSQL versions may not support this syntax
                pass

        # Get WAL keep size/segments
        # PostgreSQL 13+: wal_keep_size
        # PostgreSQL 12 and below: wal_keep_segments
        parameter = 'wal_keep_size' if version >= (13, 0, 0) else 'wal_keep_segments'
        try:
            rows = await self._exec_query_async(*self._show(parameter).to_sql())
            if rows:
                wal_info.wal_segments = list(rows[0].values())[0]
        except Exception:
            pass

        return wal_info

    async def get_replication_info(self) -> ReplicationInfo:
        """Get replication status information."""
        replication_info = ReplicationInfo()

        # Check if primary or standby
        try:
            rows = await self._exec_query_async(
                *PostgresRecoveryStateQuery(self._backend.dialect).to_sql()
            )
            if rows:
                is_in_recovery = list(rows[0].values())[0]
                # pg_is_in_recovery returns False for primary, True for standby
                replication_info.is_primary = not (
                    is_in_recovery is True or is_in_recovery == 't' or is_in_recovery == 'true'
                )
                replication_info.is_standby = (
                    is_in_recovery is True or is_in_recovery == 't' or is_in_recovery == 'true'
                )
        except Exception:
            pass

        # Get replication slots count
        try:
            rows = await self._exec_query_async(
                *PostgresReplicationSlotCountQuery(self._backend.dialect).to_sql()
            )
            if rows:
                replication_info.replication_slots = int(rows[0].get('slot_count', 0))
        except Exception:
            pass

        # Get active replications count
        try:
            rows = await self._exec_query_async(
                *PostgresActiveReplicationCountQuery(self._backend.dialect).to_sql()
            )
            if rows:
                replication_info.active_replications = int(rows[0].get('active_count', 0))
                replication_info.streaming = replication_info.active_replications > 0
        except Exception:
            pass

        # Get replication lag (for standby)
        if replication_info.is_standby:
            try:
                rows = await self._exec_query_async(
                    *PostgresReplicationLagQuery(self._backend.dialect).to_sql()
                )
                if rows and rows[0].get('lag_bytes') is not None:
                    replication_info.lag_bytes = int(rows[0]['lag_bytes'])
            except Exception:
                pass

        return replication_info

    async def get_archive_info(self) -> ArchiveInfo:
        """Get archive status information."""
        archive_info = ArchiveInfo()

        # Get archive mode
        try:
            rows = await self._exec_query_async(*self._show('archive_mode').to_sql())
            if rows:
                archive_info.archive_mode = list(rows[0].values())[0]
        except Exception:
            pass

        # Get archive command
        try:
            rows = await self._exec_query_async(*self._show('archive_command').to_sql())
            if rows:
                archive_info.archive_command = list(rows[0].values())[0]
        except Exception:
            pass

        # Get archive timeout
        try:
            rows = await self._exec_query_async(*self._show('archive_timeout').to_sql())
            if rows:
                timeout_str = list(rows[0].values())[0]
                # Parse timeout (could be like '5min' or '300s')
                if timeout_str and timeout_str != '0':
                    # Try to parse as integer (seconds)
                    try:
                        archive_info.archive_timeout = int(timeout_str)
                    except ValueError:
                        pass
        except Exception:
            pass

        # Get archive statistics
        try:
            rows = await self._exec_query_async(
                *PostgresArchiveStatsQuery(self._backend.dialect).to_sql()
            )
            if rows:
                row = rows[0]
                archive_info.archived_count = int(row.get('archived_count', 0) or 0)
                archive_info.failed_count = int(row.get('failed_count', 0) or 0)
                archive_info.last_archived = row.get('last_archived')
                archive_info.last_failed = row.get('last_failed')
        except Exception:
            pass

        return archive_info

    async def get_security_info(self) -> SecurityInfo:
        """Get security-related status information."""
        security_info = SecurityInfo()

        # Get SSL status
        try:
            rows = await self._exec_query_async(*self._show('ssl').to_sql())
            if rows:
                ssl_value = list(rows[0].values())[0]
                security_info.ssl_enabled = ssl_value == 'on' or ssl_value is True
        except Exception:
            pass

        # Get SSL certificate file
        try:
            rows = await self._exec_query_async(*self._show('ssl_cert_file').to_sql())
            if rows:
                security_info.ssl_cert_file = list(rows[0].values())[0]
        except Exception:
            pass

        # Get SSL key file
        try:
            rows = await self._exec_query_async(*self._show('ssl_key_file').to_sql())
            if rows:
                security_info.ssl_key_file = list(rows[0].values())[0]
        except Exception:
            pass

        # Get SSL CA file
        try:
            rows = await self._exec_query_async(*self._show('ssl_ca_file').to_sql())
            if rows:
                security_info.ssl_ca_file = list(rows[0].values())[0]
        except Exception:
            pass

        # Get password encryption
        try:
            rows = await self._exec_query_async(*self._show('password_encryption').to_sql())
            if rows:
                security_info.password_encryption = list(rows[0].values())[0]
        except Exception:
            pass

        # Check row-level security
        try:
            rows = await self._exec_query_async(*self._show('row_security').to_sql())
            if rows:
                rls_value = list(rows[0].values())[0]
                security_info.row_security = rls_value == 'on' or rls_value is True
        except Exception:
            pass

        return security_info

    async def list_extensions(self) -> List[ExtensionInfo]:
        """List installed extensions."""
        extensions = []

        try:
            rows = await self._exec_query_async(
                *PostgresExtensionListQuery(self._backend.dialect).to_sql()
            )

            for row in rows:
                ext = ExtensionInfo(
                    name=row['name'],
                    version=row.get('version'),
                    schema=row.get('schema'),
                    description=row.get('description'),
                )
                extensions.append(ext)
        except Exception:
            pass

        return extensions
