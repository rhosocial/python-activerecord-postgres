# tests/.../feature/backend/execute/introspection/test_status_introspector_async_offline.py
"""Offline unit tests for ``AsyncPostgreSQLStatusIntrospector``.

PostgreSQL has a native async driver, so the async introspector must be covered
without a live server.  Every I/O seam is an ``AsyncMock``: the fake backend's
``execute`` coroutine dispatches on a stable SQL token and returns a controlled
``result.data`` payload.  A real (pure-Python, version-adapted) ``PostgresDialect``
is used only so the query expression nodes render real SQL -- no connection,
socket or network is involved.

Only ``AsyncPostgreSQLStatusIntrospector`` is exercised here; the synchronous
introspector is covered by ``test_status_introspector.py``.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.introspection.status_introspector import (
    AsyncPostgreSQLStatusIntrospector,
    POSTGRES_CONFIG_VARIABLES,
)
from rhosocial.activerecord.backend.impl.postgres.introspection.status_types import (
    ArchiveInfo,
    PostgreSQLServerOverview,
    ReplicationInfo,
    SecurityInfo,
    WALInfo,
)
from rhosocial.activerecord.backend.introspection.status import (
    ConnectionInfo,
    DatabaseBriefInfo,
    ExtensionInfo,
    SessionInfo,
    StatusCategory,
    StatusItem,
    StorageInfo,
    UserInfo,
)


# ---------------------------------------------------------------------------
# SQL dispatch tokens.  Each is a stable substring of the SQL rendered by the
# (version-adapted) PostgresDialect.  SHOW statements are matched exactly because
# "SHOW ssl" is a prefix of "SHOW ssl_cert_file" / "SHOW ssl_key_file" / ... .
# ---------------------------------------------------------------------------
class SQL:
    SETTINGS_ALL = "pg_settings ORDER BY name"
    SETTINGS_NAMED = "pg_settings WHERE name IN"
    ACTIVITY = "total_connections"
    SHOW_MAX_CONN = "SHOW max_connections"
    DB_SIZE = "PG_DATABASE_SIZE(%s)"
    DB_LIST = "FROM pg_database"
    CURRENT_DB = "SELECT CURRENT_DATABASE()"
    SCHEMA_STATS = "FROM pg_namespace"
    ROLE_LIST = "FROM pg_roles"
    CURRENT_USER = "SELECT CURRENT_USER"
    CURRENT_SCHEMA = "SELECT CURRENT_SCHEMA()"
    CLIENT_ADDR = "INET_CLIENT_ADDR()"
    SSL_SESSION = "FROM pg_stat_ssl"
    SHOW_WAL_LEVEL = "SHOW wal_level"
    WAL_POSITION = 'AS "wal_bytes"'
    WAL_FILES = "pg_ls_waldir"
    CHECKPOINT_COUNT = "checkpoint_count"
    CHECKPOINT_LSN = "checkpoint_info"
    SHOW_WAL_KEEP_SIZE = "SHOW wal_keep_size"
    SHOW_WAL_KEEP_SEGMENTS = "SHOW wal_keep_segments"
    RECOVERY = "PG_IS_IN_RECOVERY()"
    REPL_SLOTS = "pg_replication_slots"
    ACTIVE_REPL = "pg_stat_replication"
    REPL_LAG = 'AS "lag_bytes"'
    SHOW_ARCHIVE_MODE = "SHOW archive_mode"
    SHOW_ARCHIVE_COMMAND = "SHOW archive_command"
    SHOW_ARCHIVE_TIMEOUT = "SHOW archive_timeout"
    ARCHIVE_STATS = "FROM pg_stat_archiver"
    SHOW_SSL = "SHOW ssl"
    SHOW_SSL_CERT = "SHOW ssl_cert_file"
    SHOW_SSL_KEY = "SHOW ssl_key_file"
    SHOW_SSL_CA = "SHOW ssl_ca_file"
    SHOW_PASSWORD_ENCRYPTION = "SHOW password_encryption"
    SHOW_ROW_SECURITY = "SHOW row_security"
    EXT_LIST = "FROM pg_extension"


class FakeBackend:
    """Minimal backend double: a real dialect plus an async execute seam."""

    def __init__(self, responses=None, version=(16, 0, 0), database="testdb", password=None):
        self.dialect = PostgresDialect()
        self.dialect.version = tuple(version)
        self.config = SimpleNamespace(database=database, password=password)
        self._server_version_cache = tuple(version)
        self.get_server_version = Mock(return_value=tuple(version))
        self.raise_all = False
        self.null_result = False
        self.execute = AsyncMock(side_effect=self._dispatch)
        self._responses = dict(responses or {})

    def _dispatch(self, sql, params=None):
        if self.null_result:
            return None
        if self.raise_all:
            raise RuntimeError("simulated backend failure")
        for token, payload in self._responses.items():
            matched = (sql == token) if token.startswith("SHOW ") else (token in sql)
            if matched:
                if isinstance(payload, BaseException):
                    raise payload
                return SimpleNamespace(data=payload)
        return SimpleNamespace(data=[])


def make_introspector(responses=None, **backend_kwargs):
    """Build an async introspector wired to a fully mocked backend."""
    backend = FakeBackend(responses, **backend_kwargs)
    return AsyncPostgreSQLStatusIntrospector(backend), backend


def setting(name, value, unit=None):
    return {"name": name, "setting": value, "unit": unit}


def show(value):
    """A SHOW statement returns one row with a single (anonymously named) column."""
    return [{"value": value}]


# ---------------------------------------------------------------------------
# get_overview
# ---------------------------------------------------------------------------
class TestAsyncOverview:
    async def test_get_overview_happy_path_populates_all_families(self):
        responses = {
            SQL.SETTINGS_ALL: [setting("port", "5432")],
            SQL.SETTINGS_NAMED: [setting("checkpoint_timeout", "300", "s")],
            SQL.ACTIVITY: [{"total_connections": 12}],
            SQL.SHOW_MAX_CONN: show("100"),
            SQL.DB_SIZE: [{"size_bytes": 4096}],
            SQL.DB_LIST: [{"name": "testdb", "size_bytes": 4096}],
            SQL.CURRENT_DB: [{"current_database": "testdb"}],
            SQL.SCHEMA_STATS: [{"schema_name": "public", "table_count": 3, "view_count": 1}],
            SQL.ROLE_LIST: [{"name": "alice", "is_superuser": True, "owner": "postgres"}],
            SQL.CURRENT_USER: [{"current_user": "alice"}],
            SQL.CURRENT_SCHEMA: [{"current_schema": "public"}],
            SQL.CLIENT_ADDR: [{"inet_client_addr": "10.0.0.5"}],
            SQL.SSL_SESSION: [{"ssl": "t", "version": "TLSv1.3", "cipher": "AES"}],
            SQL.SHOW_WAL_LEVEL: show("replica"),
            SQL.WAL_POSITION: [{"wal_bytes": 1024}],
            SQL.WAL_FILES: [{"wal_files": 7}],
            SQL.CHECKPOINT_COUNT: [{"checkpoint_count": 9}],
            SQL.CHECKPOINT_LSN: [{"checkpoint_info": "0/16B7F20"}],
            SQL.SHOW_WAL_KEEP_SIZE: show("128MB"),
            SQL.RECOVERY: show("f"),
            SQL.REPL_SLOTS: [{"slot_count": 2}],
            SQL.ACTIVE_REPL: [{"active_count": 1}],
            SQL.REPL_LAG: [{"lag_bytes": 512}],
            SQL.SHOW_ARCHIVE_MODE: show("on"),
            SQL.SHOW_ARCHIVE_COMMAND: show("wal-archive"),
            SQL.SHOW_ARCHIVE_TIMEOUT: show("300"),
            SQL.ARCHIVE_STATS: [{"archived_count": 5, "failed_count": 0}],
            SQL.SHOW_SSL: show("on"),
            SQL.SHOW_SSL_CERT: show("server.crt"),
            SQL.SHOW_SSL_KEY: show("server.key"),
            SQL.SHOW_SSL_CA: show("ca.crt"),
            SQL.SHOW_PASSWORD_ENCRYPTION: show("scram-sha-256"),
            SQL.SHOW_ROW_SECURITY: show("on"),
            SQL.EXT_LIST: [{"name": "uuid-ossp", "version": "1.1", "schema": "public"}],
        }
        introspector, _ = make_introspector(responses, password="secret")

        overview = await introspector.get_overview()

        assert isinstance(overview, PostgreSQLServerOverview)
        assert overview.server_vendor == "PostgreSQL"
        assert overview.server_version == "16.0.0"
        assert [item.name for item in overview.configuration] == ["port"]
        assert [item.name for item in overview.performance] == ["checkpoint_timeout"]
        assert isinstance(overview.connections, ConnectionInfo)
        assert overview.connections.active_count == 12
        assert overview.connections.max_connections == 100
        assert isinstance(overview.storage, StorageInfo)
        assert overview.storage.total_size_bytes == 4096
        assert isinstance(overview.databases[0], DatabaseBriefInfo)
        assert "schemas" in overview.databases[0].extra
        assert isinstance(overview.users[0], UserInfo)
        assert isinstance(overview.session, SessionInfo)
        assert overview.session.user == "alice"
        assert overview.session.password_used is True
        assert isinstance(overview.wal, WALInfo)
        assert overview.wal.wal_level == "replica"
        assert isinstance(overview.replication, ReplicationInfo)
        assert overview.replication.is_primary is True
        assert isinstance(overview.archive, ArchiveInfo)
        assert overview.archive.archive_mode == "on"
        assert isinstance(overview.security, SecurityInfo)
        assert overview.security.ssl_enabled is True
        assert isinstance(overview.extensions[0], ExtensionInfo)

    async def test_get_overview_with_no_rows_returns_empty_collections(self):
        introspector, _ = make_introspector()

        overview = await introspector.get_overview()

        assert overview.configuration == []
        assert overview.performance == []
        assert overview.databases == []
        assert overview.users == []
        assert overview.extensions == []
        assert overview.session.user is None
        assert overview.wal == WALInfo()
        assert overview.replication == ReplicationInfo()
        assert overview.archive == ArchiveInfo()
        assert overview.security == SecurityInfo()

    async def test_get_overview_to_dict_serializes_engine_families(self):
        introspector, _ = make_introspector()
        overview = await introspector.get_overview()

        payload = overview.to_dict()

        assert {"wal", "replication", "archive", "security"} <= payload.keys()
        assert payload["server_vendor"] == "PostgreSQL"


# ---------------------------------------------------------------------------
# list_configuration
# ---------------------------------------------------------------------------
class TestAsyncListConfiguration:
    SETTINGS = [
        setting("port", "5432"),
        setting("shared_buffers", "134217728", "MB"),
        setting("ssl", "on"),
        setting("max_wal_senders", "10"),
        setting("checkpoint_timeout", "300", "s"),
        setting("bogus.unknown", "ignored"),
    ]

    async def test_lists_known_settings_with_parsed_values(self):
        introspector, _ = make_introspector({SQL.SETTINGS_ALL: self.SETTINGS})

        items = await introspector.list_configuration()

        assert all(isinstance(item, StatusItem) for item in items)
        names = [item.name for item in items]
        assert "port" in names
        assert "ssl" in names
        assert "bogus.unknown" not in names
        by_name = {item.name: item for item in items}
        assert by_name["port"].value == "5432"
        assert by_name["ssl"].value is True
        assert by_name["port"].is_readonly is True

    async def test_empty_rows_return_no_items(self):
        introspector, _ = make_introspector()

        assert await introspector.list_configuration() == []

    async def test_row_unit_overrides_declared_unit(self):
        introspector, _ = make_introspector(
            {SQL.SETTINGS_ALL: [setting("work_mem", "65536", "kB")]}
        )

        items = await introspector.list_configuration()

        work_mem = next(item for item in items if item.name == "work_mem")
        assert work_mem.unit == "kB"

    async def test_missing_row_unit_falls_back_to_declared_unit(self):
        introspector, _ = make_introspector({SQL.SETTINGS_ALL: [setting("work_mem", "65536")]})

        items = await introspector.list_configuration()

        work_mem = next(item for item in items if item.name == "work_mem")
        assert work_mem.unit == "bytes"

    @pytest.mark.parametrize(
        ("category", "expected_name"),
        [
            (StatusCategory.CONFIGURATION, "port"),
            (StatusCategory.SECURITY, "ssl"),
            (StatusCategory.REPLICATION, "max_wal_senders"),
            (StatusCategory.PERFORMANCE, "checkpoint_timeout"),
        ],
    )
    async def test_category_filter_keeps_only_matching_items(self, category, expected_name):
        introspector, _ = make_introspector({SQL.SETTINGS_ALL: self.SETTINGS})

        items = await introspector.list_configuration(category=category)

        assert items
        assert all(item.category == category for item in items)
        assert expected_name in {item.name for item in items}

    async def test_category_with_no_config_variables_returns_empty(self):
        introspector, _ = make_introspector({SQL.SETTINGS_ALL: self.SETTINGS})

        items = await introspector.list_configuration(category=StatusCategory.CONNECTION)

        assert items == []

    async def test_all_declared_categories_are_reachable(self):
        expected = {cat for _, cat, _, _, _ in POSTGRES_CONFIG_VARIABLES}
        assert expected == {
            StatusCategory.CONFIGURATION,
            StatusCategory.PERFORMANCE,
            StatusCategory.SECURITY,
            StatusCategory.REPLICATION,
        }


# ---------------------------------------------------------------------------
# list_performance_metrics
# ---------------------------------------------------------------------------
class TestAsyncPerformanceMetrics:
    async def test_maps_named_settings_to_performance_items(self):
        rows = [
            setting("checkpoint_timeout", "300", "s"),
            setting("archive_command", "wal-archive"),
            setting("bogus.unknown", "x"),
        ]
        introspector, _ = make_introspector({SQL.SETTINGS_NAMED: rows})

        items = await introspector.list_performance_metrics()

        assert {item.name for item in items} == {"checkpoint_timeout", "archive_command"}
        assert all(item.category == StatusCategory.PERFORMANCE for item in items)
        assert all(isinstance(item, StatusItem) for item in items)

    async def test_empty_rows_return_no_items(self):
        introspector, _ = make_introspector()

        assert await introspector.list_performance_metrics() == []

    async def test_category_argument_does_not_alter_mapping(self):
        rows = [setting("archive_timeout", "60", "s")]
        introspector, _ = make_introspector({SQL.SETTINGS_NAMED: rows})

        items = await introspector.list_performance_metrics(category=StatusCategory.SECURITY)

        assert len(items) == 1
        assert items[0].category == StatusCategory.PERFORMANCE


# ---------------------------------------------------------------------------
# get_connection_info
# ---------------------------------------------------------------------------
class TestAsyncConnectionInfo:
    async def test_returns_activity_total_and_max_connections(self):
        responses = {
            SQL.ACTIVITY: [{"total_connections": 42}],
            SQL.SHOW_MAX_CONN: show("200"),
        }
        introspector, _ = make_introspector(responses)

        info = await introspector.get_connection_info()

        assert isinstance(info, ConnectionInfo)
        assert info.active_count == 42
        assert info.max_connections == 200

    async def test_activity_row_without_key_defaults_to_one(self):
        introspector, _ = make_introspector({SQL.ACTIVITY: [{}]})

        info = await introspector.get_connection_info()

        assert info.active_count == 1

    async def test_no_rows_returns_zero_and_none(self):
        introspector, _ = make_introspector()

        info = await introspector.get_connection_info()

        assert info.active_count == 0
        assert info.max_connections is None

    async def test_show_failure_is_swallowed(self):
        responses = {
            SQL.ACTIVITY: [{"total_connections": 5}],
            SQL.SHOW_MAX_CONN: RuntimeError("SHOW unsupported"),
        }
        introspector, _ = make_introspector(responses)

        info = await introspector.get_connection_info()

        assert info.active_count == 5
        assert info.max_connections is None


# ---------------------------------------------------------------------------
# get_storage_info
# ---------------------------------------------------------------------------
class TestAsyncStorageInfo:
    async def test_returns_database_size(self):
        introspector, _ = make_introspector({SQL.DB_SIZE: [{"size_bytes": 987654}]})

        info = await introspector.get_storage_info()

        assert isinstance(info, StorageInfo)
        assert info.total_size_bytes == 987654

    async def test_empty_rows_leave_size_none(self):
        introspector, _ = make_introspector()

        info = await introspector.get_storage_info()

        assert info.total_size_bytes is None

    async def test_uses_configured_database_name(self):
        introspector, backend = make_introspector(database="analytics")

        await introspector.get_storage_info()

        sql = backend.execute.await_args.args[0]
        assert "PG_DATABASE_SIZE(%s)" in sql


# ---------------------------------------------------------------------------
# list_databases
# ---------------------------------------------------------------------------
class TestAsyncListDatabases:
    async def test_attaches_schema_info_to_current_database(self):
        responses = {
            SQL.DB_LIST: [
                {"name": "postgres", "size_bytes": 100},
                {"name": "testdb", "size_bytes": 200},
            ],
            SQL.CURRENT_DB: [{"current_database": "testdb"}],
            SQL.SCHEMA_STATS: [
                {"schema_name": "public", "table_count": 4, "view_count": 2},
                {"schema_name": "audit", "table_count": 1, "view_count": 0},
            ],
        }
        introspector, _ = make_introspector(responses)

        databases = await introspector.list_databases()

        assert [db.name for db in databases] == ["postgres", "testdb"]
        assert all(isinstance(db, DatabaseBriefInfo) for db in databases)
        current = databases[1]
        assert current.extra["schemas"]["public"] == {"tables": 4, "views": 2}
        assert current.table_count == 5
        assert current.view_count == 2
        assert databases[0].extra == {}

    async def test_schema_rows_without_name_are_skipped(self):
        responses = {
            SQL.DB_LIST: [{"name": "testdb", "size_bytes": 200}],
            SQL.CURRENT_DB: [{"current_database": "testdb"}],
            SQL.SCHEMA_STATS: [
                {"schema_name": None, "table_count": 9, "view_count": 9},
                {"schema_name": "public", "table_count": 2, "view_count": 1},
            ],
        }
        introspector, _ = make_introspector(responses)

        databases = await introspector.list_databases()

        assert databases[0].extra["schemas"] == {"public": {"tables": 2, "views": 1}}

    async def test_no_current_database_leaves_all_databases_without_schema_info(self):
        responses = {
            SQL.DB_LIST: [{"name": "testdb", "size_bytes": 200}],
            SQL.CURRENT_DB: [],
            SQL.SCHEMA_STATS: [{"schema_name": "public", "table_count": 4, "view_count": 2}],
        }
        introspector, _ = make_introspector(responses)

        databases = await introspector.list_databases()

        assert databases[0].extra == {}
        assert databases[0].table_count is None

    async def test_schema_query_failure_is_swallowed(self):
        responses = {
            SQL.DB_LIST: [{"name": "testdb", "size_bytes": 200}],
            SQL.CURRENT_DB: [{"current_database": "testdb"}],
            SQL.SCHEMA_STATS: RuntimeError("permission denied"),
        }
        introspector, _ = make_introspector(responses)

        databases = await introspector.list_databases()

        assert len(databases) == 1
        assert databases[0].extra == {}

    async def test_current_database_query_failure_is_swallowed(self):
        responses = {
            SQL.DB_LIST: [{"name": "testdb", "size_bytes": 200}],
            SQL.CURRENT_DB: RuntimeError("query failed"),
        }
        introspector, _ = make_introspector(responses)

        databases = await introspector.list_databases()

        assert len(databases) == 1

    async def test_empty_database_list(self):
        introspector, _ = make_introspector()

        assert await introspector.list_databases() == []


# ---------------------------------------------------------------------------
# list_users
# ---------------------------------------------------------------------------
class TestAsyncListUsers:
    async def test_maps_roles_with_owner_into_extra(self):
        rows = [
            {"name": "alice", "is_superuser": True, "owner": "postgres"},
            {"name": "bob", "is_superuser": False, "owner": None},
        ]
        introspector, _ = make_introspector({SQL.ROLE_LIST: rows})

        users = await introspector.list_users()

        assert all(isinstance(user, UserInfo) for user in users)
        assert users[0].name == "alice"
        assert users[0].is_superuser is True
        assert users[0].extra == {"owner": "postgres"}
        assert users[1].extra == {}

    async def test_empty_roles(self):
        introspector, _ = make_introspector()

        assert await introspector.list_users() == []


# ---------------------------------------------------------------------------
# get_session_info
# ---------------------------------------------------------------------------
class TestAsyncSessionInfo:
    async def test_full_session_with_ssl_and_password(self):
        responses = {
            SQL.CURRENT_USER: [{"current_user": "alice"}],
            SQL.CURRENT_DB: [{"current_database": "testdb"}],
            SQL.CURRENT_SCHEMA: [{"current_schema": "public"}],
            SQL.CLIENT_ADDR: [{"inet_client_addr": "10.0.0.5"}],
            SQL.SSL_SESSION: [{"ssl": "t", "version": "TLSv1.3", "cipher": "AES-256"}],
        }
        introspector, _ = make_introspector(responses, password="pw")

        session = await introspector.get_session_info()

        assert isinstance(session, SessionInfo)
        assert session.user == "alice"
        assert session.database == "testdb"
        assert session.schema == "public"
        assert session.host == "10.0.0.5"
        assert session.ssl_enabled is True
        assert session.ssl_version == "TLSv1.3"
        assert session.ssl_cipher == "AES-256"
        assert session.password_used is True

    @pytest.mark.parametrize(
        ("ssl_value", "expected"),
        [("t", True), ("true", True), (True, True), ("f", False), (False, False), (None, False)],
    )
    async def test_ssl_flag_parsing(self, ssl_value, expected):
        introspector, _ = make_introspector(
            {SQL.SSL_SESSION: [{"ssl": ssl_value, "version": None, "cipher": None}]}
        )

        session = await introspector.get_session_info()

        assert session.ssl_enabled is expected

    async def test_null_client_host_is_ignored(self):
        introspector, _ = make_introspector({SQL.CLIENT_ADDR: [{"inet_client_addr": None}]})

        session = await introspector.get_session_info()

        assert session.host is None

    async def test_no_rows_leave_session_fields_unset(self):
        introspector, _ = make_introspector()

        session = await introspector.get_session_info()

        assert session.user is None
        assert session.database is None
        assert session.schema is None
        assert session.host is None
        assert session.ssl_enabled is None
        assert session.password_used is False

    async def test_all_queries_failing_still_returns_session_info(self):
        introspector, backend = make_introspector()
        backend.raise_all = True

        session = await introspector.get_session_info()

        assert isinstance(session, SessionInfo)
        assert session.user is None
        assert session.password_used is False


# ---------------------------------------------------------------------------
# get_wal_info
# ---------------------------------------------------------------------------
class TestAsyncWalInfo:
    def _full_responses(self):
        return {
            SQL.SHOW_WAL_LEVEL: show("replica"),
            SQL.WAL_POSITION: [{"wal_bytes": 2048}],
            SQL.WAL_FILES: [{"wal_files": 12}],
            SQL.CHECKPOINT_COUNT: [{"checkpoint_count": 30}],
            SQL.CHECKPOINT_LSN: [{"checkpoint_info": "0/2000000"}],
            SQL.SHOW_WAL_KEEP_SIZE: show("64MB"),
        }

    async def test_full_wal_info(self):
        introspector, _ = make_introspector(self._full_responses())

        wal = await introspector.get_wal_info()

        assert isinstance(wal, WALInfo)
        assert wal.wal_level == "replica"
        assert wal.wal_size_bytes == 2048
        assert wal.wal_files == 12
        assert wal.checkpoint_count == 30
        assert wal.checkpoint_time == "0/2000000"
        assert wal.wal_segments == "64MB"

    async def test_old_version_skips_pg10_queries_and_uses_keep_segments(self):
        responses = self._full_responses()
        responses[SQL.SHOW_WAL_KEEP_SEGMENTS] = show("32")
        introspector, backend = make_introspector(responses, version=(9, 6, 24))

        wal = await introspector.get_wal_info()

        assert wal.wal_segments == "32"
        executed = [call.args[0] for call in backend.execute.await_args_list]
        assert not any("pg_ls_waldir" in sql for sql in executed)
        assert not any("pg_control_checkpoint" in sql for sql in executed)

    async def test_empty_rows_leave_fields_none(self):
        introspector, _ = make_introspector()

        wal = await introspector.get_wal_info()

        assert wal == WALInfo()

    async def test_all_queries_failing_still_returns_wal_info(self):
        introspector, backend = make_introspector()
        backend.raise_all = True

        assert await introspector.get_wal_info() == WALInfo()


# ---------------------------------------------------------------------------
# get_replication_info
# ---------------------------------------------------------------------------
class TestAsyncReplicationInfo:
    async def test_primary_with_active_streaming(self):
        responses = {
            SQL.RECOVERY: show("f"),
            SQL.REPL_SLOTS: [{"slot_count": 3}],
            SQL.ACTIVE_REPL: [{"active_count": 2}],
        }
        introspector, _ = make_introspector(responses)

        repl = await introspector.get_replication_info()

        assert isinstance(repl, ReplicationInfo)
        assert repl.is_primary is True
        assert repl.is_standby is False
        assert repl.replication_slots == 3
        assert repl.active_replications == 2
        assert repl.streaming is True
        assert repl.lag_bytes is None

    async def test_standby_reports_lag(self):
        responses = {
            SQL.RECOVERY: show("t"),
            SQL.REPL_SLOTS: [{"slot_count": 1}],
            SQL.ACTIVE_REPL: [{"active_count": 0}],
            SQL.REPL_LAG: [{"lag_bytes": 4096}],
        }
        introspector, _ = make_introspector(responses)

        repl = await introspector.get_replication_info()

        assert repl.is_standby is True
        assert repl.is_primary is False
        assert repl.streaming is False
        assert repl.lag_bytes == 4096

    @pytest.mark.parametrize("recovery_value", [True, "t", "true"])
    async def test_truthy_recovery_values_mean_standby(self, recovery_value):
        introspector, _ = make_introspector({SQL.RECOVERY: show(recovery_value)})

        repl = await introspector.get_replication_info()

        assert repl.is_standby is True

    async def test_standby_lag_query_failure_is_swallowed(self):
        responses = {
            SQL.RECOVERY: show("t"),
            SQL.REPL_LAG: RuntimeError("lag query unsupported"),
        }
        introspector, _ = make_introspector(responses)

        repl = await introspector.get_replication_info()

        assert repl.is_standby is True
        assert repl.lag_bytes is None

    async def test_empty_rows_leave_flags_unset(self):
        introspector, _ = make_introspector()

        repl = await introspector.get_replication_info()

        assert repl == ReplicationInfo()

    async def test_all_queries_failing_still_returns_replication_info(self):
        introspector, backend = make_introspector()
        backend.raise_all = True

        assert await introspector.get_replication_info() == ReplicationInfo()


# ---------------------------------------------------------------------------
# get_archive_info
# ---------------------------------------------------------------------------
class TestAsyncArchiveInfo:
    async def test_full_archive_info_with_integer_timeout(self):
        responses = {
            SQL.SHOW_ARCHIVE_MODE: show("on"),
            SQL.SHOW_ARCHIVE_COMMAND: show("wal-archive %p"),
            SQL.SHOW_ARCHIVE_TIMEOUT: show("300"),
            SQL.ARCHIVE_STATS: [
                {
                    "archived_count": 10,
                    "failed_count": 2,
                    "last_archived": "2026-11-20",
                    "last_failed": "2026-11-19",
                }
            ],
        }
        introspector, _ = make_introspector(responses)

        archive = await introspector.get_archive_info()

        assert isinstance(archive, ArchiveInfo)
        assert archive.archive_mode == "on"
        assert archive.archive_command == "wal-archive %p"
        assert archive.archive_timeout == 300
        assert archive.archived_count == 10
        assert archive.failed_count == 2
        assert archive.last_archived == "2026-11-20"
        assert archive.last_failed == "2026-11-19"

    @pytest.mark.parametrize("timeout_value", ["5min", "0", None, ""])
    async def test_non_integer_or_zero_timeout_is_ignored(self, timeout_value):
        introspector, _ = make_introspector({SQL.SHOW_ARCHIVE_TIMEOUT: show(timeout_value)})

        archive = await introspector.get_archive_info()

        assert archive.archive_timeout is None

    async def test_archive_stats_with_null_counts_default_to_zero(self):
        introspector, _ = make_introspector(
            {SQL.ARCHIVE_STATS: [{"archived_count": None, "failed_count": None}]}
        )

        archive = await introspector.get_archive_info()

        assert archive.archived_count == 0
        assert archive.failed_count == 0

    async def test_empty_rows_leave_fields_none(self):
        introspector, _ = make_introspector()

        assert await introspector.get_archive_info() == ArchiveInfo()

    async def test_all_queries_failing_still_returns_archive_info(self):
        introspector, backend = make_introspector()
        backend.raise_all = True

        assert await introspector.get_archive_info() == ArchiveInfo()


# ---------------------------------------------------------------------------
# get_security_info
# ---------------------------------------------------------------------------
class TestAsyncSecurityInfo:
    async def test_full_security_info_from_show_values(self):
        responses = {
            SQL.SHOW_SSL: show("on"),
            SQL.SHOW_SSL_CERT: show("server.crt"),
            SQL.SHOW_SSL_KEY: show("server.key"),
            SQL.SHOW_SSL_CA: show("ca.crt"),
            SQL.SHOW_PASSWORD_ENCRYPTION: show("scram-sha-256"),
            SQL.SHOW_ROW_SECURITY: show("on"),
        }
        introspector, _ = make_introspector(responses)

        security = await introspector.get_security_info()

        assert isinstance(security, SecurityInfo)
        assert security.ssl_enabled is True
        assert security.ssl_cert_file == "server.crt"
        assert security.ssl_key_file == "server.key"
        assert security.ssl_ca_file == "ca.crt"
        assert security.password_encryption == "scram-sha-256"
        assert security.row_security is True

    @pytest.mark.parametrize(
        ("ssl_value", "rls_value", "expected_ssl", "expected_rls"),
        [
            (True, True, True, True),
            ("off", "off", False, False),
            (False, False, False, False),
        ],
    )
    async def test_boolean_flags(self, ssl_value, rls_value, expected_ssl, expected_rls):
        responses = {SQL.SHOW_SSL: show(ssl_value), SQL.SHOW_ROW_SECURITY: show(rls_value)}
        introspector, _ = make_introspector(responses)

        security = await introspector.get_security_info()

        assert security.ssl_enabled is expected_ssl
        assert security.row_security is expected_rls

    async def test_empty_rows_leave_fields_none(self):
        introspector, _ = make_introspector()

        assert await introspector.get_security_info() == SecurityInfo()

    async def test_all_queries_failing_still_returns_security_info(self):
        introspector, backend = make_introspector()
        backend.raise_all = True

        assert await introspector.get_security_info() == SecurityInfo()


# ---------------------------------------------------------------------------
# list_extensions
# ---------------------------------------------------------------------------
class TestAsyncExtensions:
    async def test_maps_extension_rows(self):
        rows = [
            {"name": "uuid-ossp", "version": "1.1", "schema": "public", "description": "UUIDs"},
            {"name": "pgcrypto", "version": "1.3", "schema": None, "description": None},
        ]
        introspector, _ = make_introspector({SQL.EXT_LIST: rows})

        extensions = await introspector.list_extensions()

        assert len(extensions) == 2
        assert all(isinstance(ext, ExtensionInfo) for ext in extensions)
        assert extensions[0].name == "uuid-ossp"
        assert extensions[0].version == "1.1"
        assert extensions[0].description == "UUIDs"
        assert extensions[1].schema is None

    async def test_empty_rows_return_empty_list(self):
        introspector, _ = make_introspector()

        assert await introspector.list_extensions() == []

    async def test_query_failure_returns_empty_list(self):
        introspector, backend = make_introspector()
        backend.raise_all = True

        assert await introspector.list_extensions() == []


# ---------------------------------------------------------------------------
# I/O seam and inherited helpers (through the async class)
# ---------------------------------------------------------------------------
class TestAsyncExecutionSeamAndHelpers:
    async def test_exec_query_async_returns_empty_when_execute_returns_none(self):
        introspector, backend = make_introspector()
        backend.null_result = True

        result = await introspector._exec_query_async("SELECT 1")

        assert result == []
        backend.execute.assert_awaited_once_with("SELECT 1", None)

    async def test_exec_query_async_returns_result_data(self):
        introspector, backend = make_introspector({SQL.CURRENT_DB: [{"current_database": "x"}]})

        result = await introspector._exec_query_async(*[SQL.CURRENT_DB, ()])

        assert result == [{"current_database": "x"}]

    async def test_get_vendor_name(self):
        introspector, _ = make_introspector()

        assert introspector._get_vendor_name() == "PostgreSQL"

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (b"utf8", "utf8"),
            (True, True),
            ("on", True),
            ("yes", True),
            ("off", False),
            ("no", False),
            (42, 42),
            ("5432", "5432"),
            (1.5, "1.5"),
        ],
    )
    async def test_parse_setting_value(self, raw, expected):
        introspector, _ = make_introspector()

        assert introspector.parse_setting_value(raw) == expected

    async def test_create_status_item_parses_value(self):
        introspector, _ = make_introspector()

        item = introspector._create_status_item(
            name="ssl",
            value="on",
            category=StatusCategory.SECURITY,
            description="SSL enabled",
            unit=None,
            is_readonly=True,
        )

        assert item.value is True
        assert item.category is StatusCategory.SECURITY
        assert item.description == "SSL enabled"
        assert item.is_readonly is True


class TestAsyncVersionResolution:
    async def test_version_string_from_server_version_cache(self):
        introspector, _ = make_introspector(version=(18, 3, 1))

        assert introspector._get_version_string() == "18.3.1"

    async def test_version_tuple_from_server_version_cache(self):
        introspector, _ = make_introspector(version=(18, 3, 1))

        assert introspector._get_version_tuple() == (18, 3, 1)

    async def test_version_string_falls_back_to_private_version(self):
        introspector, backend = make_introspector()
        del backend._server_version_cache
        backend._version = (15, 2, 0)

        assert introspector._get_version_string() == "15.2.0"

    async def test_version_string_falls_back_to_get_server_version(self):
        introspector, backend = make_introspector(version=(14, 1, 0))
        del backend._server_version_cache
        backend.get_server_version.return_value = (14, 1, 0)

        assert introspector._get_version_string() == "14.1.0"

    async def test_version_string_unknown_when_all_sources_fail(self):
        introspector, backend = make_introspector()
        del backend._server_version_cache
        backend.get_server_version.side_effect = RuntimeError("no server")

        assert introspector._get_version_string() == "Unknown"

    async def test_version_tuple_unknown_when_all_sources_fail(self):
        introspector, backend = make_introspector()
        del backend._server_version_cache
        backend.get_server_version.side_effect = RuntimeError("no server")

        assert introspector._get_version_tuple() == (0, 0, 0)
