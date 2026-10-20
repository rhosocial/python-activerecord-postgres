# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_status_query_nodes.py
"""The status-introspection nodes (SQL generation only).

The status introspector used to hold the catalog SQL itself: ``execute("SELECT
current_user")``, ``"SHOW wal_level"``, f-strings assembled per version. A SQL
string is a formatting artefact, so the node now names the question and this
backend's formatter writes the text; these tests pin the text each node owes, one
node at a time.

Every case is a construction → render assertion. Nothing here opens a
connection: expression tests stay offline (expression-dialect §12), and the live
legs live in ``introspection/test_status_introspector.py``.
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.status import (
    PostgresActiveReplicationCountQuery,
    PostgresActivityCountQuery,
    PostgresArchiveStatsQuery,
    PostgresCheckpointCountQuery,
    PostgresCheckpointLsnQuery,
    PostgresClientAddressQuery,
    PostgresCurrentSchemaQuery,
    PostgresCurrentUserQuery,
    PostgresDatabaseListQuery,
    PostgresExtensionListQuery,
    PostgresRecoveryStateQuery,
    PostgresReplicationLagQuery,
    PostgresReplicationSlotCountQuery,
    PostgresRoleListQuery,
    PostgresSchemaStatsQuery,
    PostgresSettingsQuery,
    PostgresShowStatement,
    PostgresSslSessionQuery,
    PostgresWalFileCountQuery,
    PostgresWalPositionQuery,
)


def _dialect(version=(17, 0, 0)):
    return PostgresDialect(version=version)


class TestSessionQueries:
    """One row of session facts, each a node with no parameters."""

    def test_current_user_is_niladic(self):
        # The standard writes CURRENT_USER without parentheses, and PostgreSQL
        # refuses CURRENT_USER() (measured 17.11: syntax error at or near "(").
        sql, params = PostgresCurrentUserQuery(_dialect()).to_sql()
        assert sql == "SELECT CURRENT_USER"
        assert params == ()

    def test_current_schema_is_a_call(self):
        sql, params = PostgresCurrentSchemaQuery(_dialect()).to_sql()
        assert sql == "SELECT CURRENT_SCHEMA()"
        assert params == ()

    def test_client_address_is_a_call(self):
        sql, params = PostgresClientAddressQuery(_dialect()).to_sql()
        assert sql == "SELECT INET_CLIENT_ADDR()"
        assert params == ()

    def test_ssl_session_reads_its_own_row(self):
        sql, params = PostgresSslSessionQuery(_dialect()).to_sql()
        assert sql == (
            "SELECT ssl, version, cipher FROM pg_stat_ssl "
            "WHERE pid = PG_BACKEND_PID()"
        )
        assert params == ()

    def test_recovery_state_is_a_call(self):
        sql, params = PostgresRecoveryStateQuery(_dialect()).to_sql()
        assert sql == "SELECT PG_IS_IN_RECOVERY()"
        assert params == ()


class TestWalQueries:
    """The WAL reads whose spelling depends on the server version."""

    def test_position_spells_wal_from_version_10(self):
        sql, params = PostgresWalPositionQuery(_dialect()).to_sql()
        assert "PG_CURRENT_WAL_LSN()" in sql
        assert "PG_WAL_LSN_DIFF(" in sql
        # the LSN difference is a numeric; the row's type is stated by the cast
        assert "::BIGINT" in sql
        assert 'AS "wal_bytes"' in sql
        assert params == ()

    def test_position_spells_xlog_before_version_10(self):
        sql, _ = PostgresWalPositionQuery(_dialect((9, 6, 0))).to_sql()
        assert "PG_CURRENT_XLOG_LOCATION()" in sql
        assert "PG_XLOG_LOCATION_DIFF(" in sql

    def test_file_count_absorbs_the_set_returning_function(self):
        sql, params = PostgresWalFileCountQuery(_dialect()).to_sql()
        assert "COUNT(*)" in sql
        assert "FROM pg_ls_waldir()" in sql
        assert params == ()

    def test_file_count_is_refused_before_version_10(self):
        # pg_ls_waldir() was introduced in 10; the node refuses rather than
        # rendering a call the server cannot resolve.
        with pytest.raises(UnsupportedFeatureError):
            PostgresWalFileCountQuery(_dialect((9, 6, 0))).to_sql()

    def test_checkpoint_count_reads_bgwriter_before_17(self):
        sql, _ = PostgresCheckpointCountQuery(_dialect((16, 0, 0))).to_sql()
        assert "FROM pg_stat_bgwriter" in sql
        assert "COALESCE(checkpoints_timed, 0)" in sql
        assert "COALESCE(checkpoints_req, 0)" in sql

    def test_checkpoint_count_reads_checkpointer_from_17(self):
        sql, _ = PostgresCheckpointCountQuery(_dialect((17, 0, 0))).to_sql()
        assert "FROM pg_stat_checkpointer" in sql
        assert "COALESCE(num_timed, 0)" in sql
        assert "COALESCE(num_requested, 0)" in sql

    def test_checkpoint_lsn_names_the_function_as_a_row_source(self):
        # ``(pg_control_checkpoint()).checkpoint_lsn`` is a syntax error on
        # PostgreSQL (measured 17.11); the composite's field is read off a
        # FROM source instead.
        sql, params = PostgresCheckpointLsnQuery(_dialect()).to_sql()
        assert sql == (
            'SELECT c.checkpoint_lsn AS "checkpoint_info" '
            'FROM pg_control_checkpoint() AS "c"'
        )
        assert params == ()

    def test_checkpoint_lsn_is_refused_before_9_6(self):
        with pytest.raises(UnsupportedFeatureError):
            PostgresCheckpointLsnQuery(_dialect((9, 5, 0))).to_sql()


class TestReplicationQueries:
    """Replication fan-out and lag."""

    def test_slot_count(self):
        sql, _ = PostgresReplicationSlotCountQuery(_dialect()).to_sql()
        assert sql == 'SELECT COUNT(*) AS "slot_count" FROM pg_replication_slots'

    def test_active_count_is_narrowed_to_streaming(self):
        sql, _ = PostgresActiveReplicationCountQuery(_dialect()).to_sql()
        assert "FROM pg_stat_replication" in sql
        assert "WHERE state = 'streaming'" in sql
        assert 'AS "active_count"' in sql

    def test_lag_is_the_lsn_distance(self):
        sql, _ = PostgresReplicationLagQuery(_dialect()).to_sql()
        assert "PG_LAST_WAL_RECEIVE_LSN()" in sql
        assert "PG_LAST_WAL_REPLAY_LSN()" in sql
        assert "::BIGINT" in sql
        assert 'AS "lag_bytes"' in sql

    def test_lag_spells_xlog_before_version_10(self):
        # The three names moved together in 10, so a 9.x server needs all three
        # old spellings -- pg_wal_lsn_diff does not exist there.
        sql, _ = PostgresReplicationLagQuery(_dialect((9, 6, 0))).to_sql()
        assert "PG_XLOG_LOCATION_DIFF(" in sql
        assert "PG_LAST_XLOG_RECEIVE_LOCATION()" in sql
        assert "PG_LAST_XLOG_REPLAY_LOCATION()" in sql


class TestArchiveAndExtensionQueries:
    """The archiver row and the extension listing."""

    def test_archive_stats_casts_the_timestamps_to_text(self):
        sql, _ = PostgresArchiveStatsQuery(_dialect()).to_sql()
        assert "last_archived_time::TEXT" in sql
        assert "last_failed_time::TEXT" in sql
        assert 'AS "last_archived"' in sql
        assert 'AS "last_failed"' in sql

    def test_extension_list_joins_the_namespace(self):
        sql, _ = PostgresExtensionListQuery(_dialect()).to_sql()
        assert "FROM pg_extension AS e" in sql
        assert "LEFT JOIN pg_namespace AS n ON n.oid = e.extnamespace" in sql
        assert "PG_CATALOG.OBJ_DESCRIPTION(e.oid, 'pg_extension')" in sql
        assert "ORDER BY e.extname" in sql


class TestShowStatement:
    """``SHOW`` names a run-time parameter and takes no bind parameter."""

    def test_show_renders_the_parameter(self):
        sql, params = PostgresShowStatement(_dialect(), "transaction_isolation").to_sql()
        assert sql == "SHOW transaction_isolation"
        assert params == ()

    @pytest.mark.parametrize("name", ["", "wal level", "a'b", "x;y"])
    def test_show_refuses_a_name_that_is_not_a_parameter(self, name):
        with pytest.raises(ValueError):
            PostgresShowStatement(_dialect(), name)


class TestSettingsAndCountingQueries:
    """The nodes that existed before this wave, now built from nodes."""

    def test_settings_without_names_reads_every_row(self):
        sql, params = PostgresSettingsQuery(_dialect()).to_sql()
        assert sql == "SELECT name, setting, unit FROM pg_settings ORDER BY name"
        assert params == ()

    def test_settings_with_names_binds_them_in_order(self):
        sql, params = PostgresSettingsQuery(_dialect(), ["a", "b"]).to_sql()
        assert sql == (
            "SELECT name, setting, unit FROM pg_settings "
            "WHERE name IN (%s, %s) ORDER BY name"
        )
        assert params == ("a", "b")

    def test_activity_count_uses_the_row_count_wildcard(self):
        sql, params = PostgresActivityCountQuery(_dialect()).to_sql()
        assert sql == 'SELECT COUNT(*) AS "total_connections" FROM pg_stat_activity'
        assert params == ()

    def test_database_list_binds_nothing(self):
        sql, params = PostgresDatabaseListQuery(_dialect()).to_sql()
        assert "PG_DATABASE_SIZE(datname)" in sql
        assert "datistemplate = FALSE" in sql
        assert "datname = CURRENT_DATABASE()" in sql
        assert params == ()

    def test_schema_stats_groups_by_schema(self):
        sql, params = PostgresSchemaStatsQuery(_dialect()).to_sql()
        assert "COUNT(CASE WHEN c.relkind = 'r' THEN 1 END)" in sql
        assert "NOT (n.nspname IN ('pg_catalog', 'information_schema', 'pg_toast'))" in sql
        assert "GROUP BY n.nspname ORDER BY n.nspname" in sql
        assert params == ()

    def test_role_list_reports_the_owner(self):
        sql, params = PostgresRoleListQuery(_dialect()).to_sql()
        assert "PG_CATALOG.PG_GET_USERBYID(oid)" in sql
        assert "FROM pg_roles ORDER BY rolname" in sql
        assert params == ()
