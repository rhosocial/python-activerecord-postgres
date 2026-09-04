from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend, PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.transaction import (
    AsyncPostgresTransactionManager,
    PostgresTransactionManager,
)


class TestPostgresBackendInitialization:
    def test_sync_backend_builds_default_config_from_kwargs(self):
        backend = PostgresBackend(database="app_db", username="app_user", application_name="tests")

        assert backend.config.host == "localhost"
        assert backend.config.port == 5432
        assert backend.config.database == "app_db"
        assert backend.config.username == "app_user"
        assert backend.config.application_name == "tests"
        assert isinstance(backend.dialect, PostgresDialect)
        assert isinstance(backend.transaction_manager, PostgresTransactionManager)

    def test_sync_backend_preserves_explicit_connection_config(self):
        config = PostgresConnectionConfig(host="db.local", port=15432, database="app_db")
        backend = PostgresBackend(connection_config=config)

        assert backend.config is config
        assert isinstance(backend.dialect, PostgresDialect)

    def test_async_backend_builds_default_config_from_kwargs(self):
        backend = AsyncPostgresBackend(
            database="app_db",
            username="app_user",
            application_name="tests",
        )

        assert backend.config.host == "localhost"
        assert backend.config.port == 5432
        assert backend.config.database == "app_db"
        assert backend.config.username == "app_user"
        assert backend.config.application_name == "tests"
        assert isinstance(backend.dialect, PostgresDialect)
        assert isinstance(backend.transaction_manager, AsyncPostgresTransactionManager)

    def test_async_backend_preserves_explicit_connection_config(self):
        config = PostgresConnectionConfig(host="db.local", port=15432, database="app_db")
        backend = AsyncPostgresBackend(connection_config=config)

        assert backend.config is config
        assert isinstance(backend.dialect, PostgresDialect)
