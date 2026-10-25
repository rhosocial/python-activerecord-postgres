# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_default_schema.py
"""Offline tests for ``default_schema`` config-aware resolution.

Both introspection stacks gained config-aware default-schema resolution:

1. ``PostgreSQLIntrospectorMixin.default_schema`` (introspector) reads
   ``self._backend.config``.
2. ``PostgresIntrospectionCapabilityMixin.default_schema`` (dialect SQL
   generation) reads ``self._config`` or ``self._backend``.

Priority: config.default_schema > first entry of config.search_path > 'public'.
"""
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.introspection import (
    SyncPostgreSQLIntrospector,
)


def make_config(**overrides) -> PostgresConnectionConfig:
    params = dict(
        host="localhost",
        port=5432,
        database="test",
        username="postgres",
        password="",
    )
    params.update(overrides)
    return PostgresConnectionConfig(**params)


def make_introspector(config: PostgresConnectionConfig) -> SyncPostgreSQLIntrospector:
    backend = PostgresBackend(connection_config=config)
    return SyncPostgreSQLIntrospector(backend, executor=object())


class TestIntrospectorDefaultSchema:
    """PostgreSQLIntrospectorMixin.default_schema (self._backend.config)."""

    def test_returns_public_when_not_configured(self):
        introspector = make_introspector(make_config())
        assert introspector.default_schema == "public"

    def test_returns_default_schema_from_config(self):
        introspector = make_introspector(make_config(default_schema="broker"))
        assert introspector.default_schema == "broker"

    def test_returns_first_entry_of_search_path(self):
        introspector = make_introspector(make_config(search_path="broker, public"))
        assert introspector.default_schema == "broker"

    def test_strips_quotes_from_search_path_entry(self):
        introspector = make_introspector(make_config(search_path='"broker", public'))
        assert introspector.default_schema == "broker"

    def test_default_schema_takes_priority_over_search_path(self):
        introspector = make_introspector(
            make_config(default_schema="app", search_path="broker, public")
        )
        assert introspector.default_schema == "app"


class TestDialectMixinDefaultSchema:
    """PostgresIntrospectionCapabilityMixin.default_schema (dialect)."""

    def test_returns_public_when_not_configured(self):
        assert PostgresDialect().default_schema == "public"

    def test_returns_default_schema_from_config(self):
        dialect = PostgresDialect()
        dialect._config = make_config(default_schema="broker")
        assert dialect.default_schema == "broker"

    def test_returns_first_entry_of_search_path(self):
        dialect = PostgresDialect()
        dialect._config = make_config(search_path="broker, public")
        assert dialect.default_schema == "broker"

    def test_returns_schema_from_backend(self):
        dialect = PostgresDialect()
        backend = PostgresBackend(connection_config=make_config(search_path="warehouse"))
        dialect._backend = backend
        assert dialect.default_schema == "warehouse"
