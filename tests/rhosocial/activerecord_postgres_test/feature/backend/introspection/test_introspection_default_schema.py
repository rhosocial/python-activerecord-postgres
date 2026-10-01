# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_default_schema.py
"""Offline tests for introspector default-schema resolution.

``PostgreSQLIntrospectorMixin._get_default_schema`` reads the introspector's
own ``self._backend.config`` and resolves in this order:

    config.default_schema > first entry of config.search_path > 'public'

The *dialect*-side counterpart used to mirror this, but it read
``self._config`` / ``self._backend`` -- attributes ``SQLDialectBase.__init__``
never assigns. It therefore always returned ``'public'`` in production, and its
tests only passed because they monkeypatched those attributes onto a bare
``PostgresDialect()``. That implementation, and the test class covering it,
have been removed; the dialect now states its ``'public'`` result directly.

Note that ``config.default_schema`` has no effect on generated SQL either -- it
never did. Model-level ``__schema_name__`` is the only thing that qualifies a
statement; unqualified names resolve through the connection's ``search_path``.
"""
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
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
    """PostgreSQLIntrospectorMixin._get_default_schema (self._backend.config)."""

    def test_returns_public_when_not_configured(self):
        introspector = make_introspector(make_config())
        assert introspector._get_default_schema() == "public"

    def test_returns_default_schema_from_config(self):
        introspector = make_introspector(make_config(default_schema="broker"))
        assert introspector._get_default_schema() == "broker"

    def test_returns_first_entry_of_search_path(self):
        introspector = make_introspector(make_config(search_path="broker, public"))
        assert introspector._get_default_schema() == "broker"

    def test_strips_quotes_from_search_path_entry(self):
        introspector = make_introspector(make_config(search_path='"broker", public'))
        assert introspector._get_default_schema() == "broker"

    def test_default_schema_takes_priority_over_search_path(self):
        introspector = make_introspector(
            make_config(default_schema="app", search_path="broker, public")
        )
        assert introspector._get_default_schema() == "app"
