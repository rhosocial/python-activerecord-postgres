# tests/rhosocial/activerecord_postgres_test/feature/backend/schema/test_schema_support.py
"""Tests for the SchemaSupport capability declared on the PostgreSQL dialect.

PostgreSQL models named schema namespaces natively (database -> schema -> table),
so the dialect must report ``supports_schema()`` as True together with the full
set of granular schema DDL capabilities.
"""
from rhosocial.activerecord.backend.dialect.protocols import SchemaSupport
from rhosocial.activerecord.backend.expression.statements.ddl_schema import (
    CreateSchemaExpression,
    DropSchemaExpression,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestSchemaCapability:
    """Umbrella flag and granular schema DDL capability bits."""

    def _dialect(self) -> PostgresDialect:
        return PostgresDialect()

    def test_supports_schema_is_true(self):
        assert self._dialect().supports_schema() is True

    def test_implements_schema_support_protocol(self):
        assert isinstance(self._dialect(), SchemaSupport)

    def test_granular_schema_ddl_capabilities(self):
        d = self._dialect()
        assert d.supports_create_schema() is True
        assert d.supports_drop_schema() is True
        assert d.supports_schema_if_not_exists() is True
        assert d.supports_schema_if_exists() is True
        assert d.supports_schema_cascade() is True

    def test_schema_authorization_capability(self):
        assert self._dialect().supports_schema_authorization() is True


class TestSchemaDDLFormatting:
    """CREATE/DROP SCHEMA rendering through the standard core formatters."""

    def _dialect(self) -> PostgresDialect:
        return PostgresDialect()

    def test_create_schema(self):
        sql, params = CreateSchemaExpression(self._dialect(), "app").to_sql()
        assert sql == 'CREATE SCHEMA "app"'
        assert params == ()

    def test_create_schema_if_not_exists(self):
        sql, _ = CreateSchemaExpression(self._dialect(), "app", if_not_exists=True).to_sql()
        assert sql == 'CREATE SCHEMA IF NOT EXISTS "app"'

    def test_create_schema_authorization(self):
        sql, _ = CreateSchemaExpression(self._dialect(), "app", authorization="app_user").to_sql()
        assert sql == 'CREATE SCHEMA "app" AUTHORIZATION "app_user"'

    def test_drop_schema(self):
        sql, params = DropSchemaExpression(self._dialect(), "app").to_sql()
        assert sql == 'DROP SCHEMA "app"'
        assert params == ()

    def test_drop_schema_if_exists_cascade(self):
        sql, _ = DropSchemaExpression(self._dialect(), "app", if_exists=True, cascade=True).to_sql()
        assert sql == 'DROP SCHEMA IF EXISTS "app" CASCADE'
