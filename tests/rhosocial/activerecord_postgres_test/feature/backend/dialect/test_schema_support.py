# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_schema_support.py
"""Tests for the schema capability declared on the PostgreSQL dialect.

Two questions, and they are not the same one. Does the engine *have* schemas --
the DDL question, answered by ``supports_schema()`` and its granular siblings?
May a *name* be qualified with one -- the naming question, answered by
``supports_schema_qualification()``? PostgreSQL answers yes to both, and it also
has a database above its schema, so the outer namespace is reported too.

The rendering tests use the ``Schema`` object rather than a string, because a
schema name is now catalogue identity: the object carries the name and renders
itself.
"""
from rhosocial.activerecord.backend.dialect.protocols import NamespaceSupport
from rhosocial.activerecord.backend.expression.objects import (
    Schema,
    Table,
)
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

    def test_implements_namespace_support_protocol(self):
        """The naming question lives on NamespaceSupport now.

        Naming was split from rendering, so the protocol that carries the
        namespace switches is no longer the schema-DDL one; it is the base
        every object protocol derives from.
        """
        assert isinstance(self._dialect(), NamespaceSupport)

    def test_granular_schema_ddl_capabilities(self):
        d = self._dialect()
        assert d.supports_create_schema() is True
        assert d.supports_drop_schema() is True
        assert d.supports_schema_if_not_exists() is True
        assert d.supports_schema_if_exists() is True
        assert d.supports_schema_cascade() is True

    def test_schema_authorization_capability(self):
        assert self._dialect().supports_schema_authorization() is True


class TestSchemaQualification:
    """PostgreSQL renders a name with its schema, and with a database above it.

    Distinct from :class:`TestSchemaCapability`: having schemas is not the same
    as qualifying a name with one, and a dialect can answer the two
    differently. PostgreSQL qualifies both, so this is asserted rather than
    assumed.
    """

    def _dialect(self) -> PostgresDialect:
        return PostgresDialect()

    def test_supports_schema_qualification_is_true(self):
        assert self._dialect().supports_schema_qualification() is True

    def test_supports_catalog_is_true(self):
        assert self._dialect().supports_catalog() is True

    def test_supports_catalog_qualification_is_true(self):
        assert self._dialect().supports_catalog_qualification() is True

    def test_schema_qualified_table_renders_both_levels(self):
        d = self._dialect()
        sql, params = d.format_table_object(
            Table(d, "users", catalog_name="appdb", schema_name="app")
        )
        assert sql == '"appdb"."app"."users"'
        assert params == ()

    def test_schema_qualified_table_without_catalog_omits_it(self):
        d = self._dialect()
        sql, _ = d.format_table_object(Table(d, "users", schema_name="app"))
        assert sql == '"app"."users"'

    def test_unqualified_table_is_not_qualified(self):
        d = self._dialect()
        sql, _ = d.format_table_object(Table(d, "users"))
        assert sql == '"users"'


class TestSchemaDDLFormatting:
    """CREATE/DROP SCHEMA rendering through the standard core formatters."""

    def _dialect(self) -> PostgresDialect:
        return PostgresDialect()

    def _schema(self) -> Schema:
        return Schema(self._dialect(), "app")

    def test_create_schema(self):
        sql, params = CreateSchemaExpression(self._dialect(), self._schema()).to_sql()
        assert sql == 'CREATE SCHEMA "app"'
        assert params == ()

    def test_create_schema_if_not_exists(self):
        sql, _ = CreateSchemaExpression(
            self._dialect(), self._schema(), if_not_exists=True
        ).to_sql()
        assert sql == 'CREATE SCHEMA IF NOT EXISTS "app"'

    def test_create_schema_authorization(self):
        sql, _ = CreateSchemaExpression(
            self._dialect(), self._schema(), authorization="app_user"
        ).to_sql()
        assert sql == 'CREATE SCHEMA "app" AUTHORIZATION "app_user"'

    def test_drop_schema(self):
        sql, params = DropSchemaExpression(self._dialect(), self._schema()).to_sql()
        assert sql == 'DROP SCHEMA "app"'
        assert params == ()

    def test_drop_schema_if_exists_cascade(self):
        sql, _ = DropSchemaExpression(
            self._dialect(), self._schema(), if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP SCHEMA IF EXISTS "app" CASCADE'