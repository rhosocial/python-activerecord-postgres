# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_schema_namespace.py
"""
Tests for PostgreSQL schema namespace support.

Covers:
  - PostgresDialect schema capability declarations
  - Expression/Dialect layer: CreateSchemaExpression, DropSchemaExpression,
    Column/TableExpression/WildcardExpression with schema_name
  - Backend layer: CREATE/DROP schema, table operations in custom schema,
    introspection with schema
  - Async Backend layer: same operations with async backend
"""

import pytest
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.core import (
    Column, TableExpression, WildcardExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_schema import (
    CreateSchemaExpression,
    DropSchemaExpression,
)


# ============================================================
# Dialect Layer Tests (no database connection needed)
# ============================================================


class TestSchemaNamespaceDialect:
    """Test PostgresDialect schema capability declarations and SQL generation."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect()

    def test_supports_create_schema(self, dialect):
        assert dialect.supports_create_schema() is True

    def test_supports_drop_schema(self, dialect):
        assert dialect.supports_drop_schema() is True

    def test_supports_schema_if_not_exists(self, dialect):
        assert dialect.supports_schema_if_not_exists() is True

    def test_supports_schema_if_exists(self, dialect):
        assert dialect.supports_schema_if_exists() is True

    def test_supports_schema_cascade(self, dialect):
        assert dialect.supports_schema_cascade() is True

    def test_column_with_schema_name(self, dialect):
        col = Column(dialect, "id", table="users", schema_name="app")
        sql, params = col.to_sql()
        assert sql == '"app"."users"."id"'
        assert params == ()

    def test_table_expression_with_schema(self, dialect):
        table = TableExpression(dialect, "users", schema_name="app")
        sql, params = table.to_sql()
        assert sql == '"app"."users"'
        assert params == ()

    def test_table_expression_with_schema_and_alias(self, dialect):
        table = TableExpression(dialect, "users", schema_name="app", alias="u")
        sql, params = table.to_sql()
        assert '"app"."users"' in sql
        assert '"u"' in sql

    def test_wildcard_with_schema(self, dialect):
        wc = WildcardExpression(dialect, table="users", schema_name="app")
        sql, params = wc.to_sql()
        assert '"app"."users".*' == sql
        assert params == ()

    def test_create_schema_expression(self, dialect):
        expr = CreateSchemaExpression(dialect, schema_name="app")
        sql, params = expr.to_sql()
        assert sql == 'CREATE SCHEMA "app"'
        assert params == ()

    def test_drop_schema_expression(self, dialect):
        expr = DropSchemaExpression(dialect, schema_name="app")
        sql, params = expr.to_sql()
        assert sql == 'DROP SCHEMA "app"'
        assert params == ()

    def test_create_schema_if_not_exists(self, dialect):
        expr = CreateSchemaExpression(dialect, schema_name="app", if_not_exists=True)
        sql, params = expr.to_sql()
        assert "IF NOT EXISTS" in sql
        assert '"app"' in sql

    def test_drop_schema_if_exists_cascade(self, dialect):
        expr = DropSchemaExpression(
            dialect, schema_name="app", if_exists=True, cascade=True,
        )
        sql, params = expr.to_sql()
        assert "IF EXISTS" in sql
        assert "CASCADE" in sql
        assert '"app"' in sql


# ============================================================
# Backend Layer Tests (require live PostgreSQL connection)
# ============================================================


class TestSchemaNamespaceBackend:
    """Test schema namespace operations with real PostgresBackend."""

    def test_get_default_schema_is_public(self, postgres_backend_single):
        assert postgres_backend_single.get_default_schema() == "public"

    def test_create_and_drop_schema(self, postgres_backend_single):
        schema_name = "test_schema_ns"
        # Ensure clean state
        postgres_backend_single.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        # Create schema
        postgres_backend_single.execute(f'CREATE SCHEMA "{schema_name}"')
        # Verify schema exists
        result = postgres_backend_single.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = %s",
            (schema_name,),
        )
        assert len(result.data) > 0
        # Drop schema
        postgres_backend_single.execute(f'DROP SCHEMA "{schema_name}"')
        # Verify schema is gone
        result = postgres_backend_single.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = %s",
            (schema_name,),
        )
        assert len(result.data) == 0

    def test_table_in_custom_schema(self, postgres_backend_single):
        schema_name = "test_schema_tbl"
        table_name = "test_users"
        # Ensure clean state
        postgres_backend_single.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        # Create schema and table
        postgres_backend_single.execute(f'CREATE SCHEMA "{schema_name}"')
        try:
            postgres_backend_single.execute(
                f'CREATE TABLE "{schema_name}"."{table_name}" '
                f'(id SERIAL PRIMARY KEY, name VARCHAR(100))'
            )
            # Insert data
            postgres_backend_single.execute(
                f'INSERT INTO "{schema_name}"."{table_name}" (name) VALUES (%s)',
                ("Alice",),
            )
            # Select data
            result = postgres_backend_single.execute(
                f'SELECT * FROM "{schema_name}"."{table_name}"'
            )
            assert len(result.data) == 1
            assert result.data[0]["name"] == "Alice"
        finally:
            postgres_backend_single.execute(
                f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
            )

    def test_introspection_with_schema(self, postgres_backend_single):
        schema_name = "test_schema_intro"
        table_name = "test_items"
        # Ensure clean state
        postgres_backend_single.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        # Create schema and table
        postgres_backend_single.execute(f'CREATE SCHEMA "{schema_name}"')
        try:
            postgres_backend_single.execute(
                f'CREATE TABLE "{schema_name}"."{table_name}" '
                f'(id SERIAL PRIMARY KEY, value INTEGER)'
            )
            # Introspect tables in the custom schema
            result = postgres_backend_single.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (schema_name, table_name),
            )
            assert len(result.data) == 1
            assert result.data[0]["table_name"] == table_name
            # Introspect columns in the custom schema
            result = postgres_backend_single.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema_name, table_name),
            )
            column_names = [row["column_name"] for row in result.data]
            assert "id" in column_names
            assert "value" in column_names
        finally:
            postgres_backend_single.execute(
                f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
            )


# ============================================================
# Async Backend Layer Tests
# ============================================================


class TestSchemaNamespaceAsync:
    """Test schema namespace operations with AsyncPostgresBackend."""

    @pytest.mark.asyncio
    async def test_async_get_default_schema_is_public(self, async_postgres_backend_single):
        assert async_postgres_backend_single.get_default_schema() == "public"

    @pytest.mark.asyncio
    async def test_async_create_and_drop_schema(self, async_postgres_backend_single):
        schema_name = "test_schema_async"
        # Ensure clean state
        await async_postgres_backend_single.execute(
            f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
        )
        # Create schema
        await async_postgres_backend_single.execute(f'CREATE SCHEMA "{schema_name}"')
        # Verify schema exists
        result = await async_postgres_backend_single.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = %s",
            (schema_name,),
        )
        assert len(result.data) > 0
        # Drop schema
        await async_postgres_backend_single.execute(f'DROP SCHEMA "{schema_name}"')
        # Verify schema is gone
        result = await async_postgres_backend_single.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = %s",
            (schema_name,),
        )
        assert len(result.data) == 0

    @pytest.mark.asyncio
    async def test_async_table_in_custom_schema(self, async_postgres_backend_single):
        schema_name = "test_schema_async_tbl"
        table_name = "test_products"
        # Ensure clean state
        await async_postgres_backend_single.execute(
            f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
        )
        # Create schema and table
        await async_postgres_backend_single.execute(f'CREATE SCHEMA "{schema_name}"')
        try:
            await async_postgres_backend_single.execute(
                f'CREATE TABLE "{schema_name}"."{table_name}" '
                f'(id SERIAL PRIMARY KEY, name VARCHAR(100))'
            )
            # Insert data
            await async_postgres_backend_single.execute(
                f'INSERT INTO "{schema_name}"."{table_name}" (name) VALUES (%s)',
                ("Widget",),
            )
            # Select data
            result = await async_postgres_backend_single.execute(
                f'SELECT * FROM "{schema_name}"."{table_name}"'
            )
            assert len(result.data) == 1
            assert result.data[0]["name"] == "Widget"
        finally:
            await async_postgres_backend_single.execute(
                f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
            )

    @pytest.mark.asyncio
    async def test_async_introspection_with_schema(self, async_postgres_backend_single):
        schema_name = "test_schema_async_intro"
        table_name = "test_orders"
        # Ensure clean state
        await async_postgres_backend_single.execute(
            f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
        )
        # Create schema and table
        await async_postgres_backend_single.execute(f'CREATE SCHEMA "{schema_name}"')
        try:
            await async_postgres_backend_single.execute(
                f'CREATE TABLE "{schema_name}"."{table_name}" '
                f'(id SERIAL PRIMARY KEY, total DECIMAL(10,2))'
            )
            # Introspect tables in the custom schema
            result = await async_postgres_backend_single.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (schema_name, table_name),
            )
            assert len(result.data) == 1
        finally:
            await async_postgres_backend_single.execute(
                f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'
            )
