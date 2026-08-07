# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_alter_table_if_exists.py
"""Tests for ALTER TABLE IF [NOT] EXISTS qualifier rendering on PostgreSQL.

PostgreSQL supports the vendor extensions since 9.6:
  - ADD COLUMN IF NOT EXISTS
  - DROP COLUMN IF EXISTS
  - DROP CONSTRAINT IF EXISTS

The qualifiers are opt-in via ``if_not_exists`` / ``if_exists`` on the
``AddColumn`` / ``DropColumn`` / ``DropTableConstraint`` actions; ``None``
(the default) renders the plain SQL-standard form.
"""

import pytest

from rhosocial.activerecord.backend.expression.statements import ColumnDefinition
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddColumn,
    AlterTableExpression,
    DropColumn,
    DropTableConstraint,
)
from rhosocial.activerecord.backend.expression.types import TextType
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


@pytest.fixture
def dialect():
    return PostgresDialect(version=(15, 0, 0))


class TestPostgresAlterTableModifierCapabilities:
    def test_supports_switches(self, dialect):
        assert dialect.supports_add_column_if_not_exists() is True
        assert dialect.supports_drop_column_if_exists() is True
        assert dialect.supports_drop_constraint_if_exists() is True


class TestPostgresAddColumnIfNotExists:
    def test_if_not_exists_renders_qualifier(self, dialect):
        action = AddColumn(
            dialect,
            ColumnDefinition("content", TextType()),
            if_not_exists=True,
        )
        sql, params = action.to_sql()
        assert 'ADD COLUMN IF NOT EXISTS "content" TEXT' == sql
        assert params == ()

    def test_none_renders_plain_form(self, dialect):
        action = AddColumn(dialect, ColumnDefinition("content", TextType()))
        sql, params = action.to_sql()
        assert 'ADD COLUMN "content" TEXT' == sql
        assert "IF NOT EXISTS" not in sql
        assert params == ()

    def test_inside_alter_table(self, dialect):
        action = AddColumn(
            dialect,
            ColumnDefinition("content", TextType()),
            if_not_exists=True,
        )
        expr = AlterTableExpression(
            dialect, table_name="users", actions=[action]
        )
        sql, params = expr.to_sql()
        assert 'ALTER TABLE "users"' in sql
        assert 'ADD COLUMN IF NOT EXISTS "content" TEXT' in sql
        assert params == ()


class TestPostgresDropColumnIfExists:
    def test_if_exists_renders_qualifier(self, dialect):
        action = DropColumn(dialect, column_name="x", if_exists=True)
        sql, params = action.to_sql()
        assert 'DROP COLUMN IF EXISTS "x"' == sql
        assert params == ()

    def test_none_renders_plain_form(self, dialect):
        action = DropColumn(dialect, column_name="x")
        sql, params = action.to_sql()
        assert 'DROP COLUMN "x"' == sql
        assert "IF EXISTS" not in sql
        assert params == ()


class TestPostgresDropConstraintIfExists:
    def test_if_exists_renders_qualifier(self, dialect):
        action = DropTableConstraint(
            dialect, constraint_name="fkey", if_exists=True
        )
        sql, params = action.to_sql()
        assert 'DROP CONSTRAINT IF EXISTS "fkey"' == sql
        assert params == ()

    def test_if_exists_with_cascade(self, dialect):
        action = DropTableConstraint(
            dialect, constraint_name="fkey", if_exists=True, cascade=True
        )
        sql, params = action.to_sql()
        assert 'DROP CONSTRAINT IF EXISTS "fkey" CASCADE' == sql
        assert params == ()

    def test_none_renders_plain_form(self, dialect):
        action = DropTableConstraint(dialect, constraint_name="fkey")
        sql, params = action.to_sql()
        assert 'DROP CONSTRAINT "fkey"' == sql
        assert "IF EXISTS" not in sql
        assert params == ()
