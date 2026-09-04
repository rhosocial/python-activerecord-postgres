# tests/rhosocial/activerecord_postgres_test/feature/backend/ddl/test_alter_table_if_exists.py
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
            dialect, table="users", actions=[action]
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


class TestPostgresAlterColumnTypeAndUsing:
    """SET DATA TYPE with the PostgreSQL ``USING`` conversion expression.

    Covered here per plan §6.3: the ALTER COLUMN subclauses added by the
    PostgreSQL DDL coverage-completion work must render through the same
    ALTER TABLE action pipeline as the IF [NOT] EXISTS qualifiers.
    """

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(15, 0, 0))

    def test_set_data_type_plain(self, dialect):
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
            AlterColumn,
        )

        action = AlterColumn(
            dialect, "price", "SET DATA TYPE", new_value="NUMERIC(10,2)"
        )
        sql, params = action.to_sql()
        assert 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)' == sql
        assert params == ()

    def test_set_data_type_with_using(self, dialect):
        from rhosocial.activerecord.backend.expression import Column, Literal
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
            AlterColumn,
        )

        action = AlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
            dialect_options={"using": Column(dialect, "price") + Literal(dialect, 1)},
        )
        sql, serialized = action.to_sql()
        assert 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)' in sql
        assert 'USING ("price" + %s)' in sql
        assert serialized == (1,)

    def test_set_data_type_using_rejected_for_other_ops(self, dialect):
        from rhosocial.activerecord.backend.expression import Column
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
            AlterColumn,
        )

        action = AlterColumn(
            dialect,
            "price",
            "SET DEFAULT",
            new_value="0",
            dialect_options={"using": Column(dialect, "price")},
        )
        with pytest.raises(ValueError, match="USING"):
            action.to_sql()


class TestPostgresRenameColumnAndTable:
    """RENAME COLUMN / RENAME TABLE render through the core actions."""

    def test_rename_column(self, dialect):
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
            RenameColumn,
        )

        sql, params = RenameColumn(
            dialect, old_name="id", new_name="order_id"
        ).to_sql()
        assert 'RENAME COLUMN "id" TO "order_id"' == sql
        assert params == ()

    def test_rename_column_inside_alter_table(self, dialect):
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
            RenameColumn,
        )

        expr = AlterTableExpression(
            dialect,
            table="orders",
            actions=[RenameColumn(dialect, old_name="id", new_name="order_id")],
        )
        sql, params = expr.to_sql()
        assert 'ALTER TABLE "orders"' in sql
        assert 'RENAME COLUMN "id" TO "order_id"' in sql
        assert params == ()

    def test_rename_table(self, dialect):
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
            RenameTable,
        )

        sql, params = RenameTable(
            dialect, old_name="orders", new_name="orders2"
        ).to_sql()
        assert 'RENAME TO "orders2"' == sql
        assert params == ()

    def test_capability_switches(self, dialect):
        assert dialect.supports_rename_column() is True
        assert dialect.supports_rename_table() is True


class TestPostgresCreateUnloggedTable:
    """CREATE UNLOGGED TABLE via dialect_options on CreateTableExpression."""

    def test_unlogged_renders_qualifier(self, dialect):
        from rhosocial.activerecord.backend.expression.statements.ddl_table import (
            CreateTableExpression,
        )

        expr = CreateTableExpression(
            dialect,
            table="audit",
            columns=[ColumnDefinition("id", TextType())],
            dialect_options={"unlogged_table": True},
        )
        sql, params = expr.to_sql()
        assert sql.startswith('CREATE UNLOGGED TABLE "audit"')
        assert params == ()

    def test_plain_omits_qualifier(self, dialect):
        from rhosocial.activerecord.backend.expression.statements.ddl_table import (
            CreateTableExpression,
        )

        expr = CreateTableExpression(
            dialect,
            table="audit",
            columns=[ColumnDefinition("id", TextType())],
        )
        sql, params = expr.to_sql()
        assert not sql.startswith("CREATE UNLOGGED")
        assert params == ()

    def test_temporary_wins_over_unlogged(self, dialect):
        from rhosocial.activerecord.backend.expression.statements.ddl_table import (
            CreateTableExpression,
        )

        expr = CreateTableExpression(
            dialect,
            table="audit",
            columns=[ColumnDefinition("id", TextType())],
            temporary=True,
            dialect_options={"unlogged_table": True},
        )
        sql, params = expr.to_sql()
        assert sql.startswith("CREATE TEMPORARY TABLE")
        assert params == ()

    def test_version_gate_94(self):
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )
        from rhosocial.activerecord.backend.expression.statements.ddl_table import (
            CreateTableExpression,
        )

        low = PostgresDialect(version=(9, 4, 0))
        expr = CreateTableExpression(
            low,
            table="audit",
            columns=[ColumnDefinition("id", TextType())],
            dialect_options={"unlogged_table": True},
        )
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()
