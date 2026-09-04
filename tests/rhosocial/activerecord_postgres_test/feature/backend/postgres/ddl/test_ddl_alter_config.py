# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/ddl/test_ddl_alter_config.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""

import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.statements import (
    AddColumn,
    AlterColumn,
    ColumnDefinition,
    DropColumn,
    DropTableConstraint,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.exceptions import (
    UnsupportedFeatureError,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresVacuumExpression,  # noqa: F401
    PostgresAnalyzeExpression,  # noqa: F401
    LoggingMode,
    RlsConfigurationMode,
    PostgresAlterTableRlsExpression,
    PostgresForceRlsExpression,
    PostgresAlterTableSettingsExpression,
    PostgresClusterExpression,
)
from rhosocial.activerecord.backend.expression.types import (
    TextType,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnConstraint, ColumnConstraintType,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.dml.extended_statistics import (
    PostgresExtendedStatisticsMixin,  # noqa: F401
)

class TestPostgresRlsConfigExpression:
    """Test RLS enable/disable/always/force DDL expressions."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_enable(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.ENABLE
        ).to_sql()
        assert sql == 'ALTER TABLE "orders" ENABLE ROW LEVEL SECURITY'

    def test_disable(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.DISABLE
        ).to_sql()
        assert sql == 'ALTER TABLE "orders" DISABLE ROW LEVEL SECURITY'

    def test_enable_always(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.ENABLE, always=True
        ).to_sql()
        assert sql == 'ALTER TABLE "orders" ENABLE ALWAYS ROW LEVEL SECURITY'

    def test_schema_qualify(self, dialect):
        sql, _ = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.ENABLE, schema="app"
        ).to_sql()
        assert sql == 'ALTER TABLE "app"."orders" ENABLE ROW LEVEL SECURITY'

    def test_force(self, dialect):
        sql, _ = PostgresForceRlsExpression(dialect, "orders").to_sql()
        assert sql == 'ALTER TABLE "orders" FORCE ROW LEVEL SECURITY'

    def test_no_force(self, dialect):
        sql, _ = PostgresForceRlsExpression(dialect, "orders", force=False).to_sql()
        assert sql == 'ALTER TABLE "orders" NO FORCE ROW LEVEL SECURITY'

    def test_version_gate_94(self):
        d = PostgresDialect(version=(9, 4, 0))
        expr = PostgresAlterTableRlsExpression(d, "orders", RlsConfigurationMode.ENABLE)
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()

    def test_disable_with_always_raises(self, dialect):
        expr = PostgresAlterTableRlsExpression(
            dialect, "orders", RlsConfigurationMode.DISABLE, always=True
        )
        with pytest.raises(ValueError, match="ALWAYS"):
            expr.to_sql()

    def test_force_version_gate_94(self):
        d = PostgresDialect(version=(9, 4, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresForceRlsExpression(d, "orders").to_sql()


class TestPostgresAlterTableSettingsExpression:
    """SET LOGGED / UNLOGGED / ACCESS METHOD DDL."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(15, 0, 0))

    def test_unlogged(self, dialect):
        sql, _ = PostgresAlterTableSettingsExpression(
            dialect, "t", mode=LoggingMode.UNLOGGED
        ).to_sql()
        assert sql == 'ALTER TABLE "t" SET UNLOGGED'

    def test_logged_schema(self, dialect):
        sql, _ = PostgresAlterTableSettingsExpression(
            dialect, "t", schema="s", mode=LoggingMode.LOGGED
        ).to_sql()
        assert sql == 'ALTER TABLE "s"."t" SET LOGGED'

    def test_access_method(self, dialect):
        sql, _ = PostgresAlterTableSettingsExpression(
            dialect, "t", access_method="heap"
        ).to_sql()
        assert sql == 'ALTER TABLE "t" SET ACCESS METHOD "heap"'

    def test_access_method_requires_15(self):
        d = PostgresDialect(version=(14, 0, 0))
        expr = PostgresAlterTableSettingsExpression(d, "t", access_method="heap")
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()

    def test_no_clause_raises(self, dialect):
        expr = PostgresAlterTableSettingsExpression(dialect, "t")
        with pytest.raises(ValueError):
            expr.to_sql()

    def test_both_clauses_raises(self, dialect):
        expr = PostgresAlterTableSettingsExpression(
            dialect, "t", mode=LoggingMode.UNLOGGED, access_method="heap"
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_logging_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        expr = PostgresAlterTableSettingsExpression(
            d, "t", mode=LoggingMode.LOGGED
        )
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()


class TestPostgresClusterExpression:
    """CLUSTER DDL."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_full(self, dialect):
        sql, _ = PostgresClusterExpression(
            dialect, "orders", schema="public",
            using_index="orders_pkey", verbose=True,
        ).to_sql()
        assert sql == 'CLUSTER VERBOSE "public"."orders" USING "orders_pkey"'

    def test_bare(self, dialect):
        sql, _ = PostgresClusterExpression(dialect, verbose=True).to_sql()
        assert sql == "CLUSTER VERBOSE"

    def test_version_gate_95(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresClusterExpression(d, "t").to_sql()

    def test_without_verbose(self, dialect):
        sql, _ = PostgresClusterExpression(
            dialect, "orders", using_index="orders_pkey"
        ).to_sql()
        assert sql == 'CLUSTER "orders" USING "orders_pkey"'

    def test_without_index(self, dialect):
        sql, _ = PostgresClusterExpression(
            dialect, "orders", verbose=True
        ).to_sql()
        assert sql == 'CLUSTER VERBOSE "orders"'

    def test_verbose_false_schema(self, dialect):
        sql, _ = PostgresClusterExpression(
            dialect, "orders", schema="app", verbose=False
        ).to_sql()
        assert sql == 'CLUSTER "app"."orders"'


class TestPostgresAlterColumnUsingExpression:
    """ALTER COLUMN ... SET DATA TYPE ... USING."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_using_clause(self, dialect):
        action = AlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
            dialect_options={"using": Column(dialect, "price") + Literal(dialect, 1)},
        )
        sql, serialized = dialect.format_alter_column_action(action)
        assert 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)' in sql
        assert 'USING ("price" + %s)' in sql
        assert serialized == (1,)

    def test_using_clause_with_cascade(self, dialect):
        action = AlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
            cascade=True,
            dialect_options={"using": Column(dialect, "price")},
        )
        sql, serialized = dialect.format_alter_column_action(action)
        assert 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)' in sql
        assert 'USING ("price")' in sql
        assert sql.endswith(" CASCADE")

    def test_using_rejected_for_non_set_data_type(self, dialect):
        """USING is only valid on SET DATA TYPE."""
        action = AlterColumn(
            dialect,
            "price",
            "SET DEFAULT",
            new_value="0",
            dialect_options={"using": Column(dialect, "price")},
        )
        with pytest.raises(ValueError, match="USING"):
            dialect.format_alter_column_action(action)

    def test_without_using_unaffected(self, dialect):
        """No USING -> output matches the standard form."""
        action = AlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
        )
        sql, _ = dialect.format_alter_column_action(action)
        assert sql == 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)'


class TestPostgresAlterColumnModifierMixin:
    """ADD/DROP COLUMN and DROP CONSTRAINT IF [NOT] EXISTS qualifiers."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_capabilities(self, dialect):
        assert dialect.supports_add_column_if_not_exists() is True
        assert dialect.supports_drop_column_if_exists() is True
        assert dialect.supports_drop_constraint_if_exists() is True

    def test_add_column(self, dialect):
        column = ColumnDefinition(
            "email", TextType(),
            constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)],
        )
        sql, params = dialect.format_add_column_action(
            AddColumn(dialect, column)
        )
        assert sql == 'ADD COLUMN "email" TEXT NOT NULL'
        assert params == ()

    def test_add_column_if_not_exists(self, dialect):
        column = ColumnDefinition("email", TextType())
        sql, _ = dialect.format_add_column_action(
            AddColumn(dialect, column, if_not_exists=True)
        )
        assert sql == 'ADD COLUMN IF NOT EXISTS "email" TEXT'

    def test_drop_column(self, dialect):
        sql, _ = dialect.format_drop_column_action(
            DropColumn(dialect, "email")
        )
        assert sql == 'DROP COLUMN "email"'

    def test_drop_column_if_exists(self, dialect):
        sql, _ = dialect.format_drop_column_action(
            DropColumn(dialect, "email", if_exists=True)
        )
        assert sql == 'DROP COLUMN IF EXISTS "email"'

    def test_drop_constraint(self, dialect):
        sql, _ = dialect.format_drop_table_constraint_action(
            DropTableConstraint(dialect, "user_email_key")
        )
        assert sql == 'DROP CONSTRAINT "user_email_key"'

    def test_drop_constraint_if_exists(self, dialect):
        sql, _ = dialect.format_drop_table_constraint_action(
            DropTableConstraint(dialect, "user_email_key", if_exists=True)
        )
        assert sql == 'DROP CONSTRAINT IF EXISTS "user_email_key"'

    def test_drop_constraint_if_exists_cascade(self, dialect):
        sql, _ = dialect.format_drop_table_constraint_action(
            DropTableConstraint(
                dialect, "user_email_key", if_exists=True, cascade=True
            )
        )
        assert sql == 'DROP CONSTRAINT IF EXISTS "user_email_key" CASCADE'


class TestPostgresConstraintCapabilities:
    """PostgreSQL-only constraint capability switches."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_capabilities(self, dialect):
        assert dialect.supports_constraint_novalidate() is True
        assert dialect.supports_exclude_constraint() is True
        assert dialect.supports_drop_constraint_if_exists() is True


class TestPostgresTypeDDL:
    """CREATE/DROP TYPE capability switches and formatting."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_capabilities(self, dialect):
        assert dialect.supports_create_type() is True
        assert dialect.supports_drop_type() is True
        assert dialect.supports_type_if_not_exists() is False
        assert dialect.supports_type_if_exists() is True
        assert dialect.supports_type_cascade() is True

    def test_create_enum(self, dialect):
        sql, params = dialect.format_create_type_enum_statement(
            "color", ["red", "blue"]
        )
        assert sql == 'CREATE TYPE "color" AS ENUM (\'red\', \'blue\')'
        assert params == ()

    def test_create_enum_with_schema(self, dialect):
        sql, _ = dialect.format_create_type_enum_statement(
            "color", ["red"], schema="app"
        )
        assert sql == 'CREATE TYPE "app"."color" AS ENUM (\'red\')'

    def test_create_enum_empty_values_raises(self, dialect):
        with pytest.raises(ValueError, match="at least one value"):
            dialect.format_create_type_enum_statement("color", [])

    def test_drop_type(self, dialect):
        sql, _ = dialect.format_drop_type_statement("color")
        assert sql == 'DROP TYPE "color"'

    def test_drop_type_full(self, dialect):
        sql, _ = dialect.format_drop_type_statement(
            "color", schema="app", if_exists=True, cascade=True
        )
        assert sql == 'DROP TYPE IF EXISTS "app"."color" CASCADE'

