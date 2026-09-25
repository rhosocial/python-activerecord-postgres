# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_expressions.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""

import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.expression.statements import (
    AddColumn,
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
    PostgresAlterColumn,
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
        action = PostgresAlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
            using=Column(dialect, "price") + Literal(dialect, 1),
        )
        sql, serialized = action.to_sql()
        assert 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)' in sql
        assert 'USING ("price" + %s)' in sql
        assert serialized == (1,)

    def test_using_clause_with_cascade(self, dialect):
        action = PostgresAlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
            cascade=True,
            using=Column(dialect, "price"),
        )
        sql, serialized = action.to_sql()
        assert 'ALTER COLUMN "price" SET DATA TYPE NUMERIC(10,2)' in sql
        assert 'USING ("price")' in sql
        assert sql.endswith(" CASCADE")

    def test_using_rejected_for_non_set_data_type(self, dialect):
        """USING is only valid on SET DATA TYPE."""
        action = PostgresAlterColumn(
            dialect,
            "price",
            "SET DEFAULT",
            new_value="0",
            using=Column(dialect, "price"),
        )
        with pytest.raises(ValueError, match="USING"):
            action.to_sql()

    def test_without_using_unaffected(self, dialect):
        """No USING -> output matches the standard form."""
        action = PostgresAlterColumn(
            dialect,
            "price",
            "SET DATA TYPE",
            new_value="NUMERIC(10,2)",
        )
        sql, _ = action.to_sql()
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
            dialect,
            "email", TextType(dialect),
            constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)],
        )
        add = AddColumn(dialect, column)
        sql, params = add.to_sql()
        assert sql == 'ADD COLUMN "email" TEXT NOT NULL'
        assert params == ()

    def test_add_column_if_not_exists(self, dialect):
        column = ColumnDefinition(dialect, "email", TextType(dialect))
        add = AddColumn(dialect, column, if_not_exists=True)
        sql, _ = add.to_sql()
        assert sql == 'ADD COLUMN IF NOT EXISTS "email" TEXT'

    def test_drop_column(self, dialect):
        drop = DropColumn(dialect, "email")
        sql, _ = drop.to_sql()
        assert sql == 'DROP COLUMN "email"'

    def test_drop_column_if_exists(self, dialect):
        drop = DropColumn(dialect, "email", if_exists=True)
        sql, _ = drop.to_sql()
        assert sql == 'DROP COLUMN IF EXISTS "email"'

    def test_drop_constraint(self, dialect):
        drop = DropTableConstraint(dialect, "user_email_key")
        sql, _ = drop.to_sql()
        assert sql == 'DROP CONSTRAINT "user_email_key"'

    def test_drop_constraint_if_exists(self, dialect):
        drop = DropTableConstraint(dialect, "user_email_key", if_exists=True)
        sql, _ = drop.to_sql()
        assert sql == 'DROP CONSTRAINT IF EXISTS "user_email_key"'

    def test_drop_constraint_if_exists_cascade(self, dialect):
        drop = DropTableConstraint(
            dialect, "user_email_key", if_exists=True, cascade=True
        )
        sql, _ = drop.to_sql()
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

    def test_create_enum_empty_values(self, dialect):
        sql, params = dialect.format_create_type_enum_statement("color", [])
        assert sql == 'CREATE TYPE "color" AS ENUM ()'
        assert params == ()

    def test_drop_type(self, dialect):
        sql, _ = dialect.format_drop_type_statement("color")
        assert sql == 'DROP TYPE "color"'

    def test_drop_type_full(self, dialect):
        sql, _ = dialect.format_drop_type_statement(
            "color", schema="app", if_exists=True, cascade=True
        )
        assert sql == 'DROP TYPE IF EXISTS "app"."color" CASCADE'


class TestPostgresConstraintEnforcement:
    def test_capability_versions(self):
        from rhosocial.activerecord.backend.expression import TableConstraintType

        assert PostgresDialect((17, 0, 0)).supports_constraint_enforced() is False
        assert PostgresDialect((18, 0, 0)).supports_constraint_enforced() is True
        pg18 = PostgresDialect((18, 0, 0))
        assert pg18.supports_alter_constraint_enforced() is False
        assert pg18.supports_alter_constraint_enforced(TableConstraintType.FOREIGN_KEY) is True
        assert pg18.supports_alter_constraint_enforced(TableConstraintType.CHECK) is False
        pg19 = PostgresDialect((19, 0, 0))
        assert pg19.supports_alter_constraint_enforced(TableConstraintType.FOREIGN_KEY) is True
        assert pg19.supports_alter_constraint_enforced(TableConstraintType.CHECK) is True
        assert PostgresDialect((14, 0, 0)).supports_validate_constraint() is True

    def test_create_and_add_check_fk_enforcement(self):
        from rhosocial.activerecord.backend.expression import (
            AddTableConstraint,
            Column,
            ColumnConstraint,
            ColumnConstraintType,
            ColumnDefinition,
            ForeignKeyConstraint,
            Literal,
            TableConstraint,
            TableConstraintType,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType

        dialect = PostgresDialect((18, 0, 0))
        condition = Column(dialect, "age") > Literal(dialect, 0, inline_literals=True)
        check = TableConstraint(
            dialect,
            TableConstraintType.CHECK,
            name="age_check",
            check_condition=condition,
            enforced=False,
        )
        fk = ForeignKeyConstraint(
            dialect,
            columns=["parent_id"],
            foreign_key_table="people",
            foreign_key_columns=["id"],
            enforced=True,
        )
        column_check = ColumnConstraint(
            dialect,
            ColumnConstraintType.CHECK,
            check_condition=condition,
            enforced=False,
        )

        assert check.to_sql()[0] == 'CONSTRAINT "age_check" CHECK ("age" > 0) NOT ENFORCED'
        assert fk.to_sql()[0] == (
            'FOREIGN KEY ("parent_id") REFERENCES "people"("id") ENFORCED'
        )
        assert ColumnDefinition(
            dialect, "age", IntegerType(dialect), [column_check]
        ).to_sql()[0] == '"age" INTEGER CHECK ("age" > 0) NOT ENFORCED'
        assert AddTableConstraint(dialect, check).to_sql()[0] == (
            'ADD CONSTRAINT "age_check" CHECK ("age" > 0) NOT ENFORCED'
        )

    def test_not_valid_is_limited_to_check_and_fk(self):
        from rhosocial.activerecord.backend.expression import AddTableConstraint
        from rhosocial.activerecord.backend.expression.statements import (
            ConstraintValidation,
            ForeignKeyConstraint,
            TableConstraint,
            TableConstraintType,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import PostgresExcludeConstraint

        dialect = PostgresDialect((18, 0, 0))
        constraint = ForeignKeyConstraint(
            dialect,
            columns=["parent_id"],
            foreign_key_table="people",
            foreign_key_columns=["id"],
            validation=ConstraintValidation.NOVALIDATE,
        )
        assert AddTableConstraint(dialect, constraint).to_sql()[0].endswith("NOT VALID")
        string_constraint = TableConstraint(
            dialect,
            " check ",
            check_condition=Column(dialect, "age") > Literal(dialect, 0, inline_literals=True),
            validation=" not valid ",
        )
        assert AddTableConstraint(dialect, string_constraint).to_sql()[0].endswith("NOT VALID")
        assert "NOT VALID" not in string_constraint.to_sql()[0]
        for constraint_type in (TableConstraintType.PRIMARY_KEY, TableConstraintType.UNIQUE):
            table_constraint = TableConstraint(
                dialect,
                constraint_type,
                columns=["id"],
                validation=ConstraintValidation.NOVALIDATE,
            )
            with pytest.raises(ValueError, match="NOT VALID"):
                AddTableConstraint(dialect, table_constraint).to_sql()
        exclude = PostgresExcludeConstraint(
            dialect,
            elements=[("range", "&&")],
            validation=ConstraintValidation.NOVALIDATE,
        )
        with pytest.raises(ValueError, match="NOT VALID"):
            AddTableConstraint(dialect, exclude).to_sql()

    def test_alter_and_validate_actions(self):
        from rhosocial.activerecord.backend.expression import (
            ColumnConstraintType,
            TableConstraintType,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
            PostgresAlterConstraint,
            PostgresValidateConstraint,
        )

        pg18 = PostgresDialect((18, 0, 0))
        fk_action = PostgresAlterConstraint(
            pg18,
            "parent_fk",
            False,
            constraint_type=ColumnConstraintType.FOREIGN_KEY,
        )
        assert fk_action.constraint_type is TableConstraintType.FOREIGN_KEY
        assert fk_action.to_sql()[0] == (
            'ALTER CONSTRAINT "parent_fk" NOT ENFORCED'
        )
        with pytest.raises(UnsupportedFeatureError):
            PostgresAlterConstraint(
                pg18,
                "age_check",
                False,
                constraint_type=TableConstraintType.CHECK,
            ).to_sql()
        assert PostgresValidateConstraint(pg18, "age_check").to_sql()[0] == (
            'VALIDATE CONSTRAINT "age_check"'
        )

        pg19 = PostgresDialect((19, 0, 0))
        assert PostgresAlterConstraint(
            pg19,
            "age_check",
            False,
            constraint_type=TableConstraintType.CHECK,
        ).to_sql()[0] == 'ALTER CONSTRAINT "age_check" NOT ENFORCED'
        with pytest.raises(TypeError):
            PostgresAlterConstraint(pg19, "age_check", False)

    def test_exclude_create_and_add(self):
        from rhosocial.activerecord.backend.expression import (
            AddTableConstraint,
            Column,
            ColumnDefinition,
            CreateTableExpression,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import PostgresExcludeConstraint

        dialect = PostgresDialect((18, 0, 0))
        with pytest.raises(ValueError, match="at least one element"):
            PostgresExcludeConstraint(dialect).to_sql()
        exclude = PostgresExcludeConstraint(
            dialect,
            name="range_ex",
            elements=[("range", "&&")],
        )
        create = CreateTableExpression(
            dialect,
            "ranges",
            [ColumnDefinition(dialect, "range", IntegerType(dialect))],
            table_constraints=[exclude],
        )
        assert create.to_sql()[0] == (
            'CREATE TABLE "ranges" ("range" INTEGER, CONSTRAINT "range_ex" '
            'EXCLUDE USING gist ("range" WITH &&))'
        )
        assert AddTableConstraint(dialect, exclude).to_sql()[0] == (
            'ADD CONSTRAINT "range_ex" EXCLUDE USING gist ("range" WITH &&)'
        )

        from rhosocial.activerecord.backend.expression.statements import (
            ConstraintValidation,
            TableConstraint,
            TableConstraintType,
        )

        not_valid_check = TableConstraint(
            dialect,
            TableConstraintType.CHECK,
            check_condition=Column(dialect, "id") > 0,
            validation=ConstraintValidation.NOVALIDATE,
        )
        invalid_create = CreateTableExpression(
            dialect,
            "invalid_checks",
            [ColumnDefinition(dialect, "id", IntegerType(dialect))],
            table_constraints=[not_valid_check],
        )
        with pytest.raises(ValueError, match="only valid when adding"):
            invalid_create.to_sql()

    def test_exclude_expression_parentheses_and_recursive_binding(self):
        from rhosocial.activerecord.backend.expression import (
            Column,
            FunctionCall,
            Literal,
            RawSQLExpression,
            RawSQLPredicate,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
            PostgresExcludeConstraint,
        )
        from rhosocial.activerecord.ddl import DialectBinder

        dialect = PostgresDialect((18, 0, 0))
        declared = PostgresExcludeConstraint(
            None,
            elements=[(FunctionCall(None, "LOWER", Column(None, "name")), "=")],
            where=Column(None, "active") == Literal(None, True, inline_literals=True),
        )
        bound = DialectBinder(dialect).bind(declared)

        assert bound.to_sql() == (
            'EXCLUDE USING gist ((LOWER("name")) WITH =) '
            'WHERE ("active" = TRUE)',
            (),
        )
        with pytest.raises(ValueError):
            declared.elements[0][0].to_sql()

        with pytest.raises(ValueError, match="Invalid exclude operator"):
            PostgresExcludeConstraint(
                dialect,
                elements=[("name", "is not")],
            ).to_sql()
        with pytest.raises(ValueError, match="parenthesized"):
            PostgresExcludeConstraint(
                dialect,
                elements=[("(lower(name))", "=")],
            ).to_sql()
        with pytest.raises(ValueError, match="unbalanced"):
            PostgresExcludeConstraint(
                dialect,
                elements=[(RawSQLExpression(dialect, "lower(name"), "=")],
            ).to_sql()
        with pytest.raises(ValueError, match="must not contain bind parameters"):
            PostgresExcludeConstraint(
                dialect,
                elements=[(RawSQLExpression(dialect, "lower(name)", ("x",)), "=")],
            ).to_sql()
        with pytest.raises(ValueError, match="must not contain bind parameters"):
            PostgresExcludeConstraint(
                dialect,
                elements=[("name", "=")],
                where=RawSQLPredicate(dialect, "active = %s", (True,)),
            ).to_sql()

