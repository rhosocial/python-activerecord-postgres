# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_expressions.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""

import pytest

from rhosocial.activerecord.backend.expression import (
    BinaryArithmeticExpression,
    Column,
    ComparisonPredicate,
    Literal,
)
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
    ColumnConstraint, ColumnNotNullConstraint,
)
from rhosocial.activerecord.backend.expression.objects import Table
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
            using=BinaryArithmeticExpression(dialect, "+", Column(dialect, "price"), Literal(dialect, 1)),
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
            constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
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


class TestPostgresConstraintDeferral:
    """``DEFERRABLE`` / ``INITIALLY`` is not an all-kinds grant.

    The ``column_constraint`` / ``table_constraint`` grammar figures draw
    ``[DEFERRABLE ...]`` as a tail property shared by every branch; the
    server contradicts that for CHECK and NOT NULL.  Values below are the
    measured accept surface of PostgreSQL 19beta4
    (``192.168.1.3:16690/test_db``), per "backend facts win over
    documentation".  SQLSTATEs are recorded on each switch's docstring.
    """

    def test_deferrable_is_not_all_kinds(self):
        dialect = PostgresDialect((19, 0, 0))
        # PostgreSQL does support DEFERRABLE -- but not for every kind.
        assert dialect.supports_deferrable_constraint() is True
        assert dialect.supports_deferrable_primary_key_constraint() is True
        assert dialect.supports_deferrable_unique_constraint() is True
        assert dialect.supports_deferrable_foreign_key_constraint() is True
        assert dialect.supports_deferrable_exclude_constraint() is True
        # The two kinds the grammar figure wrongly implies:
        # CHECK at both levels, NOT NULL at column level.
        assert dialect.supports_deferrable_check_constraint() is False
        assert dialect.supports_deferrable_not_null_constraint() is False

    def test_enable_disable_constraint_is_not_postgresql_syntax(self):
        dialect = PostgresDialect((19, 0, 0))
        assert dialect.supports_enable_disable_constraint() is False

    def test_validate_constraint_is_accepted_including_not_valid(self):
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_validate_constraint() is True

    def test_enforced_is_check_and_fk_only(self):
        """The probe is a version gate; the kinds are the formatter's narrowing.

        Measured on 19beta4: CHECK and FOREIGN KEY render ``[NOT] ENFORCED``;
        PRIMARY KEY and UNIQUE come back with ``0A000: PRIMARY KEY/UNIQUE
        constraints cannot be marked NOT ENFORCED``.  The probe answers only
        "is the clause in the grammar", so the per-kind answer lives where the
        clause is rendered -- ``format_table_constraint`` ->
        ``_require_constraint_enforced``.
        """
        from rhosocial.activerecord.backend.expression import (
            Column,
            ComparisonPredicate,
            Literal,
            TableCheckConstraint,
            TableConstraint,
            TableForeignKeyConstraint,
            TablePrimaryKeyConstraint,
            TableUniqueConstraint,
        )
        from rhosocial.activerecord.backend.expression.objects import Table

        for version, expected in (((17, 0, 0), False), ((18, 0, 0), True), ((19, 0, 0), True)):
            assert PostgresDialect(version).supports_constraint_enforced() is expected

        dialect = PostgresDialect((19, 0, 0))
        condition = ComparisonPredicate(dialect, ">", Column(dialect, "id"), Literal(dialect, 0, inline_literals=True))
        check = TableConstraint(dialect, branch=TableCheckConstraint(dialect, condition), not_enforced=True)
        fk = TableConstraint(
            dialect,
            branch=TableForeignKeyConstraint(dialect, ["parent_id"], (Table(dialect, "people"), ["id"])),
            not_enforced=True,
        )
        assert "NOT ENFORCED" in check.to_sql()[0]
        assert "NOT ENFORCED" in fk.to_sql()[0]
        for branch in (
            TablePrimaryKeyConstraint(dialect, ["id"]),
            TableUniqueConstraint(dialect, ["id"]),
        ):
            with pytest.raises(UnsupportedFeatureError, match="NOT ENFORCED"):
                TableConstraint(dialect, branch=branch, not_enforced=True).to_sql()

    def test_alter_constraint_enforced_splits_by_kind_and_version(self):
        """The ALTER path splits where the ADD path does not.

        Measured: ``ADD CONSTRAINT ... NOT ENFORCED`` works for a CHECK from
        18 on, while ``ALTER CONSTRAINT ... NOT ENFORCED`` on the same CHECK
        is refused with ``42809`` on 180006 and accepted on 190000.  So
        FOREIGN KEY is PG18 and CHECK is PG19 on this statement only.
        """
        from rhosocial.activerecord.backend.expression.statements import (
            TableCheckConstraint,
            TableForeignKeyConstraint,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
            PostgresAlterConstraint,
        )

        for version, fk_ok, check_ok in (
            ((17, 0, 0), False, False),
            ((18, 0, 0), True, False),
            ((19, 0, 0), True, True),
        ):
            dialect = PostgresDialect(version)
            assert dialect.supports_alter_constraint_enforced() is (version >= (18, 0, 0))
            for branch, expected in ((TableForeignKeyConstraint, fk_ok), (TableCheckConstraint, check_ok)):
                action = PostgresAlterConstraint(
                    dialect, "c", not_enforced=True, constraint_kind=branch
                )
                if expected:
                    assert action.to_sql()[0] == 'ALTER CONSTRAINT "c" NOT ENFORCED'
                else:
                    with pytest.raises(UnsupportedFeatureError):
                        action.to_sql()


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
        """The two enforcement probes are grammar gates with no kind in them.

        Measured: ``NOT ENFORCED`` enters the grammar in 18 on both statements,
        so ``supports_alter_constraint_enforced()`` is ``True`` on 18 as well
        -- the kind split on that statement (FOREIGN KEY 18, CHECK 19) belongs
        to ``_require_enforceable_constraint``, not to the probe.
        """
        assert PostgresDialect((17, 0, 0)).supports_constraint_enforced() is False
        assert PostgresDialect((18, 0, 0)).supports_constraint_enforced() is True
        assert PostgresDialect((17, 0, 0)).supports_alter_constraint_enforced() is False
        assert PostgresDialect((18, 0, 0)).supports_alter_constraint_enforced() is True
        assert PostgresDialect((19, 0, 0)).supports_alter_constraint_enforced() is True
        assert PostgresDialect((14, 0, 0)).supports_validate_constraint() is True

    def test_create_and_add_check_fk_enforcement(self):
        from rhosocial.activerecord.backend.expression import (
            AddTableConstraint,
            Column,
            ColumnCheckConstraint,
            ColumnConstraint,
            ColumnDefinition,
            ComparisonPredicate,
            ForeignKeyConstraint,
            Literal,
            TableCheckConstraint,
            TableConstraint,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType

        dialect = PostgresDialect((18, 0, 0))
        condition = ComparisonPredicate(dialect, ">", Column(dialect, "age"), Literal(dialect, 0, inline_literals=True))
        check = TableConstraint(
            dialect,
            branch=TableCheckConstraint(dialect, condition),
            name='age_check',
            not_enforced=True,
        )
        fk = ForeignKeyConstraint(
            dialect,
            columns=["parent_id"],
            foreign_key_table=Table(dialect, 'people'),
            foreign_key_columns=["id"],
            enforced=True,
        )
        column_check = ColumnConstraint(dialect, branch=ColumnCheckConstraint(dialect, condition), not_enforced=True)

        assert check.to_sql()[0] == 'CONSTRAINT "age_check" CHECK ("age" > 0) NOT ENFORCED'
        assert fk.to_sql()[0] == (
            'FOREIGN KEY ("parent_id") REFERENCES "people"("id") ENFORCED'
        )
        # Measured: the column-level clause is a parse error for every kind on
        # every version -- ``42601: misplaced NOT ENFORCED clause`` -- so the
        # column form refuses rather than rendering.
        with pytest.raises(UnsupportedFeatureError, match="CHECK NOT ENFORCED on a column constraint"):
            ColumnDefinition(dialect, "age", IntegerType(dialect), [column_check]).to_sql()
        assert AddTableConstraint(dialect, check).to_sql()[0] == (
            'ADD CONSTRAINT "age_check" CHECK ("age" > 0) NOT ENFORCED'
        )

    def test_not_valid_is_limited_to_check_and_fk(self):
        from rhosocial.activerecord.backend.expression import AddTableConstraint
        from rhosocial.activerecord.backend.expression.statements import (
            ConstraintValidation,
            ForeignKeyConstraint,
            TableCheckConstraint,
            TableConstraint,
            TablePrimaryKeyConstraint,
            TableUniqueConstraint,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import PostgresExcludeConstraint

        dialect = PostgresDialect((18, 0, 0))
        constraint = ForeignKeyConstraint(
            dialect,
            columns=["parent_id"],
            foreign_key_table=Table(dialect, 'people'),
            foreign_key_columns=["id"],
            validation=ConstraintValidation.NOVALIDATE,
        )
        assert AddTableConstraint(dialect, constraint).to_sql()[0].endswith("NOT VALID")
        # The kind is a branch class now, and ``validation`` is a
        # ``ConstraintValidation`` member: a spelling is a type question and is
        # answered with a TypeError, at construction rather than at render.
        with pytest.raises(TypeError):
            TableConstraint(
                dialect,
                " check ",
                check_condition=ComparisonPredicate(
                    dialect, ">", Column(dialect, "age"), Literal(dialect, 0, inline_literals=True)
                ),
                validation=" not valid ",
            )
        with pytest.raises(TypeError):
            AddTableConstraint(
                dialect,
                TableConstraint(
                    dialect,
                    branch=TableCheckConstraint(
                        dialect,
                        ComparisonPredicate(
                            dialect, ">", Column(dialect, "age"), Literal(dialect, 0, inline_literals=True)
                        ),
                    ),
                    validation=" not valid ",
                ),
            ).to_sql()
        for branch in (TablePrimaryKeyConstraint, TableUniqueConstraint):
            with pytest.raises(ValueError, match="NOT VALID"):
                AddTableConstraint(
                    dialect,
                    TableConstraint(
                        dialect,
                        branch=branch(dialect, ["id"]),
                        validation=ConstraintValidation.NOVALIDATE,
                    ),
                ).to_sql()
        exclude = PostgresExcludeConstraint(
            dialect,
            elements=[("range", "&&")],
            validation=ConstraintValidation.NOVALIDATE,
        )
        with pytest.raises(ValueError, match="NOT VALID"):
            AddTableConstraint(dialect, exclude).to_sql()

    def test_alter_and_validate_actions(self):
        from rhosocial.activerecord.backend.expression.statements import (
            TableCheckConstraint,
            TableForeignKeyConstraint,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
            PostgresAlterConstraint,
            PostgresValidateConstraint,
        )

        pg18 = PostgresDialect((18, 0, 0))
        fk_action = PostgresAlterConstraint(
            pg18,
            "parent_fk",
            not_enforced=True,
            constraint_kind=TableForeignKeyConstraint,
        )
        assert fk_action.constraint_branch is TableForeignKeyConstraint
        assert fk_action.to_sql()[0] == (
            'ALTER CONSTRAINT "parent_fk" NOT ENFORCED'
        )
        # Measured: CHECK is PG19 on the ALTER path (180006 answers 42809).
        with pytest.raises(UnsupportedFeatureError):
            PostgresAlterConstraint(
                pg18,
                "age_check",
                not_enforced=True,
                constraint_kind=TableCheckConstraint,
            ).to_sql()
        assert PostgresValidateConstraint(pg18, "age_check").to_sql()[0] == (
            'VALIDATE CONSTRAINT "age_check"'
        )

        pg19 = PostgresDialect((19, 0, 0))
        assert PostgresAlterConstraint(
            pg19,
            "age_check",
            not_enforced=True,
            constraint_kind=TableCheckConstraint,
        ).to_sql()[0] == 'ALTER CONSTRAINT "age_check" NOT ENFORCED'
        with pytest.raises(TypeError):
            PostgresAlterConstraint(pg19, "age_check", not_enforced=True)

    def test_exclude_create_and_add(self):
        from rhosocial.activerecord.backend.expression import (
            AddTableConstraint,
            Column,
            ColumnDefinition,
            CreateTableStatement,
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
        create = CreateTableStatement(
            dialect,
            Table(dialect, 'ranges'),
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
            TableCheckConstraint,
            TableConstraint,
        )

        not_valid_check = TableConstraint(
            dialect,
            branch=TableCheckConstraint(
                dialect,
                ComparisonPredicate(
                    dialect, ">", Column(dialect, "id"), Literal(dialect, 0, inline_literals=True)
                ),
            ),
            validation=ConstraintValidation.NOVALIDATE,
        )
        invalid_create = CreateTableStatement(
            dialect,
            Table(dialect, 'invalid_checks'),
            [ColumnDefinition(dialect, "id", IntegerType(dialect))],
            table_constraints=[not_valid_check],
        )
        with pytest.raises(ValueError, match="only valid when adding"):
            invalid_create.to_sql()

    def test_exclude_expression_parentheses_and_recursive_binding(self):
        from rhosocial.activerecord.backend.expression import (
            Column,
            ComparisonPredicate,
            FunctionCall,
            Literal,
            RawSQLExpression,
            RawSQLPredicate,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
            PostgresExcludeConstraint,
        )
        dialect = PostgresDialect((18, 0, 0))
        declared = PostgresExcludeConstraint(
            dialect,
            elements=[
                (
                    FunctionCall(dialect, "LOWER", Column(dialect, "name")),
                    "=",
                )
            ],
            where=ComparisonPredicate(
                dialect, "=", Column(dialect, "active"), Literal(dialect, True, inline_literals=True)
            ),
        )

        assert declared.to_sql() == (
            'EXCLUDE USING gist ((LOWER("name")) WITH =) '
            'WHERE ("active" = TRUE)',
            (),
        )

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

