# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_ddl_expressions.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.exceptions import (
    UnsupportedFeatureError,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.serialization import (
    deserialize,
    deserialize_json,
    deserialize_xml,
    serialize,
    serialize_json,
    serialize_xml,
)
from rhosocial.activerecord.backend.expression.statements.ddl_domain import (
    AlterDomainExpression,
    CreateDomainExpression,
    DomainCheckConstraint,
    DomainNullability,
    DomainValueExpression,
    DropDomainDefaultAction,
    DropDomainNotNullAction,
    RenameDomainAction,
    SetDomainDefaultAction,
    SetDomainNotNullAction,
)
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeExpression,
    CreateTypeExpression,
)
from rhosocial.activerecord.backend.expression.types import IntegerType, TextType
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresVacuumExpression,  # noqa: F401
    PostgresAnalyzeExpression,  # noqa: F401
    AlterDomainActionType,
    PostgresAddDomainCheckAction,
    PostgresAddEnumValueAction,
    PostgresAddTypeAttributeAction,
    PostgresAlterEnumAddValueExpression,
    PostgresCreateEnumTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresAlterTypeAttributeAction,
    PostgresBaseTypeDefinition,
    PostgresChangeDomainOwnerAction,
    PostgresChangeTypeOwnerAction,
    PostgresCompositeTypeAttribute,
    PostgresCompositeTypeDefinition,
    PostgresCreateDomainExpression,
    PostgresDropDomainCheckAction,
    PostgresDropTypeAttributeAction,
    PostgresDropTypeExpression,
    PostgresEnumTypeDefinition,
    PostgresRangeTypeDefinition,
    PostgresRenameDomainConstraintAction,
    PostgresRenameEnumValueAction,
    PostgresRenameTypeAction,
    PostgresRenameTypeAttributeAction,
    PostgresSetDomainSchemaAction,
    PostgresSetTypePropertiesAction,
    PostgresSetTypeSchemaAction,
    PostgresShellTypeDefinition,
    PostgresValidateDomainConstraintAction,
    PostgresAlterDomainExpression,
    PostgresDropDomainExpression,
    PostgresCreateCollationExpression,
    PostgresDropCollationExpression,
    PostgresCreateForeignTableExpression,
    PostgresDropForeignTableExpression,
    PostgresCreateFunctionExpression,
    PostgresDropFunctionExpression,
    PostgresCreateAggregateExpression,
    PostgresDropAggregateExpression,
    PostgresCreatePublicationExpression,
    PostgresDropPublicationExpression,
    PostgresCreateSubscriptionExpression,
    PostgresDropSubscriptionExpression,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.dml.extended_statistics import (
    PostgresExtendedStatisticsMixin,  # noqa: F401
)

class TestPostgresDomainExpression:
    """CREATE / ALTER / DROP DOMAIN."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateDomainExpression(
            dialect, "posint", "NUMERIC",
            default="0", constraints=["CHECK (VALUE > 0)"],
        ).to_sql()
        assert sql == "CREATE DOMAIN \"posint\" AS NUMERIC DEFAULT 0 CHECK (VALUE > 0)"

    def test_alter_set_default(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.SET_DEFAULT, new_value="0"
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" SET DEFAULT 0'

    def test_alter_rename(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.RENAME_TO, new_name="posint2"
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" RENAME TO "posint2"'

    def test_drop(self, dialect):
        sql, _ = PostgresDropDomainExpression(
            dialect, "posint", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP DOMAIN IF EXISTS "posint" CASCADE'

    def test_create_with_schema_and_collation(self, dialect):
        sql, _ = PostgresCreateDomainExpression(
            dialect, "posint", "NUMERIC",
            schema="app", collation="C",
        ).to_sql()
        assert sql == 'CREATE DOMAIN "app"."posint" AS NUMERIC COLLATE "C"'

    def test_create_without_default(self, dialect):
        sql, _ = PostgresCreateDomainExpression(
            dialect, "posint", "NUMERIC", constraints=["CHECK (VALUE > 0)"]
        ).to_sql()
        assert sql == 'CREATE DOMAIN "posint" AS NUMERIC CHECK (VALUE > 0)'

    def test_alter_drop_default(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.DROP_DEFAULT
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" DROP DEFAULT'

    def test_alter_set_default_with_schema(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.SET_DEFAULT,
            schema="app", new_value="1",
        ).to_sql()
        assert sql == 'ALTER DOMAIN "app"."posint" SET DEFAULT 1'

    def test_alter_unsupported_action_raises(self, dialect):
        expr = PostgresAlterDomainExpression(dialect, "posint", "RANDOM")
        with pytest.raises(ValueError, match="Unsupported ALTER DOMAIN"):
            expr.to_sql()

    def test_drop_restrict(self, dialect):
        sql, _ = PostgresDropDomainExpression(
            dialect, "posint", schema="app", restrict=True
        ).to_sql()
        assert sql == 'DROP DOMAIN "app"."posint" RESTRICT'

    def test_drop_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropDomainExpression(
            dialect, "posint", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateDomainExpression(d, "posint", "INTEGER").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresAlterDomainExpression(
                d, "posint", AlterDomainActionType.SET_DEFAULT, new_value="0"
            ).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropDomainExpression(d, "posint").to_sql()


    def test_typed_create_with_schema_and_all_clauses(self, dialect):
        value = DomainValueExpression(dialect)
        condition = value > Literal(dialect, 0, inline_literals=True)
        check = DomainCheckConstraint(dialect, condition, name="positive")
        sql, params = PostgresCreateDomainExpression(
            dialect,
            "amount",
            IntegerType(dialect),
            schema="app",
            collation="public.catalog",
            default=0,
            checks=[check],
            nullability=DomainNullability.NOT_NULL,
        ).to_sql()
        assert sql == (
            'CREATE DOMAIN "app"."amount" AS INTEGER COLLATE "public"."catalog" '
            'DEFAULT 0 CONSTRAINT "positive" CHECK (VALUE > 0) NOT NULL'
        )
        assert params == ()

    def test_create_domain_params_emit_one_constraint_key(self, dialect):
        value = DomainValueExpression(dialect)
        condition = value > Literal(dialect, 0, inline_literals=True)
        legacy = PostgresCreateDomainExpression(
            dialect,
            "legacy",
            IntegerType(dialect),
            constraints=["CHECK (VALUE > 0)"],
        )
        typed = PostgresCreateDomainExpression(
            dialect,
            "typed",
            IntegerType(dialect),
            checks=[condition],
        )
        legacy_params = legacy.get_params()
        typed_params = typed.get_params()
        assert "constraints" in legacy_params
        assert "checks" not in legacy_params
        assert "checks" in typed_params
        assert "constraints" not in typed_params

    def test_new_core_expression_rejects_raw_data_type(self, dialect):
        with pytest.raises(TypeError, match="DataType"):
            CreateDomainExpression(dialect, "raw_domain", "INTEGER")

    def test_all_domain_actions(self, dialect):
        value = DomainValueExpression(dialect)
        check = DomainCheckConstraint(
            dialect,
            value > Literal(dialect, 0, inline_literals=True),
            name="positive",
        )
        cases = [
            (SetDomainDefaultAction(dialect, 1), "SET DEFAULT 1"),
            (DropDomainDefaultAction(dialect), "DROP DEFAULT"),
            (SetDomainNotNullAction(dialect), "SET NOT NULL"),
            (DropDomainNotNullAction(dialect), "DROP NOT NULL"),
            (
                PostgresAddDomainCheckAction(dialect, check, not_valid=True),
                'ADD CONSTRAINT "positive" CHECK (VALUE > 0) NOT VALID',
            ),
            (
                PostgresDropDomainCheckAction(
                    dialect,
                    "positive",
                    if_exists=True,
                    cascade=True,
                ),
                'DROP CONSTRAINT IF EXISTS "positive" CASCADE',
            ),
            (
                PostgresRenameDomainConstraintAction(
                    dialect,
                    "positive",
                    "nonnegative",
                ),
                'RENAME CONSTRAINT "positive" TO "nonnegative"',
            ),
            (
                PostgresValidateDomainConstraintAction(dialect, "nonnegative"),
                'VALIDATE CONSTRAINT "nonnegative"',
            ),
            (PostgresChangeDomainOwnerAction(dialect, "app_owner"), 'OWNER TO "app_owner"'),
            (RenameDomainAction(dialect, "amount_v2"), 'RENAME TO "amount_v2"'),
            (PostgresSetDomainSchemaAction(dialect, "archive"), 'SET SCHEMA "archive"'),
        ]
        for action, expected in cases:
            sql, params = PostgresAlterDomainExpression(
                dialect,
                "amount",
                action,
                schema="app",
            ).to_sql()
            assert sql == f'ALTER DOMAIN "app"."amount" {expected}'
            assert params == ()

    def test_domain_actions_are_not_combinable(self, dialect):
        expression = AlterDomainExpression(
            dialect,
            "amount",
            [DropDomainDefaultAction(dialect), SetDomainNotNullAction(dialect)],
        )
        with pytest.raises(UnsupportedFeatureError, match="multiple ALTER DOMAIN actions"):
            expression.to_sql()

    def test_drop_domain_flags(self, dialect):
        sql, _ = PostgresDropDomainExpression(
            dialect,
            "amount",
            schema="app",
            if_exists=True,
            restrict=True,
        ).to_sql()
        assert sql == 'DROP DOMAIN IF EXISTS "app"."amount" RESTRICT'
        expression = PostgresDropDomainExpression(
            dialect,
            "amount",
            cascade=True,
            restrict=True,
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expression.to_sql()


class TestPostgresTypeDDLExpressions:
    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_composite_definition_round_trip(self, dialect):
        definition = PostgresCompositeTypeDefinition(
            dialect,
            [PostgresCompositeTypeAttribute("id", IntegerType(dialect))],
        )
        restored = (
            deserialize(serialize(definition), dialect),
            deserialize_json(serialize_json(definition), dialect),
            deserialize_xml(serialize_xml(definition), dialect),
        )
        assert all(item.get_params() == definition.get_params() for item in restored)

    def test_base_unset_round_trip(self, dialect):
        definition = PostgresBaseTypeDefinition(
            dialect,
            input_function="box_in",
            output_function="box_out",
        )
        restored = (
            deserialize(serialize(definition), dialect),
            deserialize_json(serialize_json(definition), dialect),
            deserialize_xml(serialize_xml(definition), dialect),
        )
        assert all(item.get_params() == definition.get_params() for item in restored)

    def test_compatibility_wrapper_params(self, dialect):
        create = PostgresCreateEnumTypeExpression(
            dialect,
            "status",
            ["active"],
            if_not_exists=True,
        )
        alter = PostgresAlterEnumAddValueExpression(
            dialect,
            "status",
            "pending",
            if_not_exists=True,
        )
        drop = PostgresDropEnumTypeExpression(
            dialect,
            "status",
            restrict=True,
        )
        assert create.get_params()["if_not_exists"] is True
        assert alter.get_params()["if_not_exists"] is True
        assert drop.get_params()["restrict"] is True

    def test_composite_definition(self, dialect):
        definition = PostgresCompositeTypeDefinition(
            dialect,
            [
                PostgresCompositeTypeAttribute("id", IntegerType(dialect)),
                PostgresCompositeTypeAttribute(
                    "label",
                    TextType(dialect),
                    collation="public.catalog",
                ),
            ],
        )
        sql, params = CreateTypeExpression(
            dialect,
            "app.address",
            definition,
        ).to_sql()
        assert sql == (
            'CREATE TYPE "app"."address" AS ("id" INTEGER, "label" TEXT '
            'COLLATE "public"."catalog")'
        )
        assert params == ()

    def test_enum_definition_escapes_labels(self, dialect):
        definition = PostgresEnumTypeDefinition(
            dialect,
            ["ready", "O'Reilly", ""],
        )
        sql, params = CreateTypeExpression(dialect, "status", definition).to_sql()
        assert sql == "CREATE TYPE \"status\" AS ENUM ('ready', 'O''Reilly', '')"
        assert params == ()

    def test_range_definition_with_multirange_name(self, dialect):
        definition = PostgresRangeTypeDefinition(
            dialect,
            IntegerType(dialect),
            subtype_operator_class="public.int_ops",
            canonical_function="public.canonical",
            multirange_type_name="app.span_multirange",
        )
        sql, params = CreateTypeExpression(dialect, "app.span", definition).to_sql()
        assert sql == (
            'CREATE TYPE "app"."span" AS RANGE (SUBTYPE = INTEGER, '
            'SUBTYPE_OPCLASS = "public"."int_ops", CANONICAL = "public"."canonical", '
            'MULTIRANGE_TYPE_NAME = "app"."span_multirange")'
        )
        assert params == ()

    def test_range_multirange_name_version_gate(self):
        dialect = PostgresDialect(version=(13, 0, 0))
        definition = PostgresRangeTypeDefinition(
            dialect,
            IntegerType(dialect),
            multirange_type_name="span_multirange",
        )
        with pytest.raises(
            UnsupportedFeatureError,
            match="CREATE TYPE MULTIRANGE_TYPE_NAME",
        ):
            CreateTypeExpression(dialect, "span", definition).to_sql()

    def test_base_definition(self, dialect):
        definition = PostgresBaseTypeDefinition(
            dialect,
            input_function="public.box_in",
            output_function="public.box_out",
            internallength=16,
            passedbyvalue=True,
            storage="plain",
            default=0,
            element=IntegerType(dialect),
        )
        sql, params = CreateTypeExpression(dialect, "box", definition).to_sql()
        assert sql == (
            'CREATE TYPE "box" (INPUT = "public"."box_in", '
            'OUTPUT = "public"."box_out", INTERNALLENGTH = 16, PASSEDBYVALUE, '
            'STORAGE = "plain", DEFAULT = 0, ELEMENT = INTEGER)'
        )
        assert params == ()

    def test_base_like_does_not_bypass_io_functions(self, dialect):
        with pytest.raises(ValueError, match="input_function and output_function"):
            PostgresBaseTypeDefinition(
                dialect,
                like_type=IntegerType(dialect),
            )

    def test_shell_definition(self, dialect):
        sql, params = CreateTypeExpression(
            dialect,
            "public.pending_box",
            PostgresShellTypeDefinition(dialect),
        ).to_sql()
        assert sql == 'CREATE TYPE "public"."pending_box"'
        assert params == ()

    def test_create_type_unsupported_clauses_fail_fast(self, dialect):
        enum = PostgresEnumTypeDefinition(dialect, ["active"])
        with pytest.raises(UnsupportedFeatureError, match="CREATE TYPE IF NOT EXISTS"):
            CreateTypeExpression(
                dialect,
                "status",
                enum,
                if_not_exists=True,
            ).to_sql()
        with pytest.raises(UnsupportedFeatureError, match="CREATE OR REPLACE TYPE"):
            CreateTypeExpression(
                dialect,
                "status",
                enum,
                or_replace=True,
            ).to_sql()

    def test_drop_type_flags(self, dialect):
        sql, params = PostgresDropTypeExpression(
            dialect,
            "status",
            schema_name="app",
            if_exists=True,
            cascade=True,
        ).to_sql()
        assert sql == 'DROP TYPE IF EXISTS "app"."status" CASCADE'
        assert params == ()
        expression = PostgresDropTypeExpression(
            dialect,
            "status",
            cascade=True,
            restrict=True,
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expression.to_sql()

    def test_legacy_drop_type_name_keyword(self, dialect):
        sql, params = dialect.format_drop_type_statement(
            name="status",
            schema="app",
            if_exists=True,
        )
        assert sql == 'DROP TYPE IF EXISTS "app"."status"'
        assert params == ()

    def test_all_type_actions(self, dialect):
        cases = [
            (PostgresRenameTypeAction(dialect, "state"), 'RENAME TO "state"'),
            (PostgresSetTypeSchemaAction(dialect, "archive"), 'SET SCHEMA "archive"'),
            (PostgresChangeTypeOwnerAction(dialect, "type_owner"), 'OWNER TO "type_owner"'),
            (
                PostgresRenameTypeAttributeAction(
                    dialect,
                    "label",
                    "name",
                    restrict=True,
                ),
                'RENAME ATTRIBUTE "label" TO "name" RESTRICT',
            ),
            (
                PostgresAddTypeAttributeAction(
                    dialect,
                    "active",
                    IntegerType(dialect),
                    collation="public.catalog",
                ),
                'ADD ATTRIBUTE "active" INTEGER COLLATE "public"."catalog"',
            ),
            (
                PostgresDropTypeAttributeAction(
                    dialect,
                    "active",
                    if_exists=True,
                    cascade=True,
                ),
                'DROP ATTRIBUTE IF EXISTS "active" CASCADE',
            ),
            (
                PostgresAlterTypeAttributeAction(
                    dialect,
                    "active",
                    IntegerType(dialect),
                    set_data=True,
                ),
                'ALTER ATTRIBUTE "active" SET DATA TYPE INTEGER',
            ),
            (
                PostgresAddEnumValueAction(
                    dialect,
                    "O'Reilly",
                    if_not_exists=True,
                    after="ready",
                ),
                "ADD VALUE IF NOT EXISTS 'O''Reilly' AFTER 'ready'",
            ),
            (
                PostgresRenameEnumValueAction(dialect, "old", "new"),
                "RENAME VALUE 'old' TO 'new'",
            ),
            (
                PostgresSetTypePropertiesAction(
                    dialect,
                    {"SEND": "public.box_send", "STORAGE": "plain"},
                ),
                'SET (SEND = "public"."box_send", STORAGE = "plain")',
            ),
        ]
        for action, expected in cases:
            sql, params = AlterTypeExpression(
                dialect,
                "app.status",
                [action],
            ).to_sql()
            assert sql == f'ALTER TYPE "app"."status" {expected}'
            assert params == ()

    def test_composite_actions_can_be_combined(self, dialect):
        actions = [
            PostgresAddTypeAttributeAction(dialect, "active", IntegerType(dialect)),
            PostgresDropTypeAttributeAction(dialect, "legacy"),
        ]
        sql, params = AlterTypeExpression(dialect, "app.address", actions).to_sql()
        assert sql == (
            'ALTER TYPE "app"."address" ADD ATTRIBUTE "active" INTEGER, '
            'DROP ATTRIBUTE "legacy"'
        )
        assert params == ()

    def test_mixed_type_actions_cannot_be_combined(self, dialect):
        actions = [
            PostgresAddTypeAttributeAction(dialect, "active", IntegerType(dialect)),
            PostgresRenameTypeAction(dialect, "new_address"),
        ]
        with pytest.raises(UnsupportedFeatureError, match="multiple ALTER TYPE actions"):
            AlterTypeExpression(dialect, "app.address", actions).to_sql()

    def test_type_version_boundaries(self):
        enum = PostgresEnumTypeDefinition(PostgresDialect(version=(9, 6, 0)), ["a"])
        rename = PostgresRenameEnumValueAction(
            PostgresDialect(version=(9, 6, 0)),
            "a",
            "b",
        )
        with pytest.raises(UnsupportedFeatureError, match="RENAME VALUE"):
            AlterTypeExpression(
                PostgresDialect(version=(9, 6, 0)),
                "status",
                [rename],
            ).to_sql()
        sql, _ = AlterTypeExpression(
            PostgresDialect(version=(10, 0, 0)),
            "status",
            [PostgresRenameEnumValueAction(
                PostgresDialect(version=(10, 0, 0)),
                "a",
                "b",
            )],
        ).to_sql()
        assert sql == 'ALTER TYPE "status" RENAME VALUE \'a\' TO \'b\''
        with pytest.raises(UnsupportedFeatureError, match="SET properties"):
            AlterTypeExpression(
                PostgresDialect(version=(12, 0, 0)),
                "box",
                [PostgresSetTypePropertiesAction(
                    PostgresDialect(version=(12, 0, 0)),
                    {"STORAGE": "plain"},
                )],
            ).to_sql()
        assert enum.labels == ["a"]

    def test_base_subscript_version_boundary(self):
        for version in ((13, 0, 0), (14, 0, 0)):
            dialect = PostgresDialect(version=version)
            definition = PostgresBaseTypeDefinition(
                dialect,
                input_function="box_in",
                output_function="box_out",
                subscript_function="box_subscript",
            )
            if version < (14, 0, 0):
                with pytest.raises(UnsupportedFeatureError, match="SUBSCRIPT"):
                    CreateTypeExpression(dialect, "box", definition).to_sql()
            else:
                sql, _ = CreateTypeExpression(dialect, "box", definition).to_sql()
                assert 'SUBSCRIPT = "box_subscript"' in sql


class TestPostgresCollationDDLExpression:
    """CREATE / DROP COLLATION DDL."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "enloc", locale="en_US.UTF-8", provider="libc"
        ).to_sql()
        assert sql == 'CREATE COLLATION "enloc" (LOCALE = en_US.UTF-8, PROVIDER = libc)'

    def test_create_if_not_exists(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "myloc", if_not_exists=True, lc_collate="en_US",
        ).to_sql()
        assert sql == 'CREATE COLLATION IF NOT EXISTS "myloc" (LC_COLLATE = en_US)'

    def test_drop(self, dialect):
        sql, _ = PostgresDropCollationExpression(
            dialect, "enloc", if_exists=True
        ).to_sql()
        assert sql == 'DROP COLLATION IF EXISTS "enloc"'

    def test_create_with_schema_and_all_params(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "custom", schema="app", if_not_exists=True,
            locale="en_US.UTF-8", lc_collate="en_US", lc_ctype="en_US",
            provider="icu", version="153.14",
        ).to_sql()
        assert sql == (
            'CREATE COLLATION IF NOT EXISTS "app"."custom" '
            '(LOCALE = en_US.UTF-8, LC_COLLATE = en_US, LC_CTYPE = en_US, '
            'PROVIDER = icu, VERSION = 153.14)'
        )

    def test_create_without_params(self, dialect):
        sql, _ = PostgresCreateCollationExpression(dialect, "bare").to_sql()
        assert sql == 'CREATE COLLATION "bare"'

    def test_drop_restrict_with_schema(self, dialect):
        sql, _ = PostgresDropCollationExpression(
            dialect, "enloc", schema="app", restrict=True
        ).to_sql()
        assert sql == 'DROP COLLATION "app"."enloc" RESTRICT'

    def test_drop_cascade(self, dialect):
        sql, _ = PostgresDropCollationExpression(
            dialect, "enloc", cascade=True
        ).to_sql()
        assert sql == 'DROP COLLATION "enloc" CASCADE'

    def test_drop_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropCollationExpression(
            dialect, "enloc", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateCollationExpression(d, "c").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropCollationExpression(d, "c").to_sql()


class TestPostgresForeignTableDDLExpression:
    """CREATE / DROP FOREIGN TABLE."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateForeignTableExpression(
            dialect, "ft", "srv", columns=["a integer", "b text"],
            options=["host 'h'"],
        ).to_sql()
        assert sql == (
            'CREATE FOREIGN TABLE "ft" (a integer, b text) '
            'SERVER "srv" OPTIONS (host \'h\')'
        )

    def test_drop(self, dialect):
        sql, _ = PostgresDropForeignTableExpression(
            dialect, "ft", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP FOREIGN TABLE IF EXISTS "ft" CASCADE'

    def test_create_if_not_exists_schema(self, dialect):
        sql, _ = PostgresCreateForeignTableExpression(
            dialect, "ft", "srv", schema="app", if_not_exists=True,
            columns=["a integer"],
        ).to_sql()
        assert sql == (
            'CREATE FOREIGN TABLE IF NOT EXISTS "app"."ft" (a integer) '
            'SERVER "srv"'
        )

    def test_create_without_columns_or_options(self, dialect):
        sql, _ = PostgresCreateForeignTableExpression(
            dialect, "bar_ft", "fdw_bar"
        ).to_sql()
        assert sql == 'CREATE FOREIGN TABLE "bar_ft" SERVER "fdw_bar"'

    def test_drop_restrict_with_schema(self, dialect):
        sql, _ = PostgresDropForeignTableExpression(
            dialect, "ft", schema="app", restrict=True
        ).to_sql()
        assert sql == 'DROP FOREIGN TABLE "app"."ft" RESTRICT'

    def test_drop_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropForeignTableExpression(
            dialect, "ft", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateForeignTableExpression(d, "ft", "srv").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropForeignTableExpression(d, "ft").to_sql()


class TestPostgresRoutineDDLExpression:
    """CREATE / DROP FUNCTION and AGGREGATE."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_function(self, dialect):
        sql, _ = PostgresCreateFunctionExpression(
            dialect, "add", "integer", "int 1;",
            args=["a integer"], strict=True, security="DEFINER",
        ).to_sql()
        assert sql == (
            'CREATE FUNCTION "add"(a integer) RETURNS integer '
            'STRICT SECURITY DEFINER LANGUAGE plpgsql AS $$ int 1; $$'
        )

    def test_drop_function(self, dialect):
        sql, _ = PostgresDropFunctionExpression(
            dialect, "add", args=["integer"], cascade=True
        ).to_sql()
        assert sql == 'DROP FUNCTION "add" (integer) CASCADE'

    def test_create_aggregate(self, dialect):
        sql, _ = PostgresCreateAggregateExpression(
            dialect, "mysum", "sum", "integer", initcond="0"
        ).to_sql()
        assert sql == 'CREATE AGGREGATE "mysum" (SFUNC=sum, STYPE=integer, INITCOND=0)'

    def test_drop_aggregate(self, dialect):
        sql, _ = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", if_exists=True
        ).to_sql()
        assert sql == 'DROP AGGREGATE IF EXISTS "mysum" (integer)'

    def test_create_function_full_options(self, dialect):
        sql, _ = PostgresCreateFunctionExpression(
            dialect, "add", "integer", "int 1;", schema="app",
            args=["a integer", "b integer"], or_replace=True,
            security="INVOKER", cost=10.0, rows=50,
        ).to_sql()
        assert 'CREATE OR REPLACE FUNCTION "app"."add"' in sql
        assert "(a integer, b integer)" in sql
        assert "SECURITY INVOKER" in sql
        assert "COST 10.0" in sql
        assert "ROWS 50" in sql

    def test_create_function_no_args_no_strict(self, dialect):
        sql, _ = PostgresCreateFunctionExpression(
            dialect, "now", "timestamptz", "SELECT now();", language="sql"
        ).to_sql()
        assert sql == (
            'CREATE FUNCTION "now"() RETURNS timestamptz LANGUAGE sql '
            'AS $$ SELECT now(); $$'
        )

    def test_create_function_invalid_security(self, dialect):
        expr = PostgresCreateFunctionExpression(
            dialect, "f", "integer", "int 1;", security="OWNER"
        )
        with pytest.raises(ValueError, match="DEFINER"):
            expr.to_sql()

    def test_drop_function_restrict(self, dialect):
        sql, _ = PostgresDropFunctionExpression(
            dialect, "add", schema="app", if_exists=True, restrict=True
        ).to_sql()
        assert sql == 'DROP FUNCTION IF EXISTS "app"."add" RESTRICT'

    def test_drop_function_without_args(self, dialect):
        sql, _ = PostgresDropFunctionExpression(
            dialect, "add", schema="app"
        ).to_sql()
        assert sql == 'DROP FUNCTION "app"."add"'

    def test_drop_function_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropFunctionExpression(
            dialect, "add", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_create_aggregate_full(self, dialect):
        sql, _ = PostgresCreateAggregateExpression(
            dialect, "mysum", "sum", "integer", schema="app",
            finalfunc="mysum_final",
        ).to_sql()
        assert sql == (
            'CREATE AGGREGATE "app"."mysum" '
            '(SFUNC=sum, STYPE=integer, FINALFUNC=mysum_final)'
        )

    def test_drop_aggregate_cascade(self, dialect):
        sql, _ = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", schema="app", cascade=True
        ).to_sql()
        assert sql == 'DROP AGGREGATE "app"."mysum" (integer) CASCADE'

    def test_drop_aggregate_restrict(self, dialect):
        sql, _ = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", if_exists=True, restrict=True
        ).to_sql()
        assert sql == 'DROP AGGREGATE IF EXISTS "mysum" (integer) RESTRICT'

    def test_drop_aggregate_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_routine_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateFunctionExpression(
                d, "f", "integer", "int 1;"
            ).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropFunctionExpression(d, "f").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateAggregateExpression(d, "s", "sum", "integer").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropAggregateExpression(d, "s", "integer").to_sql()


class TestPostgresPublicationExpression:
    """CREATE / DROP PUBLICATION and SUBSCRIPTION."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_publication_tables(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub1", tables=["orders", "users"]
        ).to_sql()
        assert sql == 'CREATE PUBLICATION "pub1" FOR TABLE "orders", "users"'

    def test_create_publication_all_tables(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub_all", all_tables=True,
            options=["publish='insert'"],
        ).to_sql()
        assert sql == (
            'CREATE PUBLICATION "pub_all" FOR ALL TABLES WITH (publish=\'insert\')'
        )

    def test_drop_publication(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", if_exists=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION IF EXISTS "pub1"'

    def test_create_subscription(self, dialect):
        sql, _ = PostgresCreateSubscriptionExpression(
            dialect, "sub1", "host=db port=5432", ["pub1"]
        ).to_sql()
        assert sql == (
            'CREATE SUBSCRIPTION "sub1" CONNECTION \'host=db port=5432\' '
            'PUBLICATION "pub1"'
        )

    def test_drop_subscription(self, dialect):
        sql, _ = PostgresDropSubscriptionExpression(dialect, "sub1", cascade=True).to_sql()
        assert sql == 'DROP SUBSCRIPTION "sub1" CASCADE'

    def test_create_publication_tables_options(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub1", tables=["orders"],
            options=["publish='insert'"],
        ).to_sql()
        assert sql == (
            'CREATE PUBLICATION "pub1" FOR TABLE "orders" '
            "WITH (publish='insert')"
        )

    def test_create_publication_tables_and_all_raises(self, dialect):
        expr = PostgresCreatePublicationExpression(
            dialect, "pub1", tables=["orders"], all_tables=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_create_publication_neither_raises(self, dialect):
        expr = PostgresCreatePublicationExpression(dialect, "pub1")
        with pytest.raises(ValueError, match="requires either"):
            expr.to_sql()

    def test_drop_publication_cascade(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", cascade=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION "pub1" CASCADE'

    def test_drop_publication_restrict(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", restrict=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION "pub1" RESTRICT'

    def test_drop_publication_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropPublicationExpression(
            dialect, "pub1", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_drop_publication_if_exists(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", if_exists=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION IF EXISTS "pub1"'

    def test_create_subscription_options(self, dialect):
        sql, _ = PostgresCreateSubscriptionExpression(
            dialect, "sub2", "host=db port=5432", ["pub1", "pub2"],
            options=["copy_data = false"],
        ).to_sql()
        assert sql == (
            'CREATE SUBSCRIPTION "sub2" CONNECTION \'host=db port=5432\' '
            'PUBLICATION "pub1", "pub2" WITH (copy_data = false)'
        )

    def test_drop_subscription_if_exists(self, dialect):
        sql, _ = PostgresDropSubscriptionExpression(
            dialect, "sub1", if_exists=True
        ).to_sql()
        assert sql == 'DROP SUBSCRIPTION IF EXISTS "sub1"'

    def test_pubsub_version_gate_10(self):
        d = PostgresDialect(version=(9, 6, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreatePublicationExpression(
                d, "pub1", tables=["orders"]
            ).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropPublicationExpression(d, "pub1").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateSubscriptionExpression(d, "s", "conn", ["p"]).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropSubscriptionExpression(d, "s").to_sql()

