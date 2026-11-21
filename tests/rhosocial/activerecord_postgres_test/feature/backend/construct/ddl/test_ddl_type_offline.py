# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/ddl/test_ddl_type_offline.py
"""Offline unit tests for the PostgreSQL user-defined TYPE DDL mixin.

A connectionless :class:`PostgresDialect` is constructed directly and its
``format_*`` / ``supports_*`` methods are exercised against representative
definitions and alter actions. No connection, driver or server is involved.
"""

from unittest.mock import MagicMock, patch

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.postgres.functions import create_type_enum, drop_type
from rhosocial.activerecord.backend.expression.objects import Type
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeStatement,
    CreateTypeStatement,
)
from rhosocial.activerecord.backend.expression.types import DataType
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.type import (
    PostgresAddEnumValueAction,
    PostgresAddTypeAttributeAction,
    PostgresAlterTypeAttributeAction,
    PostgresBaseTypeDefinition,
    PostgresChangeTypeOwnerAction,
    PostgresCompositeTypeAttribute,
    PostgresCompositeTypeDefinition,
    PostgresDropTypeAttributeAction,
    PostgresDropTypeExpression,
    PostgresEnumTypeDefinition,
    PostgresRangeTypeDefinition,
    PostgresRenameEnumValueAction,
    PostgresRenameTypeAction,
    PostgresRenameTypeAttributeAction,
    PostgresSetTypePropertiesAction,
    PostgresSetTypeSchemaAction,
    PostgresShellTypeDefinition,
)

PG96 = (9, 6, 0)
PG10 = (10, 0, 0)
PG13 = (13, 0, 0)
PG14 = (14, 0, 0)
PG16 = (16, 0, 0)


def _dialect(version=PG16):
    return PostgresDialect(version=version)


def _type(name, schema=None):
    return Type(_dialect(), name, schema_name=schema)


def _data_type(text, dialect=None):
    return DataType.as_type(dialect or _dialect(), text)


# ===========================================================================
# capability switches
# ===========================================================================


class TestCapabilities:
    @pytest.mark.parametrize("version,supported", [((9, 5, 0), False), (PG96, True), (PG16, True)])
    def test_type_objects_versions(self, version, supported):
        assert _dialect(version).supports_type_objects() is supported

    @pytest.mark.parametrize(
        "method",
        [
            "supports_type_objects",
            "supports_create_type",
            "supports_alter_type",
            "supports_drop_type",
            "supports_drop_type_if_exists",
            "supports_multiple_type_alter_actions",
            "supports_drop_type_cascade",
            "supports_drop_type_restrict",
            "supports_type_if_exists",
            "supports_type_cascade",
        ],
    )
    def test_supported_on_modern_server(self, method):
        assert getattr(_dialect(PG16), method)() is True

    @pytest.mark.parametrize(
        "method",
        [
            "supports_create_type_if_not_exists",
            "supports_create_type_or_replace",
            "supports_alter_type_if_exists",
            "supports_type_if_not_exists",
        ],
    )
    def test_never_supported(self, method):
        assert getattr(_dialect(PG16), method)() is False

    @pytest.mark.parametrize(
        "method",
        [
            "supports_type_objects",
            "supports_create_type",
            "supports_alter_type",
            "supports_drop_type",
            "supports_drop_type_if_exists",
            "supports_multiple_type_alter_actions",
            "supports_drop_type_cascade",
            "supports_drop_type_restrict",
        ],
    )
    def test_not_supported_before_96(self, method):
        assert getattr(_dialect((9, 5, 0)), method)() is False

    def test_supported_type_definitions(self):
        assert _dialect(PG16).supported_type_definitions() == (
            PostgresCompositeTypeDefinition,
            PostgresEnumTypeDefinition,
            PostgresRangeTypeDefinition,
            PostgresBaseTypeDefinition,
            PostgresShellTypeDefinition,
        )
        assert _dialect((9, 5, 0)).supported_type_definitions() == ()

    @pytest.mark.parametrize(
        "definition",
        [
            PostgresCompositeTypeDefinition,
            PostgresEnumTypeDefinition,
            PostgresRangeTypeDefinition,
            PostgresBaseTypeDefinition,
            PostgresShellTypeDefinition,
        ],
    )
    def test_supports_type_definition(self, definition):
        assert _dialect(PG16).supports_type_definition(definition) is True
        assert _dialect((9, 5, 0)).supports_type_definition(definition) is False

    @pytest.mark.parametrize("bogus", [str, int, object])
    def test_rejects_unknown_definition(self, bogus):
        assert _dialect(PG16).supports_type_definition(bogus) is False

    @pytest.mark.parametrize(
        "action",
        [
            PostgresRenameTypeAction,
            PostgresSetTypeSchemaAction,
            PostgresChangeTypeOwnerAction,
            PostgresRenameTypeAttributeAction,
            PostgresAddTypeAttributeAction,
            PostgresDropTypeAttributeAction,
            PostgresAlterTypeAttributeAction,
            PostgresAddEnumValueAction,
            PostgresRenameEnumValueAction,
            PostgresSetTypePropertiesAction,
        ],
    )
    def test_supports_all_actions_on_modern_server(self, action):
        assert _dialect(PG16).supports_type_alter_action(action) is True

    @pytest.mark.parametrize(
        "action,version,expected",
        [
            (PostgresRenameEnumValueAction, PG96, False),
            (PostgresRenameEnumValueAction, PG10, True),
            (PostgresSetTypePropertiesAction, PG10, False),
            (PostgresSetTypePropertiesAction, PG13, True),
            (PostgresAddEnumValueAction, PG96, True),
        ],
    )
    def test_version_gated_actions(self, action, version, expected):
        assert _dialect(version).supports_type_alter_action(action) is expected

    @pytest.mark.parametrize("bogus", [str, object])
    def test_rejects_unknown_action(self, bogus):
        assert _dialect(PG16).supports_type_alter_action(bogus) is False


# ===========================================================================
# pure formatting helpers
# ===========================================================================


class TestFormattingHelpers:
    @pytest.mark.parametrize(
        "name,schema,expected",
        [
            ("name", None, '"name"'),
            ("name", "public", '"public"."name"'),
            ("MiXeD", "MySchema", '"MySchema"."MiXeD"'),
        ],
    )
    def test_qualified_identifier(self, name, schema, expected):
        assert _dialect()._format_qualified_identifier(name, schema) == expected

    @pytest.mark.parametrize("value", ["", "a..b", ".b", "a."])
    def test_qualified_identifier_rejects_empty_segments(self, value):
        with pytest.raises(ValueError, match="non-empty segments"):
            _dialect()._format_qualified_identifier(value)

    @pytest.mark.parametrize(
        "text",
        ["INTEGER", "TEXT", "UUID", "NUMERIC", "TIMESTAMP", "JSONB", "DECIMAL(10,2)", "TEXT[]"],
    )
    def test_data_type_rendering(self, text):
        assert _dialect()._format_type_data_type(_data_type(text)) == text

    def test_data_type_requires_data_type(self):
        with pytest.raises(TypeError, match="must be a DataType"):
            _dialect()._format_type_data_type("TEXT")

    def test_data_type_rejects_bind_parameters(self):
        fake = MagicMock(spec=DataType)
        fake.to_sql.return_value = ("?", ("param",))
        with pytest.raises(ValueError, match="without bind parameters"):
            _dialect()._format_type_data_type(fake)

    @pytest.mark.parametrize(
        "cascade,restrict,expected",
        [(False, False, ""), (True, False, "CASCADE"), (False, True, "RESTRICT")],
    )
    def test_behavior(self, cascade, restrict, expected):
        assert _dialect()._format_type_behavior(cascade, restrict) == expected

    def test_behavior_rejects_both(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _dialect()._format_type_behavior(True, True)

    def test_enum_value(self):
        assert _dialect()._format_postgres_enum_value("a'b") == "'a''b'"

    def test_enum_value_requires_string(self):
        with pytest.raises(TypeError, match="must be strings"):
            _dialect()._format_postgres_enum_value(1)

    def test_composite_attribute(self):
        attr = PostgresCompositeTypeAttribute(
            "label", _data_type("TEXT"), collation="public.mycoll"
        )
        assert _dialect()._format_postgres_composite_attribute(attr) == (
            '"label" TEXT COLLATE "public"."mycoll"'
        )

    def test_composite_attribute_without_collation(self):
        attr = PostgresCompositeTypeAttribute("n", _data_type("INTEGER"))
        assert _dialect()._format_postgres_composite_attribute(attr) == '"n" INTEGER'


# ===========================================================================
# CREATE TYPE / type definitions
# ===========================================================================


class TestCreateType:
    def test_enum_helper(self):
        sql, params = create_type_enum(_dialect(), "mood", ["happy", "sad"]).to_sql()
        assert sql == "CREATE TYPE \"mood\" AS ENUM ('happy', 'sad')"
        assert params == ()

    def test_enum_helper_with_schema(self):
        sql, _ = create_type_enum(_dialect(), "mood", ["ok"], schema="public").to_sql()
        assert sql == 'CREATE TYPE "public"."mood" AS ENUM (\'ok\')'

    def test_composite_statement(self):
        definition = PostgresCompositeTypeDefinition(
            _dialect(),
            [
                PostgresCompositeTypeAttribute("x", _data_type("INTEGER")),
                PostgresCompositeTypeAttribute(
                    "label", _data_type("TEXT"), collation="public.mycoll"
                ),
            ],
        )
        sql, params = _dialect().format_create_type_statement(
            CreateTypeStatement(_dialect(), _type("addr"), definition)
        )
        assert sql == 'CREATE TYPE "addr" AS ("x" INTEGER, "label" TEXT COLLATE "public"."mycoll")'
        assert params == ()

    def test_statement_type_must_be_type_object(self):
        definition = PostgresEnumTypeDefinition(_dialect(), ["a"])
        bad = MagicMock()
        bad.definition_kind = "enum"
        with pytest.raises(TypeError, match="must be a Type"):
            _dialect().format_create_type_statement(
                CreateTypeStatement(_dialect(), bad, definition)
            )

    @pytest.mark.parametrize(
        "kwargs,match",
        [
            (dict(if_not_exists=True), "IF NOT EXISTS"),
            (dict(or_replace=True), "OR REPLACE"),
        ],
    )
    def test_unsupported_create_modifiers(self, kwargs, match):
        definition = PostgresEnumTypeDefinition(_dialect(), ["a"])
        with pytest.raises(UnsupportedFeatureError, match=match):
            _dialect().format_create_type_statement(
                CreateTypeStatement(_dialect(), _type("mood"), definition, **kwargs)
            )

    def test_create_type_requires_96(self):
        with pytest.raises(UnsupportedFeatureError, match="CREATE TYPE"):
            create_type_enum(_dialect((9, 5, 0)), "mood", ["a"]).to_sql()

    def test_definition_must_be_supported(self):
        with pytest.raises(UnsupportedFeatureError, match="TYPE definition"):
            _dialect().format_create_type_statement(
                CreateTypeStatement(_dialect(), _type("mood"), MagicMock())
            )


class TestTypeDefinitions:
    def test_enum_definition(self):
        definition = PostgresEnumTypeDefinition(_dialect(), ["a", "b'c"])
        assert definition.definition_kind == "enum"
        assert _dialect().format_type_definition(definition) == ("AS ENUM ('a', 'b''c')", ())

    @pytest.mark.parametrize("labels", [["a", "a"], [1]])
    def test_enum_definition_rejects_bad_labels(self, labels):
        with pytest.raises((ValueError, TypeError)):
            PostgresEnumTypeDefinition(_dialect(), labels)

    def test_range_definition_minimal(self):
        definition = PostgresRangeTypeDefinition(_dialect(), _data_type("INTEGER"))
        assert _dialect().format_type_definition(definition) == ("AS RANGE (SUBTYPE = INTEGER)", ())

    def test_range_definition_full(self):
        definition = PostgresRangeTypeDefinition(
            _dialect(),
            _data_type("NUMERIC"),
            subtype_operator_class="public.opc",
            collation="public.coll",
            canonical_function="public.canon",
            subtype_diff_function="public.diff",
            multirange_type_name="public.mr",
        )
        sql, params = _dialect().format_type_definition(definition)
        assert sql == (
            'AS RANGE (SUBTYPE = NUMERIC, SUBTYPE_OPCLASS = "public"."opc", '
            'COLLATION = "public"."coll", CANONICAL = "public"."canon", '
            'SUBTYPE_DIFF = "public"."diff", MULTIRANGE_TYPE_NAME = "public"."mr")'
        )
        assert params == ()

    def test_multirange_requires_14(self):
        definition = PostgresRangeTypeDefinition(
            _dialect(PG13), _data_type("INT4RANGE"), multirange_type_name="public.mr"
        )
        with pytest.raises(UnsupportedFeatureError, match="MULTIRANGE_TYPE_NAME"):
            _dialect(PG13).format_type_definition(definition)

    def test_base_definition_full(self):
        definition = PostgresBaseTypeDefinition(
            _dialect(),
            input_function="public.in_fn",
            output_function="public.out_fn",
            internallength=4,
            passedbyvalue=True,
            alignment="int4",
            storage="plain",
            like_type=_data_type("INTEGER"),
            category="U",
            preferred=True,
            default="x",
            element=_data_type("TEXT"),
            delimiter=",",
            collatable=True,
        )
        sql, params = _dialect().format_type_definition(definition)
        assert sql == (
            '(INPUT = "public"."in_fn", OUTPUT = "public"."out_fn", INTERNALLENGTH = 4, '
            'PASSEDBYVALUE, ALIGNMENT = "int4", STORAGE = "plain", LIKE = INTEGER, '
            "CATEGORY = \"U\", PREFERRED = TRUE, DEFAULT = 'x', ELEMENT = TEXT, "
            "DELIMITER = ',', COLLATABLE = TRUE)"
        )
        assert params == ()

    def test_base_definition_variable_length(self):
        definition = PostgresBaseTypeDefinition(
            _dialect(),
            input_function="in",
            output_function="out",
            internallength="VARIABLE",
        )
        assert "INTERNALLENGTH = VARIABLE" in _dialect().format_type_definition(definition)[0]

    def test_base_subscript_requires_14(self):
        definition = PostgresBaseTypeDefinition(
            _dialect(PG13),
            input_function="public.i",
            output_function="public.o",
            subscript_function="public.s",
        )
        with pytest.raises(UnsupportedFeatureError, match="SUBSCRIPT"):
            _dialect(PG13).format_type_definition(definition)

    def test_base_requires_io_functions(self):
        with pytest.raises(ValueError, match="required"):
            PostgresBaseTypeDefinition(_dialect(), input_function="only_in")

    def test_shell_definition_is_empty(self):
        assert _dialect().format_type_definition(PostgresShellTypeDefinition(_dialect())) == (
            "",
            (),
        )

    def test_unknown_definition_rejected(self):
        with pytest.raises(UnsupportedFeatureError, match="TYPE definition"):
            _dialect().format_type_definition(MagicMock(definition_kind="bogus"))


# ===========================================================================
# ALTER TYPE actions and statements
# ===========================================================================


class TestAlterActions:
    @pytest.mark.parametrize(
        "action,expected",
        [
            (PostgresRenameTypeAction(_dialect(), "newname"), 'RENAME TO "newname"'),
            (PostgresSetTypeSchemaAction(_dialect(), "other"), 'SET SCHEMA "other"'),
            (PostgresChangeTypeOwnerAction(_dialect(), "bob"), 'OWNER TO "bob"'),
            (
                PostgresRenameTypeAttributeAction(_dialect(), "old", "new", cascade=True),
                'RENAME ATTRIBUTE "old" TO "new" CASCADE',
            ),
            (
                PostgresAddTypeAttributeAction(
                    _dialect(), "extra", _data_type("TEXT"), collation="public.c"
                ),
                'ADD ATTRIBUTE "extra" TEXT COLLATE "public"."c"',
            ),
            (
                PostgresDropTypeAttributeAction(_dialect(), "gone", if_exists=True, restrict=True),
                'DROP ATTRIBUTE IF EXISTS "gone" RESTRICT',
            ),
            (
                PostgresAlterTypeAttributeAction(
                    _dialect(), "col", _data_type("INTEGER"), set_data=True
                ),
                'ALTER ATTRIBUTE "col" SET DATA TYPE INTEGER',
            ),
            (
                PostgresAlterTypeAttributeAction(_dialect(), "col", _data_type("TEXT")),
                'ALTER ATTRIBUTE "col" TYPE TEXT',
            ),
            (
                PostgresAddEnumValueAction(_dialect(), "new", before="z"),
                "ADD VALUE 'new' BEFORE 'z'",
            ),
            (
                PostgresAddEnumValueAction(_dialect(), "new", after="a", if_not_exists=True),
                "ADD VALUE IF NOT EXISTS 'new' AFTER 'a'",
            ),
            (
                PostgresRenameEnumValueAction(_dialect(), "old", "new"),
                "RENAME VALUE 'old' TO 'new'",
            ),
            (
                PostgresSetTypePropertiesAction(
                    _dialect(), {"STORAGE": "plain", "RECEIVE": "public.recv"}
                ),
                'SET (STORAGE = "plain", RECEIVE = "public"."recv")',
            ),
        ],
    )
    def test_action_sql(self, action, expected):
        assert _dialect().format_type_alter_action(action) == (expected, ())

    def test_set_properties_none_becomes_none(self):
        action = PostgresSetTypePropertiesAction(_dialect(), {"RECEIVE": None})
        assert _dialect().format_type_alter_action(action) == ("SET (RECEIVE = NONE)", ())

    def test_rename_enum_value_requires_10(self):
        action = PostgresRenameEnumValueAction(_dialect(PG96), "a", "b")
        with pytest.raises(UnsupportedFeatureError, match="RENAME VALUE"):
            _dialect(PG96).format_type_alter_action(action)

    def test_add_enum_if_not_exists_requires_93(self):
        dialect = _dialect((9, 2, 0))
        action = PostgresAddEnumValueAction(dialect, "x", if_not_exists=True)
        with patch.object(dialect, "supports_type_objects", return_value=True):
            with pytest.raises(UnsupportedFeatureError, match="IF NOT EXISTS"):
                dialect.format_type_alter_action(action)

    def test_set_properties_requires_13(self):
        action = PostgresSetTypePropertiesAction(_dialect(PG10), {"RECEIVE": "public.r"})
        with pytest.raises(UnsupportedFeatureError, match="SET properties"):
            _dialect(PG10).format_type_alter_action(action)

    def test_set_subscript_requires_14(self):
        action = PostgresSetTypePropertiesAction(_dialect(PG13), {"SUBSCRIPT": "public.s"})
        with pytest.raises(UnsupportedFeatureError, match="SUBSCRIPT"):
            _dialect(PG13).format_type_alter_action(action)

    def test_set_storage_none_rejected(self):
        with pytest.raises(ValueError, match="STORAGE"):
            PostgresSetTypePropertiesAction(_dialect(), {"STORAGE": None})

    def test_unknown_action_rejected(self):
        with pytest.raises(UnsupportedFeatureError, match="ALTER TYPE action"):
            _dialect().format_type_alter_action(MagicMock(action_kind="bogus"))

    def test_add_enum_rejects_before_and_after(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            PostgresAddEnumValueAction(_dialect(), "x", before="a", after="b")


class TestAlterStatement:
    def test_single_action(self):
        statement = AlterTypeStatement(
            _dialect(), _type("mood"), [PostgresAddEnumValueAction(_dialect(), "x")]
        )
        assert _dialect().format_alter_type_statement(statement) == (
            "ALTER TYPE \"mood\" ADD VALUE 'x'",
            (),
        )

    def test_multiple_combinable_actions(self):
        statement = AlterTypeStatement(
            _dialect(),
            _type("addr"),
            [
                PostgresAddTypeAttributeAction(_dialect(), "a", _data_type("TEXT")),
                PostgresDropTypeAttributeAction(_dialect(), "b"),
            ],
        )
        assert _dialect().format_alter_type_statement(statement) == (
            'ALTER TYPE "addr" ADD ATTRIBUTE "a" TEXT, DROP ATTRIBUTE "b"',
            (),
        )

    def test_multiple_non_combinable_actions_rejected(self):
        statement = AlterTypeStatement(
            _dialect(),
            _type("mood"),
            [
                PostgresAddEnumValueAction(_dialect(), "a"),
                PostgresRenameTypeAction(_dialect(), "b"),
            ],
        )
        with pytest.raises(UnsupportedFeatureError, match="multiple ALTER TYPE actions"):
            _dialect().format_alter_type_statement(statement)

    def test_if_exists_unsupported(self):
        statement = AlterTypeStatement(
            _dialect(),
            _type("mood"),
            [PostgresAddEnumValueAction(_dialect(), "x")],
            if_exists=True,
        )
        with pytest.raises(UnsupportedFeatureError, match="IF EXISTS"):
            _dialect().format_alter_type_statement(statement)

    def test_statement_type_must_be_type_object(self):
        statement = AlterTypeStatement(
            _dialect(), MagicMock(), [PostgresAddEnumValueAction(_dialect(), "x")]
        )
        with pytest.raises(TypeError, match="must be a Type"):
            _dialect().format_alter_type_statement(statement)


# ===========================================================================
# DROP TYPE
# ===========================================================================


class TestDropType:
    def test_by_name(self):
        assert drop_type(_dialect(), name="mood").to_sql() == ('DROP TYPE "mood"', ())

    def test_with_schema_and_if_exists(self):
        assert drop_type(_dialect(), 
            name="mood", schema="public", if_exists=True
        ).to_sql() == ('DROP TYPE IF EXISTS "public"."mood"', ())

    def test_legacy_schema_name(self):
        assert drop_type(_dialect(), name="mood", schema_name="public").to_sql() == (
            'DROP TYPE "public"."mood"',
            (),
        )

    def test_cascade(self):
        assert drop_type(_dialect(), name="mood", cascade=True).to_sql() == (
            'DROP TYPE "mood" CASCADE',
            (),
        )

    def test_restrict_via_expression(self):
        expression = PostgresDropTypeExpression(
            _dialect(), Type(_dialect(), "mood"), if_exists=True, restrict=True
        )
        assert expression.to_sql() == (
            'DROP TYPE IF EXISTS "mood" RESTRICT',
            (),
        )

    def test_expr_or_name_required(self):
        with pytest.raises(TypeError, match="required"):
            drop_type(_dialect()).to_sql()

    def test_conflicting_schema_arguments(self):
        with pytest.raises(ValueError, match="must match"):
            drop_type(_dialect(), name="mood", schema="a", schema_name="b").to_sql()

    def test_drop_type_requires_96(self):
        with pytest.raises(UnsupportedFeatureError, match="DROP TYPE"):
            drop_type(_dialect((9, 5, 0)), "mood").to_sql()

    def test_if_exists_guard(self):
        dialect = _dialect()
        with patch.object(dialect, "supports_drop_type_if_exists", return_value=False):
            with pytest.raises(UnsupportedFeatureError, match="IF EXISTS"):
                drop_type(dialect, name="mood", if_exists=True).to_sql()
