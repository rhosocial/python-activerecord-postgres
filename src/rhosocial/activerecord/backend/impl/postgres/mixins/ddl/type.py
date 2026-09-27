# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/type.py
"""PostgreSQL user-defined TYPE DDL capabilities and formatting."""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple, Type, Union, cast, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins.user_defined_type import UserDefinedTypeMixin
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeExpression,
    CreateTypeExpression,
    DropTypeExpression,
    TypeAlterAction,
    TypeDefinition,
)
from rhosocial.activerecord.backend.expression.types import DataType
from ...expression.ddl.type import (
    PostgresAddEnumValueAction,
    PostgresAddTypeAttributeAction,
    PostgresAlterTypeAttributeAction,
    PostgresBaseTypeDefinition,
    PostgresChangeTypeOwnerAction,
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

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from ...expression.ddl.type import PostgresCompositeTypeAttribute


__all__ = ["PostgresTypeMixin"]


class PostgresTypeMixin(UserDefinedTypeMixin):
    """PostgreSQL user-defined TYPE DDL support."""

    if TYPE_CHECKING:
        name: str
        version: Tuple[int, int, int]

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str: ...

        def inline_sql_literal(self, value: object) -> str: ...

    def _as_dialect(self) -> "SQLDialectBase":
        return cast("SQLDialectBase", self)

    _POSTGRES_TYPE_DEFINITIONS = (
        PostgresCompositeTypeDefinition,
        PostgresEnumTypeDefinition,
        PostgresRangeTypeDefinition,
        PostgresBaseTypeDefinition,
        PostgresShellTypeDefinition,
    )
    _POSTGRES_TYPE_ACTIONS = (
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
    )
    _COMBINABLE_TYPE_ACTIONS = (
        PostgresAddTypeAttributeAction,
        PostgresDropTypeAttributeAction,
        PostgresAlterTypeAttributeAction,
    )

    def _format_qualified_identifier(self, name: str, schema: Optional[str] = None) -> str:
        value = f"{schema}.{name}" if schema is not None else name
        parts = value.split(".")
        if not parts or any(not part.strip() for part in parts):
            raise ValueError("PostgreSQL identifiers must contain non-empty segments")
        return ".".join(self.format_identifier(part) for part in parts)

    def _format_type_data_type(self, data_type: DataType) -> str:
        if not isinstance(data_type, DataType):
            raise TypeError(
                f"data_type must be a DataType instance, got {type(data_type).__name__}"
            )
        sql, params = data_type.to_sql()
        if params:
            raise ValueError("TYPE data types must render without bind parameters")
        return sql

    def _format_type_behavior(self, cascade: bool, restrict: bool) -> str:
        if cascade and restrict:
            raise ValueError("CASCADE and RESTRICT are mutually exclusive")
        if cascade:
            return "CASCADE"
        if restrict:
            return "RESTRICT"
        return ""

    def _format_postgres_composite_attribute(
        self,
        expr: "PostgresCompositeTypeAttribute",
    ) -> str:
        parts = [
            self.format_identifier(expr.name),
            self._format_type_data_type(expr.data_type),
        ]
        if expr.collation is not None:
            parts.append(f"COLLATE {self._format_qualified_identifier(expr.collation)}")
        return " ".join(parts)

    def _format_postgres_enum_value(self, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("ENUM labels must be strings")
        return self.inline_sql_literal(value)

    def _format_postgres_base_type_definition(
        self,
        expr: PostgresBaseTypeDefinition,
    ) -> str:
        if expr.subscript_function is not None and self.version < (14, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "CREATE TYPE base SUBSCRIPT property",
                suggestion="requires PostgreSQL 14+",
            )
        clauses: List[str] = []
        function_values = (
            ("INPUT", expr.input_function),
            ("OUTPUT", expr.output_function),
            ("RECEIVE", expr.receive_function),
            ("SEND", expr.send_function),
            ("TYPMOD_IN", expr.type_modifier_input_function),
            ("TYPMOD_OUT", expr.type_modifier_output_function),
            ("ANALYZE", expr.analyze_function),
            ("SUBSCRIPT", expr.subscript_function),
        )
        for keyword, function_name in function_values:
            if function_name is not None:
                clauses.append(f"{keyword} = {self._format_qualified_identifier(function_name)}")
        if expr.internallength is not None:
            value = "VARIABLE" if isinstance(expr.internallength, str) else str(expr.internallength)
            clauses.append(f"INTERNALLENGTH = {value}")
        if expr.passedbyvalue:
            clauses.append("PASSEDBYVALUE")
        if expr.alignment is not None:
            clauses.append(f"ALIGNMENT = {self.format_identifier(expr.alignment)}")
        if expr.storage is not None:
            clauses.append(f"STORAGE = {self.format_identifier(expr.storage)}")
        if expr.like_type is not None:
            clauses.append(f"LIKE = {self._format_type_data_type(expr.like_type)}")
        if expr.category is not None:
            clauses.append(f"CATEGORY = {self.format_identifier(expr.category)}")
        if expr.preferred is not None:
            clauses.append(f"PREFERRED = {'TRUE' if expr.preferred else 'FALSE'}")
        if expr.has_default:
            clauses.append(f"DEFAULT = {self.inline_sql_literal(expr.default)}")
        if expr.element is not None:
            clauses.append(f"ELEMENT = {self._format_type_data_type(expr.element)}")
        if expr.delimiter is not None:
            clauses.append(f"DELIMITER = {self.inline_sql_literal(expr.delimiter)}")
        if expr.collatable is not None:
            clauses.append(f"COLLATABLE = {'TRUE' if expr.collatable else 'FALSE'}")
        return f"({', '.join(clauses)})"

    def supports_type_objects(self) -> bool:
        return self.version >= (9, 6, 0)

    def supports_create_type(self) -> bool:
        return self.supports_type_objects()

    def supports_alter_type(self) -> bool:
        return self.supports_type_objects()

    def supports_drop_type(self) -> bool:
        return self.supports_type_objects()

    def supported_type_definitions(self) -> Tuple[Type[TypeDefinition], ...]:
        if not self.supports_type_objects():
            return ()
        return self._POSTGRES_TYPE_DEFINITIONS

    def supports_type_definition(
        self,
        definition_type: Type[TypeDefinition],
    ) -> bool:
        if not self.supports_type_objects():
            return False
        try:
            return any(
                issubclass(definition_type, supported)
                for supported in self.supported_type_definitions()
            )
        except TypeError:
            return False

    def supports_type_alter_action(
        self,
        action_type: Type[TypeAlterAction],
    ) -> bool:
        if not self.supports_type_objects():
            return False
        try:
            if not issubclass(action_type, self._POSTGRES_TYPE_ACTIONS):
                return False
        except TypeError:
            return False
        if issubclass(action_type, PostgresRenameEnumValueAction):
            return self.version >= (10, 0, 0)
        if issubclass(action_type, PostgresSetTypePropertiesAction):
            return self.version >= (13, 0, 0)
        return True

    def supports_create_type_if_not_exists(self) -> bool:
        return False

    def supports_create_type_or_replace(self) -> bool:
        return False

    def supports_alter_type_if_exists(self) -> bool:
        return False

    def supports_drop_type_if_exists(self) -> bool:
        return self.supports_type_objects()

    def supports_multiple_type_alter_actions(self) -> bool:
        return self.supports_type_objects()

    def supports_drop_type_cascade(self) -> bool:
        return self.supports_type_objects()

    def supports_drop_type_restrict(self) -> bool:
        return self.supports_type_objects()

    def supports_type_if_not_exists(self) -> bool:
        return False

    def supports_type_if_exists(self) -> bool:
        return self.supports_drop_type_if_exists()

    def supports_type_cascade(self) -> bool:
        return self.supports_drop_type_cascade()

    def format_create_type_enum_statement(
        self,
        name: str,
        values: Sequence[str],
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        dialect = self._as_dialect()
        expr = CreateTypeExpression(
            dialect,
            name,
            PostgresEnumTypeDefinition(dialect, values),
            schema_name=schema,
        )
        return self.format_create_type_statement(expr)

    def format_create_type_statement(
        self,
        expr: CreateTypeExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_type_objects() or not self.supports_create_type():
            raise UnsupportedFeatureError(self.name, "CREATE TYPE")
        if expr.if_not_exists and not self.supports_create_type_if_not_exists():
            raise UnsupportedFeatureError(self.name, "CREATE TYPE IF NOT EXISTS")
        if expr.or_replace and not self.supports_create_type_or_replace():
            raise UnsupportedFeatureError(self.name, "CREATE OR REPLACE TYPE")
        if not self.supports_type_definition(type(expr.definition)):
            raise UnsupportedFeatureError(
                self.name,
                f"TYPE definition {expr.definition.definition_kind}",
            )
        definition_sql, definition_params = expr.definition.to_sql()
        parts = ["CREATE"]
        if expr.or_replace:
            parts.append("OR REPLACE")
        parts.append("TYPE")
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")
        parts.append(self._format_qualified_identifier(expr.type_name, expr.schema_name))
        if definition_sql:
            parts.append(definition_sql)
        return " ".join(parts), tuple(definition_params)

    def _validate_type_action_version(self, expr: TypeAlterAction) -> None:
        if isinstance(expr, PostgresRenameEnumValueAction) and self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TYPE RENAME VALUE",
                suggestion="requires PostgreSQL 10+",
            )
        if isinstance(expr, PostgresSetTypePropertiesAction) and self.version < (13, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TYPE SET properties",
                suggestion="requires PostgreSQL 13+",
            )

    def format_alter_type_statement(
        self,
        expr: AlterTypeExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_type_objects() or not self.supports_alter_type():
            raise UnsupportedFeatureError(self.name, "ALTER TYPE")
        if expr.if_exists and not self.supports_alter_type_if_exists():
            raise UnsupportedFeatureError(self.name, "ALTER TYPE IF EXISTS")
        if len(expr.actions) > 1 and not self.supports_multiple_type_alter_actions():
            raise UnsupportedFeatureError(self.name, "multiple ALTER TYPE actions")
        if len(expr.actions) > 1 and any(
            not isinstance(action, self._COMBINABLE_TYPE_ACTIONS)
            for action in expr.actions
        ):
            raise UnsupportedFeatureError(
                self.name,
                "multiple ALTER TYPE actions",
                suggestion="PostgreSQL combines only composite attribute actions",
            )
        action_parts: List[str] = []
        action_params: List[object] = []
        for action in expr.actions:
            self._validate_type_action_version(action)
            if not self.supports_type_alter_action(type(action)):
                raise UnsupportedFeatureError(
                    self.name,
                    f"ALTER TYPE action {action.action_kind}",
                )
            action_sql, params = action.to_sql()
            action_parts.append(action_sql)
            action_params.extend(params)
        parts = ["ALTER TYPE"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_qualified_identifier(expr.type_name, expr.schema_name))
        parts.append(", ".join(action_parts))
        return " ".join(parts), tuple(action_params)

    def format_drop_type_statement(
        self,
        expr: Optional[Union[DropTypeExpression, str]] = None,
        schema_name: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
        *,
        schema: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        if expr is not None and name is not None:
            raise ValueError("expr and name are mutually exclusive")
        if expr is None:
            if name is None:
                raise TypeError("expr or name is required")
            expr = name
        if isinstance(expr, str):
            if schema is not None and schema_name is not None and schema != schema_name:
                raise ValueError("schema and schema_name must match when both are provided")
            resolved_schema = schema if schema is not None else schema_name
            expr = PostgresDropTypeExpression(
                self._as_dialect(),
                expr,
                schema_name=resolved_schema,
                if_exists=if_exists,
                cascade=cascade,
                restrict=restrict,
            )
        if not self.supports_type_objects() or not self.supports_drop_type():
            raise UnsupportedFeatureError(self.name, "DROP TYPE")
        if expr.if_exists and not self.supports_drop_type_if_exists():
            raise UnsupportedFeatureError(self.name, "DROP TYPE IF EXISTS")
        behavior = self._format_type_behavior(
            getattr(expr, "cascade", False),
            getattr(expr, "restrict", False),
        )
        if getattr(expr, "cascade", False) and not self.supports_drop_type_cascade():
            raise UnsupportedFeatureError(self.name, "DROP TYPE CASCADE")
        if getattr(expr, "restrict", False) and not self.supports_drop_type_restrict():
            raise UnsupportedFeatureError(self.name, "DROP TYPE RESTRICT")
        parts = ["DROP TYPE"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_qualified_identifier(expr.type_name, expr.schema_name))
        if behavior:
            parts.append(behavior)
        return " ".join(parts), ()

    def format_type_definition(
        self,
        expr: TypeDefinition,
    ) -> Tuple[str, tuple]:
        if not self.supports_type_definition(type(expr)):
            raise UnsupportedFeatureError(
                self.name,
                f"TYPE definition {getattr(expr, 'definition_kind', type(expr).__name__)}",
            )
        if isinstance(expr, PostgresCompositeTypeDefinition):
            attributes = ", ".join(
                self._format_postgres_composite_attribute(attribute)
                for attribute in expr.attributes
            )
            return f"AS ({attributes})", ()
        if isinstance(expr, PostgresEnumTypeDefinition):
            labels = ", ".join(
                self._format_postgres_enum_value(label) for label in expr.labels
            )
            return f"AS ENUM ({labels})", ()
        if isinstance(expr, PostgresRangeTypeDefinition):
            if expr.multirange_type_name is not None and self.version < (14, 0, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE TYPE MULTIRANGE_TYPE_NAME",
                    suggestion="requires PostgreSQL 14+",
                )
            clauses = [f"SUBTYPE = {self._format_type_data_type(expr.subtype)}"]
            optional_values = (
                ("SUBTYPE_OPCLASS", expr.subtype_operator_class),
                ("COLLATION", expr.collation),
                ("CANONICAL", expr.canonical_function),
                ("SUBTYPE_DIFF", expr.subtype_diff_function),
                ("MULTIRANGE_TYPE_NAME", expr.multirange_type_name),
            )
            for keyword, value in optional_values:
                if value is not None:
                    clauses.append(f"{keyword} = {self._format_qualified_identifier(value)}")
            return f"AS RANGE ({', '.join(clauses)})", ()
        if isinstance(expr, PostgresBaseTypeDefinition):
            return self._format_postgres_base_type_definition(expr), ()
        if isinstance(expr, PostgresShellTypeDefinition):
            return "", ()
        raise UnsupportedFeatureError(
            self.name,
            f"TYPE definition {getattr(expr, 'definition_kind', type(expr).__name__)}",
        )

    def format_type_alter_action(
        self,
        expr: TypeAlterAction,
    ) -> Tuple[str, tuple]:
        self._validate_type_action_version(expr)
        if not self.supports_type_alter_action(type(expr)):
            raise UnsupportedFeatureError(
                self.name,
                f"ALTER TYPE action {getattr(expr, 'action_kind', type(expr).__name__)}",
            )
        if isinstance(expr, PostgresRenameTypeAction):
            return f"RENAME TO {self.format_identifier(expr.new_name)}", ()
        if isinstance(expr, PostgresSetTypeSchemaAction):
            return f"SET SCHEMA {self.format_identifier(expr.new_schema)}", ()
        if isinstance(expr, PostgresChangeTypeOwnerAction):
            return f"OWNER TO {self.format_identifier(expr.new_owner)}", ()
        if isinstance(expr, PostgresRenameTypeAttributeAction):
            behavior = self._format_type_behavior(expr.cascade, expr.restrict)
            sql = (
                f"RENAME ATTRIBUTE {self.format_identifier(expr.attribute_name)} "
                f"TO {self.format_identifier(expr.new_name)}"
            )
            return (f"{sql} {behavior}" if behavior else sql), ()
        if isinstance(expr, PostgresAddTypeAttributeAction):
            parts = [
                "ADD ATTRIBUTE",
                self.format_identifier(expr.attribute_name),
                self._format_type_data_type(expr.data_type),
            ]
            if expr.collation is not None:
                parts.append(f"COLLATE {self._format_qualified_identifier(expr.collation)}")
            behavior = self._format_type_behavior(expr.cascade, expr.restrict)
            if behavior:
                parts.append(behavior)
            return " ".join(parts), ()
        if isinstance(expr, PostgresDropTypeAttributeAction):
            parts = ["DROP ATTRIBUTE"]
            if expr.if_exists:
                parts.append("IF EXISTS")
            parts.append(self.format_identifier(expr.attribute_name))
            behavior = self._format_type_behavior(expr.cascade, expr.restrict)
            if behavior:
                parts.append(behavior)
            return " ".join(parts), ()
        if isinstance(expr, PostgresAlterTypeAttributeAction):
            type_keyword = "SET DATA TYPE" if expr.set_data else "TYPE"
            parts = [
                "ALTER ATTRIBUTE",
                self.format_identifier(expr.attribute_name),
                type_keyword,
                self._format_type_data_type(expr.data_type),
            ]
            if expr.collation is not None:
                parts.append(f"COLLATE {self._format_qualified_identifier(expr.collation)}")
            behavior = self._format_type_behavior(expr.cascade, expr.restrict)
            if behavior:
                parts.append(behavior)
            return " ".join(parts), ()
        if isinstance(expr, PostgresAddEnumValueAction):
            if expr.if_not_exists and self.version < (9, 3, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER TYPE ADD VALUE IF NOT EXISTS",
                    suggestion="requires PostgreSQL 9.3+",
                )
            parts = ["ADD VALUE"]
            if expr.if_not_exists:
                parts.append("IF NOT EXISTS")
            parts.append(self._format_postgres_enum_value(expr.new_value))
            if expr.before is not None:
                parts.extend(("BEFORE", self._format_postgres_enum_value(expr.before)))
            elif expr.after is not None:
                parts.extend(("AFTER", self._format_postgres_enum_value(expr.after)))
            return " ".join(parts), ()
        if isinstance(expr, PostgresRenameEnumValueAction):
            return (
                "RENAME VALUE "
                f"{self._format_postgres_enum_value(expr.old_value)} "
                f"TO {self._format_postgres_enum_value(expr.new_value)}"
            ), ()
        if isinstance(expr, PostgresSetTypePropertiesAction):
            if self.version < (13, 0, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER TYPE SET properties",
                    suggestion="requires PostgreSQL 13+",
                )
            if "SUBSCRIPT" in expr.properties and self.version < (14, 0, 0):
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER TYPE SET SUBSCRIPT",
                    suggestion="requires PostgreSQL 14+",
                )
            clauses: List[str] = []
            for name, value in expr.properties.items():
                if name == "STORAGE":
                    if value is None:
                        raise ValueError("STORAGE property cannot be None")
                    rendered_value = self.format_identifier(value)
                else:
                    rendered_value = "NONE" if value is None else self._format_qualified_identifier(value)
                clauses.append(f"{name} = {rendered_value}")
            return f"SET ({', '.join(clauses)})", ()
        raise UnsupportedFeatureError(
            self.name,
            f"ALTER TYPE action {getattr(expr, 'action_kind', type(expr).__name__)}",
        )
