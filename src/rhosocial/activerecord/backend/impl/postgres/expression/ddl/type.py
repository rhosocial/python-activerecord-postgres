# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/type.py
"""PostgreSQL user-defined TYPE DDL expressions."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Sequence, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeExpression,
    CreateTypeExpression,
    DropTypeExpression,
    TypeAlterAction,
    TypeDefinition,
)
from rhosocial.activerecord.backend.expression.types import DataType

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresCompositeTypeAttribute",
    "PostgresCompositeTypeDefinition",
    "PostgresEnumTypeDefinition",
    "PostgresRangeTypeDefinition",
    "PostgresBaseTypeDefinition",
    "PostgresShellTypeDefinition",
    "PostgresRenameTypeAction",
    "PostgresSetTypeSchemaAction",
    "PostgresChangeTypeOwnerAction",
    "PostgresRenameTypeAttributeAction",
    "PostgresAddTypeAttributeAction",
    "PostgresDropTypeAttributeAction",
    "PostgresAlterTypeAttributeAction",
    "PostgresAddEnumValueAction",
    "PostgresRenameEnumValueAction",
    "PostgresSetTypePropertiesAction",
    "PostgresDropTypeExpression",
    "PostgresCreateEnumTypeExpression",
    "PostgresDropEnumTypeExpression",
    "PostgresAlterEnumAddValueExpression",
    "PostgresAlterEnumTypeAddValueExpression",
    "PostgresAlterEnumTypeRenameValueExpression",
    "PostgresCreateRangeTypeExpression",
    "EnumTypeNameExpression",
    "EnumValuesExpression",
    "CreateEnumTypeExpression",
    "DropEnumTypeExpression",
    "AlterEnumAddValueExpression",
]


def _validate_name(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _validate_qualified_name(value: str, field_name: str) -> None:
    _validate_name(value, field_name)
    if any(not part.strip() for part in value.split(".")):
        raise ValueError(f"{field_name} must contain non-empty identifier segments")


def _validate_label(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")


def _require_data_type(value: DataType, field_name: str) -> None:
    if not isinstance(value, DataType):
        raise TypeError(
            f"{field_name} must be a DataType instance, got {type(value).__name__}"
        )


def _coerce_legacy_data_type(
    dialect: "SQLDialectBase",
    value: Union[DataType, str],
    field_name: str,
) -> DataType:
    if isinstance(value, DataType):
        return value
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field_name} must be a DataType instance")
    warnings.warn(
        f"Raw {field_name} strings are deprecated; pass a DataType instance",
        DeprecationWarning,
        stacklevel=3,
    )
    return DataType.parse_data_type_str(dialect, value)


@dataclass
class PostgresCompositeTypeAttribute:
    """One attribute in a PostgreSQL composite TYPE definition."""

    name: str
    data_type: DataType
    collation: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_name(self.name, "name")
        _require_data_type(self.data_type, "data_type")
        if self.collation is not None:
            _validate_qualified_name(self.collation, "collation")


class PostgresCompositeTypeDefinition(TypeDefinition):
    """Composite TYPE definition."""

    definition_kind = "composite"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        attributes: Sequence[PostgresCompositeTypeAttribute],
    ) -> None:
        super().__init__(dialect)
        items = list(attributes or [])
        if any(not isinstance(item, PostgresCompositeTypeAttribute) for item in items):
            raise TypeError("attributes must contain PostgresCompositeTypeAttribute instances")
        self.attributes = items


class PostgresEnumTypeDefinition(TypeDefinition):
    """ENUM TYPE definition."""

    definition_kind = "enum"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        labels: Sequence[str],
    ) -> None:
        super().__init__(dialect)
        items = list(labels or [])
        for label in items:
            _validate_label(label, "labels")
        if len(items) != len(set(items)):
            raise ValueError("ENUM labels must be unique")
        self.labels = items


class PostgresRangeTypeDefinition(TypeDefinition):
    """RANGE TYPE definition."""

    definition_kind = "range"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        subtype: DataType,
        *,
        subtype_operator_class: Optional[str] = None,
        collation: Optional[str] = None,
        canonical_function: Optional[str] = None,
        subtype_diff_function: Optional[str] = None,
        multirange_type_name: Optional[str] = None,
    ) -> None:
        super().__init__(dialect)
        _require_data_type(subtype, "subtype")
        optional_names = {
            "subtype_operator_class": subtype_operator_class,
            "collation": collation,
            "canonical_function": canonical_function,
            "subtype_diff_function": subtype_diff_function,
            "multirange_type_name": multirange_type_name,
        }
        for field_name, value in optional_names.items():
            if value is not None:
                _validate_qualified_name(value, field_name)
        self.subtype = subtype
        self.subtype_operator_class = subtype_operator_class
        self.collation = collation
        self.canonical_function = canonical_function
        self.subtype_diff_function = subtype_diff_function
        self.multirange_type_name = multirange_type_name


class _UnsetValue(Enum):
    TOKEN = "UNSET"


_UNSET = _UnsetValue.TOKEN


class PostgresBaseTypeDefinition(TypeDefinition):
    """Base TYPE definition."""

    definition_kind = "base"

    _ALLOWED_ALIGNMENTS = frozenset({"char", "int2", "int4", "double"})
    _ALLOWED_STORAGE = frozenset({"plain", "external", "extended", "main"})

    def __init__(
        self,
        dialect: "SQLDialectBase",
        *,
        input_function: Optional[str] = None,
        output_function: Optional[str] = None,
        receive_function: Optional[str] = None,
        send_function: Optional[str] = None,
        type_modifier_input_function: Optional[str] = None,
        type_modifier_output_function: Optional[str] = None,
        analyze_function: Optional[str] = None,
        subscript_function: Optional[str] = None,
        internallength: Optional[Union[int, str]] = None,
        passedbyvalue: bool = False,
        alignment: Optional[str] = None,
        storage: Optional[str] = None,
        like_type: Optional[DataType] = None,
        category: Optional[str] = None,
        preferred: Optional[bool] = None,
        default: Any = _UNSET,
        element: Optional[DataType] = None,
        delimiter: Optional[str] = None,
        collatable: Optional[bool] = None,
    ) -> None:
        super().__init__(dialect)
        function_names = {
            "input_function": input_function,
            "output_function": output_function,
            "receive_function": receive_function,
            "send_function": send_function,
            "type_modifier_input_function": type_modifier_input_function,
            "type_modifier_output_function": type_modifier_output_function,
            "analyze_function": analyze_function,
            "subscript_function": subscript_function,
        }
        for field_name, value in function_names.items():
            if value is not None:
                _validate_qualified_name(value, field_name)
        if input_function is None or output_function is None:
            raise ValueError("input_function and output_function are required")
        if like_type is not None:
            _require_data_type(like_type, "like_type")
        if internallength is not None:
            if isinstance(internallength, bool):
                raise TypeError("internallength must be a positive integer or VARIABLE")
            if isinstance(internallength, int) and internallength <= 0:
                raise ValueError("internallength must be a positive integer or VARIABLE")
            if isinstance(internallength, str) and internallength.upper() != "VARIABLE":
                raise ValueError("internallength must be a positive integer or VARIABLE")
        if alignment is not None and alignment.lower() not in self._ALLOWED_ALIGNMENTS:
            raise ValueError("alignment must be char, int2, int4, or double")
        if storage is not None and storage.lower() not in self._ALLOWED_STORAGE:
            raise ValueError("storage must be plain, external, extended, or main")
        if category is not None and (len(category) != 1 or not category.isascii()):
            raise ValueError("category must be a single ASCII character")
        if element is not None:
            _require_data_type(element, "element")
        if delimiter is not None and not isinstance(delimiter, str):
            raise TypeError("delimiter must be a string")
        self.input_function = input_function
        self.output_function = output_function
        self.receive_function = receive_function
        self.send_function = send_function
        self.type_modifier_input_function = type_modifier_input_function
        self.type_modifier_output_function = type_modifier_output_function
        self.analyze_function = analyze_function
        self.subscript_function = subscript_function
        self.internallength = internallength
        self.passedbyvalue = passedbyvalue
        self.alignment = alignment
        self.storage = storage
        self.like_type = like_type
        self.category = category
        self.preferred = preferred
        self.default = default
        self.element = element
        self.delimiter = delimiter
        self.collatable = collatable

    @property
    def has_default(self) -> bool:
        return self.default is not _UNSET


class PostgresShellTypeDefinition(TypeDefinition):
    """Shell TYPE definition."""

    definition_kind = "shell"


class PostgresRenameTypeAction(TypeAlterAction):
    action_kind = "rename"

    def __init__(self, dialect: "SQLDialectBase", new_name: str) -> None:
        super().__init__(dialect)
        _validate_name(new_name, "new_name")
        self.new_name = new_name


class PostgresSetTypeSchemaAction(TypeAlterAction):
    action_kind = "set_schema"

    def __init__(self, dialect: "SQLDialectBase", new_schema: str) -> None:
        super().__init__(dialect)
        _validate_name(new_schema, "new_schema")
        self.new_schema = new_schema


class PostgresChangeTypeOwnerAction(TypeAlterAction):
    action_kind = "change_owner"

    def __init__(self, dialect: "SQLDialectBase", new_owner: str) -> None:
        super().__init__(dialect)
        _validate_name(new_owner, "new_owner")
        self.new_owner = new_owner


class PostgresRenameTypeAttributeAction(TypeAlterAction):
    action_kind = "rename_attribute"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        attribute_name: str,
        new_name: str,
        *,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(dialect)
        _validate_name(attribute_name, "attribute_name")
        _validate_name(new_name, "new_name")
        self.attribute_name = attribute_name
        self.new_name = new_name
        self.cascade = cascade
        self.restrict = restrict


class PostgresAddTypeAttributeAction(TypeAlterAction):
    action_kind = "add_attribute"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        attribute_name: str,
        data_type: DataType,
        *,
        collation: Optional[str] = None,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(dialect)
        _validate_name(attribute_name, "attribute_name")
        _require_data_type(data_type, "data_type")
        if collation is not None:
            _validate_qualified_name(collation, "collation")
        self.attribute_name = attribute_name
        self.data_type = data_type
        self.collation = collation
        self.cascade = cascade
        self.restrict = restrict


class PostgresDropTypeAttributeAction(TypeAlterAction):
    action_kind = "drop_attribute"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        attribute_name: str,
        *,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(dialect)
        _validate_name(attribute_name, "attribute_name")
        self.attribute_name = attribute_name
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict


class PostgresAlterTypeAttributeAction(TypeAlterAction):
    action_kind = "alter_attribute"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        attribute_name: str,
        data_type: DataType,
        *,
        collation: Optional[str] = None,
        set_data: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(dialect)
        _validate_name(attribute_name, "attribute_name")
        _require_data_type(data_type, "data_type")
        if collation is not None:
            _validate_qualified_name(collation, "collation")
        self.attribute_name = attribute_name
        self.data_type = data_type
        self.collation = collation
        self.set_data = set_data
        self.cascade = cascade
        self.restrict = restrict


class PostgresAddEnumValueAction(TypeAlterAction):
    action_kind = "add_enum_value"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        new_value: str,
        *,
        before: Optional[str] = None,
        after: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        super().__init__(dialect)
        _validate_label(new_value, "new_value")
        if before is not None and after is not None:
            raise ValueError("before and after are mutually exclusive")
        if before is not None:
            _validate_label(before, "before")
        if after is not None:
            _validate_label(after, "after")
        self.new_value = new_value
        self.before = before
        self.after = after
        self.if_not_exists = if_not_exists


class PostgresRenameEnumValueAction(TypeAlterAction):
    action_kind = "rename_enum_value"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        old_value: str,
        new_value: str,
    ) -> None:
        super().__init__(dialect)
        _validate_label(old_value, "old_value")
        _validate_label(new_value, "new_value")
        self.old_value = old_value
        self.new_value = new_value


class PostgresSetTypePropertiesAction(TypeAlterAction):
    action_kind = "set_properties"

    _FUNCTION_PROPERTIES = frozenset(
        {
            "RECEIVE",
            "SEND",
            "TYPMOD_IN",
            "TYPMOD_OUT",
            "ANALYZE",
            "SUBSCRIPT",
        }
    )
    _ALLOWED_PROPERTIES = _FUNCTION_PROPERTIES | {"STORAGE"}

    def __init__(
        self,
        dialect: "SQLDialectBase",
        properties: Mapping[str, Optional[str]],
    ) -> None:
        super().__init__(dialect)
        normalized: Dict[str, Optional[str]] = {}
        for raw_name, value in dict(properties or {}).items():
            if not isinstance(raw_name, str) or not raw_name.strip():
                raise ValueError("property names must be non-empty strings")
            name = raw_name.upper()
            if name not in self._ALLOWED_PROPERTIES:
                raise ValueError(f"Unsupported ALTER TYPE property: {name}")
            if value is not None and not isinstance(value, str):
                raise TypeError("property values must be strings or None")
            if value is not None:
                _validate_qualified_name(value, name)
            if name == "STORAGE" and value is None:
                raise ValueError("STORAGE property cannot be None")
            normalized[name] = value
        if not normalized:
            raise ValueError("properties must contain at least one property")
        self.properties = normalized


class PostgresDropTypeExpression(DropTypeExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        type_name: str,
        *,
        schema_name: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(
            dialect,
            type_name,
            schema_name=schema_name,
            if_exists=if_exists,
        )
        self.cascade = cascade
        self.restrict = restrict


class PostgresCreateEnumTypeExpression(CreateTypeExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        values: Sequence[str],
        schema: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        definition = PostgresEnumTypeDefinition(dialect, values)
        super().__init__(
            dialect,
            name,
            definition,
            schema_name=schema,
            if_not_exists=if_not_exists,
        )
        self.name = name
        self.values = list(values)
        self.schema = schema


class PostgresDropEnumTypeExpression(PostgresDropTypeExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(
            dialect,
            name,
            schema_name=schema,
            if_exists=if_exists,
            cascade=cascade,
            restrict=restrict,
        )
        self.name = name
        self.schema = schema


class PostgresAlterEnumAddValueExpression(AlterTypeExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        if not isinstance(type_name, str) or not type_name.strip():
            raise ValueError("Enum type name cannot be empty")
        if not isinstance(new_value, str):
            raise TypeError("New value must be a string")
        if new_value == "":
            raise ValueError("New value cannot be empty")
        if before is not None and after is not None:
            raise ValueError("Cannot specify both 'before' and 'after'")
        action = PostgresAddEnumValueAction(
            dialect,
            new_value,
            before=before,
            after=after,
            if_not_exists=if_not_exists,
        )
        super().__init__(dialect, type_name, [action], schema_name=schema)
        self.type_name = type_name
        self.new_value = new_value
        self.schema = schema
        self.before = before
        self.after = after
        self.if_not_exists = if_not_exists


class PostgresAlterEnumTypeAddValueExpression(PostgresAlterEnumAddValueExpression):
    pass


class PostgresAlterEnumTypeRenameValueExpression(AlterTypeExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        type_name: str,
        old_value: str,
        new_value: str,
        schema: Optional[str] = None,
    ) -> None:
        if not isinstance(type_name, str) or not type_name.strip():
            raise ValueError("Enum type name cannot be empty")
        if not isinstance(old_value, str):
            raise TypeError("Old value must be a string")
        if not isinstance(new_value, str):
            raise TypeError("New value must be a string")
        if old_value == "":
            raise ValueError("Old value cannot be empty")
        if new_value == "":
            raise ValueError("New value cannot be empty")
        super().__init__(
            dialect,
            type_name,
            [PostgresRenameEnumValueAction(dialect, old_value, new_value)],
            schema_name=schema,
        )
        self.type_name = type_name
        self.old_value = old_value
        self.new_value = new_value
        self.schema = schema


class PostgresCreateRangeTypeExpression(CreateTypeExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        subtype: Union[DataType, str],
        schema: Optional[str] = None,
        subtype_opclass: Optional[str] = None,
        collation: Optional[str] = None,
        canonical: Optional[str] = None,
        subtype_diff: Optional[str] = None,
        if_not_exists: bool = False,
        multirange_type_name: Optional[str] = None,
    ) -> None:
        normalized_subtype = _coerce_legacy_data_type(dialect, subtype, "subtype")
        definition = PostgresRangeTypeDefinition(
            dialect,
            normalized_subtype,
            subtype_operator_class=subtype_opclass,
            collation=collation,
            canonical_function=canonical,
            subtype_diff_function=subtype_diff,
            multirange_type_name=multirange_type_name,
        )
        super().__init__(
            dialect,
            name,
            definition,
            schema_name=schema,
            if_not_exists=if_not_exists,
        )
        self.name = name
        self.subtype = subtype
        self.schema = schema
        self.subtype_opclass = subtype_opclass
        self.collation = collation
        self.canonical = canonical
        self.subtype_diff = subtype_diff
        self.multirange_type_name = multirange_type_name


class EnumTypeNameExpression(BaseExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
    ) -> None:
        super().__init__(dialect)
        self.name = name
        self.schema = schema

    @property
    def format_method(self) -> str:
        return "format_enum_type_name_expression"


class EnumValuesExpression(BaseExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        values: Sequence[str],
    ) -> None:
        super().__init__(dialect)
        self.values = list(values)

    @property
    def format_method(self) -> str:
        return "format_enum_values_expression"


class CreateEnumTypeExpression(PostgresCreateEnumTypeExpression):
    pass


class DropEnumTypeExpression(PostgresDropEnumTypeExpression):
    pass


class AlterEnumAddValueExpression(PostgresAlterEnumAddValueExpression):
    pass
