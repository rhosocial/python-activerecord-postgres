# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/type.py
"""PostgreSQL user-defined TYPE DDL expressions."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Sequence, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Type
from rhosocial.activerecord.backend.expression.statements.ddl_type import (
    AlterTypeStatement,
    CreateTypeStatement,
    DropTypeStatement,
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
    "PostgresEnumTypeNameExpression",
    "PostgresEnumValuesExpression",
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
    """One attribute in a PostgreSQL composite TYPE definition.

    Not a node: it is a record the caller builds, and a node would have to
    declare a kind and a formatter for a shape ``CREATE TYPE`` only ever
    reads. What it has instead is a plain spelling -- :meth:`as_params` and
    :meth:`from_params` -- because a constructor parameter has to be able to
    travel in an artifact, and a dataclass instance has no language-neutral
    encoding of its own (rules §10: two parameter shapes, scalars and nodes;
    a host object with no wire form is the third, and it is refused).
    """

    name: str
    data_type: DataType
    collation: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_name(self.name, "name")
        _require_data_type(self.data_type, "data_type")
        if self.collation is not None:
            _validate_qualified_name(self.collation, "collation")

    def as_params(self) -> Dict[str, Any]:
        """This attribute as the plain mapping an artifact carries."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "collation": self.collation,
        }

    @classmethod
    def from_params(cls, params: Mapping[str, Any]) -> "PostgresCompositeTypeAttribute":
        """Rebuild an attribute from the mapping :meth:`as_params` wrote.

        The wide entry is the point: an artifact arrives as a mapping and a
        call site hands over a dataclass, while the stored form is the
        dataclass either way (rules §10, "the entry may be wide, the storage is
        normalized"). ``__post_init__`` validates, so a mapping missing a
        field or carrying an unknown one fails here by name rather than
        somewhere downstream.
        """
        unexpected = sorted(set(params) - {"name", "data_type", "collation"})
        if unexpected:
            raise TypeError(
                f"a composite type attribute carries no field(s) {unexpected}; "
                f"it declares 'name', 'data_type' and 'collation'"
            )
        missing = sorted({"name", "data_type"} - set(params))
        if missing:
            raise TypeError(
                f"a composite type attribute is missing required field(s) {missing}"
            )
        return cls(
            name=params["name"],
            data_type=params["data_type"],
            collation=params.get("collation"),
        )


class PostgresCompositeTypeDefinition(TypeDefinition):
    """Composite TYPE definition."""

    definition_kind = "composite"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        attributes: Sequence[Union[PostgresCompositeTypeAttribute, Mapping[str, Any]]],
    ) -> None:
        super().__init__(dialect)
        items = [
            attribute
            if isinstance(attribute, PostgresCompositeTypeAttribute)
            else PostgresCompositeTypeAttribute.from_params(attribute)
            for attribute in (attributes or [])
        ]
        if any(not isinstance(item, PostgresCompositeTypeAttribute) for item in items):
            raise TypeError("attributes must contain PostgresCompositeTypeAttribute instances")
        self.attributes = items

    def get_params(self) -> Dict[str, Any]:
        """The inherited parameter view, with ``attributes`` in its wire shape.

        The inherited version reads ``self.attributes`` off by name and hands
        back the dataclass instances, which the artifact codec cannot write: a
        dataclass is a host object with no language-neutral spelling, and the
        codec refuses it by name rather than inventing one. Converting here
        keeps the conversion on the node that owns the parameter -- its
        constructor is where a mapping becomes an attribute again.
        """
        params = super().get_params()
        params["attributes"] = [item.as_params() for item in self.attributes]
        return params


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


class _UnsetValue(str, Enum):
    """The "this TYPE clause was not supplied" sentinel, as a writable enum.

    ``str`` as the base and ``str.__str__`` as the spelling, because that is
    what makes it an artifact value rather than a host object the codec has no
    wire form for: a plain ``Enum`` is refused by name on the way out, and a
    ``str`` member travels as ``string_value`` in every language's reader
    without a second spelling to reimplement (rules §11.3).
    """

    TOKEN = "UNSET"

    __str__ = str.__str__


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
        # The sentinel is this parameter's "not supplied" spelling, and an
        # artifact carries it as the text "UNSET" -- the enum is a ``str``, so
        # that is the same fact in a form every language's reader can parse.
        # The constructor is where the wire form becomes the stored form again
        # (rules §10, "the conversion happens in the constructor"); leaving the
        # text in place would make ``has_default`` answer True for a TYPE that
        # declared no default at all.
        #
        # The boundary this costs: a bare Python string ``"UNSET"`` passed as
        # the default is read as "not supplied". There is one spelling on the
        # wire, so the two cannot both have it; naming the sentinel after the
        # SQL keyword it stands for is worth more than the one pathological
        # DEFAULT it makes unreachable.
        if default == _UNSET:
            default = _UNSET
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


class PostgresDropTypeExpression(DropTypeStatement):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        type: Type,
        *,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(dialect, type, if_exists=if_exists)
        self.cascade = cascade
        self.restrict = restrict


class PostgresCreateEnumTypeExpression(CreateTypeStatement):
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
            Type(dialect, name, schema_name=schema),
            definition,
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
            Type(dialect, name, schema_name=schema),
            if_exists=if_exists,
            cascade=cascade,
            restrict=restrict,
        )
        self.name = name
        self.schema = schema


class PostgresAlterEnumAddValueExpression(AlterTypeStatement):
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
        super().__init__(
            dialect, Type(dialect, type_name, schema_name=schema), [action]
        )
        self.type_name = type_name
        self.new_value = new_value
        self.schema = schema
        self.before = before
        self.after = after
        self.if_not_exists = if_not_exists


class PostgresAlterEnumTypeAddValueExpression(PostgresAlterEnumAddValueExpression):
    pass


class PostgresAlterEnumTypeRenameValueExpression(AlterTypeStatement):
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
            Type(dialect, type_name, schema_name=schema),
            [PostgresRenameEnumValueAction(dialect, old_value, new_value)],
        )
        self.type_name = type_name
        self.old_value = old_value
        self.new_value = new_value
        self.schema = schema


class PostgresCreateRangeTypeExpression(CreateTypeStatement):
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
            Type(dialect, name, schema_name=schema),
            definition,
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


class PostgresEnumTypeNameExpression(BaseExpression):
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
        return "format_postgres_enum_type_name_expression"


class PostgresEnumValuesExpression(BaseExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        values: Sequence[str],
    ) -> None:
        super().__init__(dialect)
        self.values = list(values)

    @property
    def format_method(self) -> str:
        return "format_postgres_enum_values_expression"

