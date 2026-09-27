# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/domain.py
"""PostgreSQL DOMAIN DDL expressions."""

from __future__ import annotations

import warnings
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression, SQLPredicate
from rhosocial.activerecord.backend.expression.statements.ddl_domain import (
    AddDomainCheckAction,
    AlterDomainExpression,
    CreateDomainExpression,
    DomainAlterAction,
    DomainCheckConstraint,
    DomainNullability,
    DomainValueExpression,
    DropDomainCheckAction,
    DropDomainDefaultAction,
    DropDomainExpression,
    DropDomainNotNullAction,
    RenameDomainAction,
    SetDomainDefaultAction,
    SetDomainNotNullAction,
)
from rhosocial.activerecord.backend.expression.types import DataType

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "AlterDomainActionType",
    "DomainNullability",
    "DomainValueExpression",
    "DomainCheckConstraint",
    "SetDomainDefaultAction",
    "DropDomainDefaultAction",
    "SetDomainNotNullAction",
    "DropDomainNotNullAction",
    "PostgresAddDomainCheckAction",
    "PostgresDropDomainCheckAction",
    "PostgresRenameDomainConstraintAction",
    "PostgresValidateDomainConstraintAction",
    "PostgresChangeDomainOwnerAction",
    "PostgresSetDomainSchemaAction",
    "PostgresCreateDomainExpression",
    "PostgresAlterDomainExpression",
    "PostgresDropDomainExpression",
]


def _validate_name(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _coerce_legacy_data_type(
    dialect: "SQLDialectBase",
    value: Union[DataType, str],
) -> DataType:
    if isinstance(value, DataType):
        return value
    if not isinstance(value, str) or not value.strip():
        raise TypeError("data_type must be a DataType instance")
    warnings.warn(
        "Raw DOMAIN data_type strings are deprecated; pass a DataType instance",
        DeprecationWarning,
        stacklevel=3,
    )
    return DataType.parse_data_type_str(dialect, value)


class _LegacySqlExpression(BaseExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        sql: str,
        field_name: str,
    ) -> None:
        super().__init__(dialect)
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError(f"{field_name} must be a non-empty SQL string")
        self.sql = sql
        self.field_name = field_name

    @property
    def format_method(self) -> str:
        return "_format_postgres_legacy_sql_expression"


class _UnsupportedDomainAction(DomainAlterAction):
    action_kind = "unsupported"

    def __init__(self, dialect: "SQLDialectBase", action: Any) -> None:
        super().__init__(dialect)
        self.action = action

    @property
    def format_method(self) -> str:
        return "_format_postgres_unsupported_domain_action"


class AlterDomainActionType(Enum):
    SET_DEFAULT = "SET DEFAULT"
    DROP_DEFAULT = "DROP DEFAULT"
    RENAME_TO = "RENAME TO"


class PostgresAddDomainCheckAction(AddDomainCheckAction):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        check: Union[DomainCheckConstraint, SQLPredicate],
        *,
        not_valid: bool = False,
    ) -> None:
        if isinstance(check, SQLPredicate):
            check = DomainCheckConstraint(dialect, check)
        super().__init__(dialect, check)
        self.not_valid = not_valid


class PostgresDropDomainCheckAction(DropDomainCheckAction):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        *,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ) -> None:
        super().__init__(dialect, name=name)
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict


class PostgresRenameDomainConstraintAction(DomainAlterAction):
    action_kind = "rename_constraint"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        new_name: str,
    ) -> None:
        super().__init__(dialect)
        _validate_name(name, "name")
        _validate_name(new_name, "new_name")
        self.name = name
        self.new_name = new_name


class PostgresValidateDomainConstraintAction(DomainAlterAction):
    action_kind = "validate_constraint"

    def __init__(self, dialect: "SQLDialectBase", name: str) -> None:
        super().__init__(dialect)
        _validate_name(name, "name")
        self.name = name


class PostgresChangeDomainOwnerAction(DomainAlterAction):
    action_kind = "change_owner"

    def __init__(self, dialect: "SQLDialectBase", new_owner: str) -> None:
        super().__init__(dialect)
        _validate_name(new_owner, "new_owner")
        self.new_owner = new_owner


class PostgresSetDomainSchemaAction(DomainAlterAction):
    action_kind = "set_schema"

    def __init__(self, dialect: "SQLDialectBase", new_schema: str) -> None:
        super().__init__(dialect)
        _validate_name(new_schema, "new_schema")
        self.new_schema = new_schema


class PostgresCreateDomainExpression(CreateDomainExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        data_type: Union[DataType, str],
        schema: Optional[str] = None,
        collation: Optional[str] = None,
        default: Optional[Any] = None,
        constraints: Optional[Sequence[Union[str, DomainCheckConstraint, SQLPredicate]]] = None,
        *,
        nullability: DomainNullability = DomainNullability.UNSPECIFIED,
        checks: Optional[Sequence[Union[DomainCheckConstraint, SQLPredicate]]] = None,
        schema_name: Optional[str] = None,
    ) -> None:
        normalized_type = _coerce_legacy_data_type(dialect, data_type)
        resolved_schema = schema if schema is not None else schema_name
        if schema is not None and schema_name is not None and schema != schema_name:
            raise ValueError("schema and schema_name must match when both are provided")
        constraint_items: List[Any]
        if constraints is not None and checks is not None:
            raise ValueError("constraints and checks are mutually exclusive")
        if checks is not None:
            check_parameter = "checks"
            constraint_items = list(checks)
        elif constraints is not None:
            check_parameter = "constraints"
            constraint_items = list(constraints)
        else:
            check_parameter = None
            constraint_items = []
        clauses: List[BaseExpression] = []
        normalized_checks: List[DomainCheckConstraint] = []
        for item in constraint_items:
            if isinstance(item, str):
                warnings.warn(
                    "Raw DOMAIN constraint strings are deprecated; pass SQLPredicate "
                    "or DomainCheckConstraint instances",
                    DeprecationWarning,
                    stacklevel=2,
                )
                clauses.append(_LegacySqlExpression(dialect, item, "constraints"))
            elif isinstance(item, DomainCheckConstraint):
                clauses.append(item)
                normalized_checks.append(item)
            elif isinstance(item, SQLPredicate):
                check = DomainCheckConstraint(dialect, item)
                clauses.append(check)
                normalized_checks.append(check)
            else:
                raise TypeError(
                    "constraints must contain DomainCheckConstraint, SQLPredicate, or "
                    "legacy string instances"
                )
        normalized_default = default
        if isinstance(default, str):
            warnings.warn(
                "Raw DOMAIN DEFAULT strings are deprecated; pass a scalar or expression",
                DeprecationWarning,
                stacklevel=2,
            )
            normalized_default = _LegacySqlExpression(dialect, default, "default")
        super().__init__(
            dialect,
            name,
            normalized_type,
            default=normalized_default,
            nullability=nullability,
            checks=normalized_checks,
            collation=collation,
        )
        self.schema = resolved_schema
        self.schema_name = resolved_schema
        self.constraints = list(constraint_items)
        self._constraint_clauses = clauses
        self._check_parameter = check_parameter

    def get_params(self) -> Dict[str, Any]:
        params = super().get_params()
        params.pop("constraints", None)
        params.pop("checks", None)
        if self._check_parameter == "constraints":
            params["constraints"] = list(self.constraints)
        elif self._check_parameter == "checks":
            params["checks"] = list(self.checks)
        return params

    @property
    def name(self) -> str:
        return self.domain_name


class PostgresAlterDomainExpression(AlterDomainExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        action: Union[AlterDomainActionType, DomainAlterAction, Any],
        schema: Optional[str] = None,
        new_value: Any = None,
        new_name: Optional[str] = None,
        *,
        schema_name: Optional[str] = None,
    ) -> None:
        resolved_schema = schema if schema is not None else schema_name
        if schema is not None and schema_name is not None and schema != schema_name:
            raise ValueError("schema and schema_name must match when both are provided")
        if isinstance(action, DomainAlterAction):
            actions = [action]
        elif isinstance(action, AlterDomainActionType):
            if action is AlterDomainActionType.SET_DEFAULT:
                normalized_default: Any = new_value
                if isinstance(new_value, str):
                    warnings.warn(
                        "Raw ALTER DOMAIN DEFAULT strings are deprecated; pass a scalar "
                        "or expression",
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    normalized_default = _LegacySqlExpression(
                        dialect,
                        new_value,
                        "new_value",
                    )
                actions = [SetDomainDefaultAction(dialect, normalized_default)]
            elif action is AlterDomainActionType.DROP_DEFAULT:
                actions = [DropDomainDefaultAction(dialect)]
            else:
                if new_name is None:
                    raise ValueError("new_name is required for RENAME TO")
                actions = [RenameDomainAction(dialect, new_name)]
        else:
            actions = [_UnsupportedDomainAction(dialect, action)]
        super().__init__(dialect, name, actions)
        self.schema = resolved_schema
        self.schema_name = resolved_schema
        self.action = action
        self.new_value = new_value
        self.new_name = new_name

    @property
    def name(self) -> str:
        return self.domain_name


class PostgresDropDomainExpression(DropDomainExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
        *,
        schema_name: Optional[str] = None,
    ) -> None:
        resolved_schema = schema if schema is not None else schema_name
        if schema is not None and schema_name is not None and schema != schema_name:
            raise ValueError("schema and schema_name must match when both are provided")
        super().__init__(dialect, name)
        self.schema = resolved_schema
        self.schema_name = resolved_schema
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict

    @property
    def name(self) -> str:
        return self.domain_name
