# src/rhosocial/activerecord/backend/impl/postgres/functions/ddl_type.py
"""TYPE DDL factories returning expression nodes."""

from typing import Optional, Sequence

from rhosocial.activerecord.backend.expression.objects import Type

from ..expression.ddl.type import (
    PostgresCreateEnumTypeExpression,
    PostgresDropTypeExpression,
)


def create_type_enum(
    dialect,
    name: str,
    values: Sequence[str],
    schema: Optional[str] = None,
    *,
    if_not_exists: bool = False,
) -> PostgresCreateEnumTypeExpression:
    """Build CREATE TYPE ... AS ENUM from scalar arguments."""
    return PostgresCreateEnumTypeExpression(
        dialect, name, values, schema=schema, if_not_exists=if_not_exists
    )


def drop_type(
    dialect,
    name: str,
    schema: Optional[str] = None,
    *,
    if_exists: bool = False,
    cascade: bool = False,
    restrict: bool = False,
    schema_name: Optional[str] = None,
) -> PostgresDropTypeExpression:
    """Build DROP TYPE from scalar arguments."""
    if schema is not None and schema_name is not None and schema != schema_name:
        raise ValueError("schema and schema_name must match when both are provided")
    resolved = schema if schema is not None else schema_name
    return PostgresDropTypeExpression(
        dialect,
        Type(dialect, name, schema_name=resolved),
        if_exists=if_exists,
        cascade=cascade,
        restrict=restrict,
    )


__all__ = ["create_type_enum", "drop_type"]
