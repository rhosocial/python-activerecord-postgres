# src/rhosocial/activerecord/backend/impl/postgres/functions/ddl_type.py
"""TYPE DDL factories returning expression nodes."""

from typing import Optional, Sequence

from ..expression.statements.ddl import (
    PostgresCreateEnumTypeExpression,
    PostgresDropTypeNameExpression,
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
) -> PostgresDropTypeNameExpression:
    """Collect arguments for DROP TYPE; the formatter validates them."""
    return PostgresDropTypeNameExpression(
        dialect, name, schema=schema, schema_name=schema_name,
        if_exists=if_exists, cascade=cascade, restrict=restrict,
    )


__all__ = ["create_type_enum", "drop_type"]
