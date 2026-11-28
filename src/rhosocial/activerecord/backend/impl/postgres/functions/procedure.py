# src/rhosocial/activerecord/backend/impl/postgres/functions/procedure.py
"""Procedure statement factories — arbitrary arguments in, expression nodes out."""

from typing import Any, Dict, Optional, Sequence

from ..expression.statements.ddl import (
    PostgresCallProcedureStatement,
    PostgresCreateProcedureStatement,
    PostgresDropProcedureStatement,
)


def create_procedure(
    dialect,
    name: str,
    body: str = "",
    *,
    parameters: Optional[Sequence[Dict[str, Any]]] = None,
    returns: Optional[str] = None,
    language: str = "plpgsql",
    schema: Optional[str] = None,
    or_replace: bool = False,
    security: Optional[str] = None,
    cost: Optional[float] = None,
    rows: Optional[int] = None,
    set_params: Optional[Dict[str, str]] = None,
) -> PostgresCreateProcedureStatement:
    return PostgresCreateProcedureStatement(
        dialect, name, body,
        parameters=parameters, returns=returns, language=language, schema=schema,
        or_replace=or_replace, security=security, cost=cost, rows=rows,
        set_params=set_params,
    )


def drop_procedure(
    dialect,
    name: str,
    *,
    parameters: Optional[Sequence[Any]] = None,
    schema: Optional[str] = None,
    if_exists: bool = False,
    cascade: bool = False,
) -> PostgresDropProcedureStatement:
    return PostgresDropProcedureStatement(
        dialect, name, parameters=parameters, schema=schema,
        if_exists=if_exists, cascade=cascade,
    )


def call_procedure(
    dialect,
    name: str,
    *,
    arguments: Optional[Sequence[Any]] = None,
    schema: Optional[str] = None,
) -> PostgresCallProcedureStatement:
    return PostgresCallProcedureStatement(
        dialect, name, arguments=arguments, schema=schema
    )


__all__ = ["create_procedure", "drop_procedure", "call_procedure"]
