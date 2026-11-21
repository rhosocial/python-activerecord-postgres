# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/procedure.py
"""Procedure DDL/CALL nodes (PostgreSQL 11+)."""

from typing import Any, Dict, Optional, Sequence

from rhosocial.activerecord.backend.expression.bases import BaseExpression


class PostgresCreateProcedureStatement(BaseExpression):
    def __init__(
        self,
        dialect,
        name: str,
        body: str = "",
        *,
        parameters: Optional[Sequence[Dict[str, Any]]] = None,
        language: str = "plpgsql",
        schema: Optional[str] = None,
        or_replace: bool = False,
        security: Optional[str] = None,
        cost: Optional[float] = None,
        rows: Optional[int] = None,
        set_params: Optional[Dict[str, str]] = None,
        returns: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.body = body
        self.parameters = list(parameters or [])
        self.language = language
        self.schema = schema
        self.or_replace = or_replace
        self.security = security
        self.cost = cost
        self.rows = rows
        self.set_params = dict(set_params or {})
        self.returns = returns

    @property
    def format_method(self) -> str:
        return "format_postgres_create_procedure_statement"


class PostgresDropProcedureStatement(BaseExpression):
    def __init__(
        self,
        dialect,
        name: str,
        *,
        parameters: Optional[Sequence[Any]] = None,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
    ):
        super().__init__(dialect)
        self.name = name
        self.parameters = list(parameters or [])
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade

    @property
    def format_method(self) -> str:
        return "format_postgres_drop_procedure_statement"


class PostgresCallProcedureStatement(BaseExpression):
    def __init__(
        self,
        dialect,
        name: str,
        *,
        arguments: Optional[Sequence[Any]] = None,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.arguments = list(arguments or [])
        self.schema = schema

    @property
    def format_method(self) -> str:
        return "format_postgres_call_procedure_statement"


__all__ = [
    "PostgresCreateProcedureStatement",
    "PostgresDropProcedureStatement",
    "PostgresCallProcedureStatement",
]
