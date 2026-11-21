# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/stored_procedure.py
"""Stored-procedure capability probes and node renderers (PostgreSQL 11+)."""

from typing import Optional, Tuple

from rhosocial.activerecord.backend.expression.objects import Procedure

from ...expression.ddl import (
    PostgresCallProcedureStatement,
    PostgresCreateProcedureStatement,
    PostgresDropProcedureStatement,
)


class PostgresStoredProcedureMixin:
    """PostgreSQL stored procedure implementation."""

    def _format_procedure_ref(self, schema: Optional[str], name: str) -> str:
        sql, _ = self.format_procedure_object(Procedure(self, name, schema_name=schema))
        return sql

    def supports_call_statement(self) -> bool:
        """CALL statement is supported since PostgreSQL 11."""
        return self.version_at_least((11, 0, 0))

    def supports_stored_procedure_transaction_control(self) -> bool:
        """Transaction control within procedures is supported since PostgreSQL 11."""
        return self.version_at_least((11, 0, 0))

    def supports_sql_body_functions(self) -> bool:
        """SQL-standard function body syntax is supported since PostgreSQL 14."""
        return self.version_at_least((14, 0, 0))

    def format_postgres_create_procedure_statement(
        self, expr: PostgresCreateProcedureStatement
    ) -> Tuple[str, tuple]:
        """Render a CREATE PROCEDURE node."""
        if not self.supports_call_statement():
            raise ValueError("CREATE PROCEDURE requires PostgreSQL 11+")

        full_name = self._format_procedure_ref(expr.schema, expr.name)
        replace_clause = "OR REPLACE " if expr.or_replace else ""
        sql = f"CREATE {replace_clause}PROCEDURE {full_name}"

        if expr.parameters:
            param_strs = []
            for param in expr.parameters:
                part = ""
                mode = param.get("mode", "IN")
                if mode and mode != "IN":
                    part += f"{mode} "
                part += f"{param['name']} {param['type']}"
                if "default" in param:
                    part += f" = {param['default']}"
                param_strs.append(part)
            sql += f"({', '.join(param_strs)})"
        else:
            sql += "()"

        sql += f"\nLANGUAGE {expr.language}"
        if expr.security:
            security = expr.security.upper()
            if security not in ("DEFINER", "INVOKER"):
                raise ValueError("Security must be 'DEFINER' or 'INVOKER'")
            sql += f"\nSECURITY {security}"
        if expr.set_params:
            sql += "\nSET " + ", ".join(
                f"{k} = {v}" for k, v in expr.set_params.items()
            )
        sql += f"\nAS $$\n{expr.body}\n$$"
        return sql, ()

    def format_postgres_drop_procedure_statement(
        self, expr: PostgresDropProcedureStatement
    ) -> Tuple[str, tuple]:
        full_name = self._format_procedure_ref(expr.schema, expr.name)
        exists_clause = "IF EXISTS " if expr.if_exists else ""
        sql = f"DROP PROCEDURE {exists_clause}{full_name}"
        if expr.parameters:
            types = [
                p["type"] if isinstance(p, dict) else p for p in expr.parameters
            ]
            sql += f"({', '.join(types)})"
        if expr.cascade:
            sql += " CASCADE"
        return sql, ()

    def format_postgres_call_procedure_statement(
        self, expr: PostgresCallProcedureStatement
    ) -> Tuple[str, tuple]:
        if not self.supports_call_statement():
            raise ValueError("CALL statement requires PostgreSQL 11+")
        full_name = self._format_procedure_ref(expr.schema, expr.name)
        if expr.arguments:
            placeholders = ", ".join([self.get_parameter_placeholder()] * len(expr.arguments))
            return f"CALL {full_name}({placeholders})", tuple(expr.arguments)
        return f"CALL {full_name}()", ()
