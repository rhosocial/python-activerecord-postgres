# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/stored_procedure.py
"""Stored-procedure protocol — capability probes and node renderers."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresStoredProcedureSupport(Protocol):
    """PostgreSQL stored procedure protocol."""

    def supports_call_statement(self) -> bool: ...

    def supports_stored_procedure_transaction_control(self) -> bool: ...

    def supports_sql_body_functions(self) -> bool: ...

    def format_postgres_create_procedure_statement(self, expr) -> tuple: ...

    def format_postgres_drop_procedure_statement(self, expr) -> tuple: ...

    def format_postgres_call_procedure_statement(self, expr) -> tuple: ...
