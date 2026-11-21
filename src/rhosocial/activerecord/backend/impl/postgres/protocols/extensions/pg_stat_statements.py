# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_stat_statements.py
"""pg_stat_statements protocol — probes and the query renderer."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgStatStatementsSupport(Protocol):
    """pg_stat_statements capability protocol."""

    def supports_pg_stat_statements_view(self) -> bool: ...

    def supports_pg_stat_statements_reset(self) -> bool: ...

    def format_postgres_pg_stat_statement(self, expr) -> tuple: ...
