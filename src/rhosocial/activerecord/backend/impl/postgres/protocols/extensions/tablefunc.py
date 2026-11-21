# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/tablefunc.py
"""tablefunc protocol — probes and typed-query renderers."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresTablefuncSupport(Protocol):
    """tablefunc capability protocol."""

    def supports_tablefunc_crosstab(self) -> bool: ...

    def supports_tablefunc_connectby(self) -> bool: ...

    def supports_tablefunc_normal_rand(self) -> bool: ...

    def format_postgres_crosstab_with_definition_statement(self, expr) -> tuple: ...

    def format_postgres_connectby_full_statement(self, expr) -> tuple: ...
