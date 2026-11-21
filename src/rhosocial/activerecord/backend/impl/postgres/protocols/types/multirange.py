# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/multirange.py
"""PostgreSQL multirange protocol — capability probes only."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresMultirangeSupport(Protocol):
    """PostgreSQL multirange capability protocol."""

    def supports_multirange(self) -> bool: ...

    def supports_multirange_constructor(self) -> bool: ...

    def supports_multirange_agg(self) -> bool: ...

    def format_postgres_create_multirange_type_expression(self, expr) -> tuple: ...

    def format_postgres_multirange_agg_function_expression(self, expr) -> tuple: ...
