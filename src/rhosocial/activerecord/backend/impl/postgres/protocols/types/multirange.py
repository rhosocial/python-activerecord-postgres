# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/multirange.py
"""PostgreSQL multirange protocol."""

from typing import Optional, Protocol, Tuple, runtime_checkable


__all__ = ["PostgresMultirangeSupport"]


@runtime_checkable
class PostgresMultirangeSupport(Protocol):
    def supports_multirange(self) -> bool:
        ...

    def supports_multirange_constructor(self) -> bool:
        ...

    def supports_multirange_agg(self) -> bool:
        ...

    def format_create_multirange_type_statement(
        self,
        name: str,
        range_type: str,
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        ...

    def format_multirange_agg_function(
        self,
        range_column: str,
        table_name: str,
        where_clause: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        ...

    def format_create_multirange_type_statement_expression(self, expr: object) -> Tuple[str, tuple]:
        ...

    def format_multirange_agg_function_expression(self, expr: object) -> Tuple[str, tuple]:
        ...
