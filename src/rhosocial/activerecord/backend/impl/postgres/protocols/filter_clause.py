# src/rhosocial/activerecord/backend/impl/postgres/protocols/filter_clause.py
"""PostgreSQL filter clause feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresFilterSupport(Protocol):
    """PostgreSQL filter clause feature support protocol."""

    def supports_filter_clause(self)-> bool: ...
