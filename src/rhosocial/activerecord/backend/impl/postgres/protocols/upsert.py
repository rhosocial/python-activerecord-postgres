# src/rhosocial/activerecord/backend/impl/postgres/protocols/upsert.py
"""PostgreSQL upsert feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresUpsertSupport(Protocol):
    """PostgreSQL upsert feature support protocol."""

    def supports_upsert(self) -> bool: ...

    def supports_on_conflict_clause(self) -> bool: ...

    def supports_multiple_on_conflict_clauses(self) -> bool: ...
