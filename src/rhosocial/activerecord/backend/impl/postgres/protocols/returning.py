# src/rhosocial/activerecord/backend/impl/postgres/protocols/returning.py
"""PostgreSQL returning feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresReturningSupport(Protocol):
    """PostgreSQL returning feature support protocol."""

    def supports_returning_insert(self)-> bool: ...

    def supports_returning_update(self)-> bool: ...

    def supports_returning_delete(self)-> bool: ...
