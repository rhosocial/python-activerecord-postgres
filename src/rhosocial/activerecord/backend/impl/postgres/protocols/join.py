# src/rhosocial/activerecord/backend/impl/postgres/protocols/join.py
"""PostgreSQL join feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresJoinSupport(Protocol):
    """PostgreSQL join feature support protocol."""

    def supports_right_join(self)-> bool: ...

    def supports_full_join(self)-> bool: ...

    def supports_wildcard(self)-> bool: ...
