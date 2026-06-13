# src/rhosocial/activerecord/backend/impl/postgres/protocols/lateral_join.py
"""PostgreSQL lateral join feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresLateralJoinSupport(Protocol):
    """PostgreSQL lateral join feature support protocol."""

    def supports_lateral_join(self)-> bool: ...
