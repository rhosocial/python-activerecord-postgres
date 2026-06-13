# src/rhosocial/activerecord/backend/impl/postgres/protocols/ordered_set_aggregation.py
"""PostgreSQL ordered set aggregation feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresOrderedSetAggSupport(Protocol):
    """PostgreSQL ordered set aggregation feature support protocol."""

    def supports_ordered_set_aggregation(self)-> bool: ...
