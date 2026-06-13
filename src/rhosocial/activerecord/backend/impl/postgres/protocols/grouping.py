# src/rhosocial/activerecord/backend/impl/postgres/protocols/grouping.py
"""PostgreSQL grouping feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresGroupingSupport(Protocol):
    """PostgreSQL grouping feature support protocol."""

    def supports_rollup(self)-> bool: ...

    def supports_cube(self)-> bool: ...

    def supports_grouping_sets(self)-> bool: ...
