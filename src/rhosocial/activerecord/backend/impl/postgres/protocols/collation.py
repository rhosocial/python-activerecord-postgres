# src/rhosocial/activerecord/backend/impl/postgres/protocols/collation.py
"""PostgreSQL collation feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresCollationSupport(Protocol):
    """PostgreSQL collation feature support protocol."""

    def supports_collate_expression(self)-> bool: ...
