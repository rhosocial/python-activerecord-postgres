# src/rhosocial/activerecord/backend/impl/postgres/protocols/sequence.py
"""PostgreSQL sequence feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresSequenceSupport(Protocol):
    """PostgreSQL sequence feature support protocol."""

    def supports_create_sequence(self)-> bool: ...

    def supports_drop_sequence(self)-> bool: ...
