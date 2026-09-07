# src/rhosocial/activerecord/backend/impl/postgres/protocols/sequence.py
"""PostgreSQL sequence feature support protocol."""

from typing import Any, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresSequenceSupport(Protocol):
    """PostgreSQL sequence feature support protocol."""

    def supports_create_sequence(self)-> bool: ...

    def supports_drop_sequence(self)-> bool: ...

    def format_nextval(self, expr: Any) -> Tuple[str, tuple]: ...

    def format_currval(self, expr: Any) -> Tuple[str, tuple]: ...
