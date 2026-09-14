# src/rhosocial/activerecord/backend/impl/postgres/protocols/ilike.py
"""PostgreSQL ilike feature support protocol."""

from typing import Protocol, runtime_checkable, Tuple


@runtime_checkable
class PostgresILIKESupport(Protocol):
    """PostgreSQL ilike feature support protocol."""

    def supports_ilike(self) -> bool: ...

    def format_ilike_expression(
        self, column, pattern: str = "", negate: bool = False
    ) -> Tuple[str, tuple]: ...
