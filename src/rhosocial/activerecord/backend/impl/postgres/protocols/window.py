# src/rhosocial/activerecord/backend/impl/postgres/protocols/window.py
"""PostgreSQL window feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresWindowSupport(Protocol):
    """PostgreSQL window feature support protocol."""

    def supports_window_functions(self)-> bool: ...

    def supports_window_frame_clause(self)-> bool: ...
