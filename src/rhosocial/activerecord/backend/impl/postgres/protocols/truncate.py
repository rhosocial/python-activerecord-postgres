# src/rhosocial/activerecord/backend/impl/postgres/protocols/truncate.py
"""PostgreSQL truncate feature support protocol."""

from typing import Any, Protocol, runtime_checkable, Tuple


@runtime_checkable
class PostgresTruncateSupport(Protocol):
    """PostgreSQL truncate feature support protocol."""

    def supports_truncate_restart_identity(self)-> bool: ...

    def supports_truncate_cascade(self)-> bool: ...

    def format_truncate_statement(self, expr: Any) -> Tuple[str, tuple]: ...
