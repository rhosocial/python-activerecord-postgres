# src/rhosocial/activerecord/backend/impl/postgres/protocols/truncate.py
"""PostgreSQL truncate feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresTruncateSupport(Protocol):
    """PostgreSQL truncate feature support protocol."""

    def supports_truncate_restart_identity(self)-> bool: ...

    def supports_truncate_cascade(self)-> bool: ...
