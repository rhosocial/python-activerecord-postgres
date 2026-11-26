# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_logicalinspect.py
"""Protocol for the pg_logicalinspect extension."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgLogicalinspectSupport(Protocol):
    """Capability probe for pg_logicalinspect: Logical decoding snapshot inspection."""

    def supports_pg_logicalinspect(self) -> bool:
        """Whether this extension's capabilities are available."""
        ...
