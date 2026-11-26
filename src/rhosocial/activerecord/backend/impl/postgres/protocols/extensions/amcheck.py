# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/amcheck.py
"""Protocol for the amcheck extension."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresAmcheckSupport(Protocol):
    """Capability probe for amcheck: Index and heap consistency checking."""

    def supports_amcheck(self) -> bool:
        """Whether this extension's capabilities are available."""
        ...
