# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgstattuple.py
"""Protocol for the pgstattuple extension."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgstattupleSupport(Protocol):
    """Capability probe for pgstattuple: Tuple-level statistics."""

    def supports_pgstattuple(self) -> bool:
        """Whether this extension's capabilities are available."""
        ...
