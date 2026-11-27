# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/timescaledb.py
"""Protocol for the TimescaleDB extension."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgreTimescaleDBSupport(Protocol):
    """Capability probe for TimescaleDB hypertables."""

    def supports_timescaledb(self) -> bool:
        """Whether TimescaleDB hypertable features are available."""
        ...
