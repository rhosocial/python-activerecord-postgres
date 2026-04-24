# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_walinspect.py
"""pg_walinspect extension protocol definition.

This module defines the protocol for pg_walinspect WAL inspection
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgWalinspectSupport(Protocol):
    """pg_walinspect WAL inspection extension protocol.

    Feature Source: Extension support (requires pg_walinspect extension)

    pg_walinspect provides WAL inspection:
    - Read WAL records
    - Decode WAL entries

    Extension Information:
    - Extension name: pg_walinspect
    - Install command: CREATE EXTENSION pg_walinspect;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/pgwalinspect.html
    """

    def supports_pg_walinspect(self) -> bool:
        """Whether pg_walinspect extension is available."""
        ...

    def format_pg_get_wal_records_info(self) -> str:
        """Format WAL records info query."""
        ...

    def format_pg_get_wal_blocks_info(self) -> str:
        """Format WAL blocks info query."""
        ...

    def format_pg_logical_emit_message(
        self, transactional: bool = False, prefix: str = "test"
    ) -> str:
        """Format logical WAL message emission."""
        ...