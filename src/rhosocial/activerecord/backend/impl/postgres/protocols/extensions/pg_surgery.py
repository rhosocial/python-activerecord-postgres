# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_surgery.py
"""pg_surgery extension protocol definition.

This module defines the protocol for pg_surgery data repair
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgSurgerySupport(Protocol):
    """pg_surgery data repair extension protocol.

    Feature Source: Extension support (requires pg_surgery extension)

    pg_surgery provides data repair:
    - Heap freezing
    - Page healing

    Extension Information:
    - Extension name: pg_surgery
    - Install command: CREATE EXTENSION pg_surgery;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/pgsurgery.html
    """

    def supports_pg_surgery(self) -> bool:
        """Whether pg_surgery extension is available."""
        ...

    def format_pg_surgery_heap_freeze(self, table_name: str) -> str:
        """Format heap freeze operation."""
        ...

    def format_pg_surgery_heap_page_header(
        self, table_name: str, page_offset: int, page_pid: int
    ) -> str:
        """Format heap page header repair."""
        ...