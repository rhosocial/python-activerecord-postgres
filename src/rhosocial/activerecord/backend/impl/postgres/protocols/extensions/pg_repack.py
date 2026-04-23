# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_repack.py
"""pg_repack extension protocol definition.

This module defines the protocol for pg_repack online rebuild
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgRepackSupport(Protocol):
    """pg_repack online rebuild extension protocol.

    Feature Source: Extension support (requires pg_repack extension)

    pg_repack provides online table/index rebuild:
    - Non-blocking table rebuild
    - Index rebuild

    Extension Information:
    - Extension name: pg_repack
    - Install command: CREATE EXTENSION pg_repack;
    - Minimum version: 1.5
    - Documentation: https://github.com/reorg/pg_repack
    """

    def supports_pg_repack(self) -> bool:
        """Whether pg_repack extension is available."""
        ...