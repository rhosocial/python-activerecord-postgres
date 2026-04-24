# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/hypopg.py
"""hypopg extension protocol definition.

This module defines the protocol for hypopg hypothetical indexes
functionality in PostgreSQL.
"""

from typing import List, Protocol, runtime_checkable


@runtime_checkable
class PostgresHypoPgSupport(Protocol):
    """hypopg hypothetical indexes extension protocol.

    Feature Source: Extension support (requires hypopg extension)

    hypopg provides hypothetical indexes:
    - Create virtual indexes
    - Test query plans without building

    Extension Information:
    - Extension name: hypopg
    - Install command: CREATE EXTENSION hypopg;
    - Minimum version: 1.4
    - Documentation: https://github.com/ HYPoPG/hypopg
    """

    def supports_hypopg(self) -> bool:
        """Whether hypopg extension is available."""
        ...

    def format_hypopg_create_index(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
        index_type: str = "btree",
    ) -> str:
        """Format a hypothetical index creation."""
        ...

    def format_hypopg_reset(self) -> str:
        """Format hypothetical index reset."""
        ...

    def format_hypopg_show_indexes(self) -> str:
        """Format show all hypothetical indexes."""
        ...

    def format_hypopg_estimate_size(self, index_id: int) -> str:
        """Format hypothetical index size estimation."""
        ...