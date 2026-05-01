# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/btree_gist.py
"""btree_gist extension protocol definition.

This module defines the protocol for btree_gist composite index
functionality in PostgreSQL.
"""

from typing import List, Optional, Protocol, runtime_checkable


@runtime_checkable
class PostgresBtreeGistSupport(Protocol):
    """btree_gist composite index extension protocol.

    Feature Source: Extension support (requires btree_gist extension)

    btree_gist provides GiST index for btree-comparable types:
    - Multi-column GiST indexes
    - Composite key indexes

    Extension Information:
    - Extension name: btree_gist
    - Install command: CREATE EXTENSION btree_gist;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/btree-gist.html
    """

    def supports_btree_gist(self) -> bool:
        """Whether btree_gist is available."""
        ...

    def format_gist_index(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
        include: Optional[List[str]] = None,
    ) -> str:
        """Format a GiST index using btree_gist."""
        ...

    def format_btree_gist_operator_class(self, data_type: str) -> str:
        """Format btree_gist operator class name."""
        ...