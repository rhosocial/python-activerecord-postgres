# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/btree_gist.py
"""btree_gist extension protocol definition.

This module defines the protocol for btree_gist composite index
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


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