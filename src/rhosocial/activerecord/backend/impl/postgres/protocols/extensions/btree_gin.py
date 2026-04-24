# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/btree_gin.py
"""btree_gin extension protocol definition.

This module defines the protocol for btree_gin composite index
functionality in PostgreSQL.
"""

from typing import List, Protocol, runtime_checkable


@runtime_checkable
class PostgresBtreeGinSupport(Protocol):
    """btree_gin composite index extension protocol.

    Feature Source: Extension support (requires btree_gin extension)

    btree_gin provides GIN index for btree-comparable types:
    - Multi-column GIN indexes
    - Composite key indexes

    Extension Information:
    - Extension name: btree_gin
    - Install command: CREATE EXTENSION btree_gin;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/btree-gin.html
    """

    def supports_btree_gin(self) -> bool:
        """Whether btree_gin is available."""
        ...

    def format_gin_index(
        self,
        index_name: str,
        table_name: str,
        columns: List[str],
    ) -> str:
        """Format a GIN index using btree_gin."""
        ...

    def format_btree_gin_operator_class(self, data_type: str) -> str:
        """Format btree_gin operator class name."""
        ...