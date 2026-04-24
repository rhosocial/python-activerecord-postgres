# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pglogical.py
"""pglogical extension protocol definition.

This module defines the protocol for pglogical logical replication
functionality in PostgreSQL.
"""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class PostgresPgLogicalSupport(Protocol):
    """pglogical logical replication extension protocol.

    Feature Source: Extension support (requires pglogical extension)

    pglogical provides logical replication:
    - Publication
    - Subscription
    - Conflict resolution

    Extension Information:
    - Extension name: pglogical
    - Install command: CREATE EXTENSION pglogical;
    - Minimum version: 2.4
    - Documentation: https://github.com/2ndquadrant/pglogical
    """

    def supports_pglogical(self) -> bool:
        """Whether pglogical extension is available."""
        ...

    def supports_pglogical_replication(self) -> bool:
        """Whether pglogical supports logical replication."""
        ...

    def format_pglogical_create_node(self, node_name: str, dsn: str,
                                      useserial: bool = False) -> str:
        """Format pglogical node creation."""
        ...

    def format_pglogical_create_publication(self, pub_name: str,
                                             replicate_insert: bool = True) -> str:
        """Format pglogical publication creation."""
        ...

    def format_pglogical_create_subscription(self, sub_name: str, pub_dsn: str,
                                              replication_sets: Optional[list] = None) -> str:
        """Format pglogical subscription creation."""
        ...

    def format_pglogical_show_subscription_status(self, sub_name: Optional[str] = None) -> str:
        """Format pglogical subscription status query."""
        ...

    def format_pglogical_alter_subscription_synchronize(self, sub_name: str,
                                                         truncate: bool = False) -> str:
        """Format pglogical subscription synchronization."""
        ...