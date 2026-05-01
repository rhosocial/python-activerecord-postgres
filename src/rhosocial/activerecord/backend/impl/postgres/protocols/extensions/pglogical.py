"""pglogical extension protocol definition.

This module defines the protocol for pglogical logical replication
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/pglogical.py`` instead of the removed format_* methods.
"""

from typing import Protocol, runtime_checkable


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
