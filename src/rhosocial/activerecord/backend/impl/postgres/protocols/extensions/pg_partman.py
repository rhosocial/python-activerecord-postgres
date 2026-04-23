# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_partman.py
"""pg_partman extension protocol definition.

This module defines the protocol for pg_partman partition management
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgPartmanSupport(Protocol):
    """pg_partman partition management extension protocol.

    Feature Source: Extension support (requires pg_partman extension)

    pg_partman provides partition management:
    - Auto create partitions
    - Partition maintenance
    - Time andserial partitioning

    Extension Information:
    - Extension name: pg_partman
    - Install command: CREATE EXTENSION pg_partman;
    - Minimum version: 4.0
    - Documentation: https://github.com/pgpartman/pg_partman
    """

    def supports_pg_partman(self) -> bool:
        """Whether pg_partman extension is available."""
        ...

    def supports_pg_partman_auto_partition(self) -> bool:
        """Whether pg_partman supports auto partitioning."""
        ...