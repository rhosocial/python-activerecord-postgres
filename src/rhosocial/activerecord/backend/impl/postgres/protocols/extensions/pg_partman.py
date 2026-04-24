# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_partman.py
"""pg_partman extension protocol definition.

This module defines the protocol for pg_partman partition management
functionality in PostgreSQL.
"""

from typing import Optional, Protocol, runtime_checkable


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

    def format_create_partition_time(self, table_name: str, partition_type: str = "daily",
                                      interval: Optional[str] = None, preload: bool = True) -> str:
        """Format time-based partition creation."""
        ...

    def format_create_partition_id(self, table_name: str, interval: int = 10000,
                                    preload: bool = True) -> str:
        """Format ID-based partition creation."""
        ...

    def format_run_maintenance(self, config: Optional[str] = None) -> str:
        """Format partition maintenance execution."""
        ...

    def format_auto_partition_config(self, table_name: str, automatic: bool = True,
                                      infinite_time_partitions: bool = False,
                                      retention: Optional[str] = None) -> str:
        """Format auto partition configuration update."""
        ...