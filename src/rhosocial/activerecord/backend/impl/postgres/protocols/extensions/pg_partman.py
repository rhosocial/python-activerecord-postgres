# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_partman.py
"""pg_partman extension protocol definition.

This module defines the protocol for pg_partman partition management
functionality in PostgreSQL.

For SQL expression generation of partition creation and maintenance functions,
use the function factories in ``functions/pg_partman.py`` instead of the
removed format_* methods.
"""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class PostgresPgPartmanSupport(Protocol):
    """pg_partman partition management extension protocol.

    Feature Source: Extension support (requires pg_partman extension)

    pg_partman provides partition management:
    - Auto create partitions
    - Partition maintenance
    - Time and serial partitioning

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

    def format_auto_partition_config(self, table_name: str, automatic: bool = True,
                                      infinite_time_partitions: bool = False,
                                      retention: Optional[str] = None) -> str:
        """Format auto partition configuration update."""
        ...
