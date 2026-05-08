"""pg_logicalinspect extension support protocol.

This module defines the protocol for pg_logicalinspect logical replication
inspection functionality in PostgreSQL.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pglogicalinspect.html
"""

from typing import Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresPgLogicalinspectSupport(Protocol):
    """pg_logicalinspect logical replication inspection protocol.

    Feature Source: Extension support (requires pg_logicalinspect extension)

    pg_logicalinspect provides functions to inspect logical replication state:
    - pg_logicalinspect_get_changes: Inspect changes in a logical slot
    - pg_logicalinspect_get_slot_info: Get information about a logical slot

    Extension Information:
    - Extension name: pg_logicalinspect
    - Install command: CREATE EXTENSION pg_logicalinspect;
    - Minimum version: 1.0 (PostgreSQL 18+)
    - Documentation: https://www.postgresql.org/docs/current/pglogicalinspect.html
    """

    def supports_pg_logicalinspect(self) -> bool:
        """Whether pg_logicalinspect functions are available."""
        ...

    def format_inspect_slot_statement(
        self, slot_name: str, limit: int = 100
    ) -> Tuple[str, tuple]:
        """Format statement to inspect changes in a logical replication slot."""
        ...