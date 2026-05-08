"""amcheck extension support protocol.

This module defines the protocol for amcheck index integrity checking
functionality in PostgreSQL.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/amcheck.html
"""

from typing import Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresAmcheckSupport(Protocol):
    """amcheck index integrity checking protocol.

    Feature Source: Extension support (requires amcheck extension)

    amcheck provides index verification functions for B-tree indexes:
    - bt_index_check: Verify a single B-tree index
    - bt_index_parent_check: Verify B-tree index with parent-child validation
    - verify_heapam: Verify heap data blocks (PG 14+)

    Extension Information:
    - Extension name: amcheck
    - Install command: CREATE EXTENSION amcheck;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/amcheck.html
    """

    def supports_amcheck_bt_index_check(self) -> bool:
        """Whether bt_index_check() is available for single index verification."""
        ...

    def supports_amcheck_bt_index_parent_check(self) -> bool:
        """Whether bt_index_parent_check() is available for thorough index verification."""
        ...

    def supports_amcheck_heap_verification(self) -> bool:
        """Whether verify_heapam() is available (requires PostgreSQL 14+ / amcheck 1.2+)."""
        ...

    def format_verify_index_statement(
        self, index_name: str, schema: Optional[str] = None, parent_check: bool = False
    ) -> Tuple[str, tuple]:
        """Format statement to verify a B-tree index with bt_index_check."""
        ...

    def format_verify_table_statement(
        self, table_name: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format statement to verify all indexes on a table."""
        ...