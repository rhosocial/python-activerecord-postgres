# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/locking.py
"""PostgreSQL row-level locking protocol definition.

This module defines the protocol for PostgreSQL-specific row-level locking
features that are not part of the SQL standard.
"""

from typing import Any, Protocol, runtime_checkable, Tuple


@runtime_checkable
class PostgresLockingSupport(Protocol):
    """PostgreSQL row-level locking protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL locking features beyond SQL standard:
    - FOR NO KEY UPDATE: Weaker exclusive lock (PG 9.0+)
    - FOR SHARE: Shared lock (PG 9.0+)
    - FOR KEY SHARE: Weakest shared lock (PG 9.3+)
    - SKIP LOCKED: Skip locked rows (PG 9.5+)

    Official Documentation:
    - Row Locking: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE

    Version Requirements:
    - FOR NO KEY UPDATE: PostgreSQL 9.0+
    - FOR SHARE: PostgreSQL 9.0+
    - FOR KEY SHARE: PostgreSQL 9.3+
    - SKIP LOCKED: PostgreSQL 9.5+
    """

    def supports_for_no_key_update(self) -> bool:
        """Whether FOR NO KEY UPDATE is supported (PostgreSQL 9.0+)."""
        ...

    def supports_for_share(self) -> bool:
        """Whether FOR SHARE is supported (PostgreSQL 9.0+)."""
        ...

    def supports_for_key_share(self) -> bool:
        """Whether FOR KEY SHARE is supported (PostgreSQL 9.3+)."""
        ...

    def supports_for_update_skip_locked(self) -> bool:
        """Whether FOR UPDATE SKIP LOCKED is supported (PostgreSQL 9.5+)."""
        ...

    def format_postgres_for_update_clause(self, clause: Any) -> Tuple[str, tuple]:
        """Format PostgreSQL-specific FOR UPDATE clause.

        Args:
            clause: PostgresForUpdateClause instance

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
