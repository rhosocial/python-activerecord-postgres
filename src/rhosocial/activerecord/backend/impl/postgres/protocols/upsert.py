# src/rhosocial/activerecord/backend/impl/postgres/protocols/upsert.py
"""PostgreSQL upsert feature support protocol."""

from typing import Any, Protocol, runtime_checkable, Tuple


@runtime_checkable
class PostgresUpsertSupport(Protocol):
    """PostgreSQL upsert feature support protocol."""

    def supports_upsert(self) -> bool: ...

    def supports_on_conflict_clause(self) -> bool: ...

    def supports_multiple_on_conflict_clauses(self) -> bool: ...

    def format_on_conflict_clause(self, expr: Any) -> Tuple[str, tuple]:
        """Format ON CONFLICT clause for PostgreSQL.

        Handles the EXCLUDED pseudo-table without quoting its name.

        Args:
            expr: OnConflictClause expression instance

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
