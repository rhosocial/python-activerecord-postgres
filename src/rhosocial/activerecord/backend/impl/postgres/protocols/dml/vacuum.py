# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/vacuum.py
"""PostgreSQL VACUUM enhancements protocol definition.

This module defines the protocol for PostgreSQL-specific VACUUM features
that are not part of the SQL standard.
"""
from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expressions import VacuumExpression, AnalyzeExpression


@runtime_checkable
class PostgresVacuumSupport(Protocol):
    """PostgreSQL VACUUM enhancements protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL VACUUM features:
    - Parallel VACUUM (PG 13+)
    - INDEX_CLEANUP AUTO (PG 14+)
    - PROCESS_TOAST control (PG 14+)

    Official Documentation:
    - VACUUM: https://www.postgresql.org/docs/current/sql-vacuum.html
    - Parallel VACUUM: https://www.postgresql.org/docs/current/sql-vacuum.html#VACUUM-PARALLEL
    - Routine Vacuuming: https://www.postgresql.org/docs/current/routine-vacuuming.html

    Version Requirements:
    - Parallel VACUUM: PostgreSQL 13+
    - INDEX_CLEANUP AUTO: PostgreSQL 14+
    - PROCESS_TOAST: PostgreSQL 14+
    """

    def supports_parallel_vacuum(self) -> bool:
        """Whether parallel VACUUM for indexes is supported.

        Native feature, PostgreSQL 13+.
        Enables parallel workers for VACUUM index processing.
        """
        ...

    def supports_index_cleanup_auto(self) -> bool:
        """Whether INDEX_CLEANUP AUTO option is supported.

        Native feature, PostgreSQL 14+.
        Automatic index cleanup decision based on bloat.
        """
        ...

    def supports_vacuum_process_toast(self) -> bool:
        """Whether PROCESS_TOAST option is supported.

        Native feature, PostgreSQL 14+.
        Allows skipping TOAST table processing.
        """
        ...

    def format_vacuum_statement(self, expr: "VacuumExpression") -> Tuple[str, tuple]:
        """Format VACUUM statement from VacuumExpression.

        Args:
            expr: VacuumExpression containing all VACUUM options

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...

    def format_analyze_statement(self, expr: "AnalyzeExpression") -> Tuple[str, tuple]:
        """Format ANALYZE statement from AnalyzeExpression.

        Args:
            expr: AnalyzeExpression containing all ANALYZE options

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
