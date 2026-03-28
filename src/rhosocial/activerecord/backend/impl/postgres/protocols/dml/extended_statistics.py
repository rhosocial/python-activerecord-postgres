# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/extended_statistics.py
"""PostgreSQL extended statistics protocol definition.

This module defines the protocol for PostgreSQL extended statistics features,
which provide advanced query planning statistics for better performance.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expressions import CreateStatisticsExpression, DropStatisticsExpression


@runtime_checkable
class PostgresExtendedStatisticsSupport(Protocol):
    """PostgreSQL extended statistics protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL extended statistics features:
    - CREATE STATISTICS (PG 10+)
    - Functional dependencies tracking (PG 10+)
    - NDistinct statistics (PG 10+)
    - MCV (Most Common Values) statistics (PG 12+)

    Official Documentation:
    - Extended Statistics: https://www.postgresql.org/docs/current/planner-stats.html#PLANNER-STATS-EXTENDED

    Version Requirements:
    - CREATE STATISTICS: PostgreSQL 10+
    - Dependencies/NDistinct: PostgreSQL 10+
    - MCV statistics: PostgreSQL 12+
    """

    def supports_create_statistics(self) -> bool:
        """Whether CREATE STATISTICS for extended statistics is supported.

        Native feature, PostgreSQL 10+.
        Enables creating extended statistics objects.
        """
        ...

    def supports_statistics_dependencies(self) -> bool:
        """Whether functional dependencies statistics are supported.

        Native feature, PostgreSQL 10+.
        Tracks column correlations for better planning.
        """
        ...

    def supports_statistics_ndistinct(self) -> bool:
        """Whether ndistinct statistics are supported.

        Native feature, PostgreSQL 10+.
        Tracks distinct values for column groups.
        """
        ...

    def supports_statistics_mcv(self) -> bool:
        """Whether MCV (Most Common Values) statistics are supported.

        Native feature, PostgreSQL 12+.
        Improves estimation for combined column values.
        """
        ...

    def format_create_statistics_statement(self, expr: "CreateStatisticsExpression") -> Tuple[str, tuple]:
        """Format CREATE STATISTICS statement for extended statistics.

        Args:
            expr: CreateStatisticsExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_drop_statistics_statement(self, expr: "DropStatisticsExpression") -> Tuple[str, tuple]:
        """Format DROP STATISTICS statement.

        Args:
            expr: DropStatisticsExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...
