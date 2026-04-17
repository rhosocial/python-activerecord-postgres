# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/extended_statistics.py
"""PostgreSQL extended statistics implementation.

This module provides mixin class for PostgreSQL extended statistics,
which help the query planner make better estimates for combined column values.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import CreateStatisticsExpression, DropStatisticsExpression


class PostgresExtendedStatisticsMixin:
    """PostgreSQL extended statistics implementation.

    Extended statistics help the query planner make better estimates
    for combined column values.
    """

    def supports_create_statistics(self) -> bool:
        """CREATE STATISTICS is supported since PostgreSQL 10."""
        return self.version >= (10, 0, 0)

    def supports_statistics_dependencies(self) -> bool:
        """Functional dependencies statistics are supported since PostgreSQL 10."""
        return self.version >= (10, 0, 0)

    def supports_statistics_ndistinct(self) -> bool:
        """NDistinct statistics are supported since PostgreSQL 10."""
        return self.version >= (10, 0, 0)

    def supports_statistics_mcv(self) -> bool:
        """MCV (Most Common Values) statistics are supported since PostgreSQL 12."""
        return self.version >= (12, 0, 0)

    def format_create_statistics_statement(self, expr: "CreateStatisticsExpression") -> Tuple[str, tuple]:
        """Format CREATE STATISTICS statement for extended statistics.

        Args:
            expr: CreateStatisticsExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        if not self.supports_create_statistics():
            raise ValueError("CREATE STATISTICS requires PostgreSQL 10+")

        full_name = f"{expr.schema}.{expr.name}" if expr.schema else expr.name
        table_full = f"{expr.schema}.{expr.table_name}" if expr.schema else expr.table_name

        exists_clause = "IF NOT EXISTS " if expr.if_not_exists else ""

        # Statistics types clause
        types_clause = ""
        if expr.statistics_type:
            # Validate statistics type
            valid_types = {"ndistinct", "dependencies", "mcv"}
            if expr.statistics_type not in valid_types:
                raise ValueError(f"Invalid statistics type: {expr.statistics_type}. Valid types are: {valid_types}")
            if expr.statistics_type == "mcv" and not self.supports_statistics_mcv():
                raise ValueError("MCV statistics require PostgreSQL 12+")

            types_clause = f"({expr.statistics_type})"

        columns_str = ", ".join(expr.columns)

        sql = f"CREATE STATISTICS {exists_clause}{full_name}{types_clause} ON {columns_str} FROM {table_full}"

        return sql, ()

    def format_drop_statistics_statement(self, expr: "DropStatisticsExpression") -> Tuple[str, tuple]:
        """Format DROP STATISTICS statement.

        Args:
            expr: DropStatisticsExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        full_name = f"{expr.schema}.{expr.name}" if expr.schema else expr.name
        exists_clause = "IF EXISTS " if expr.if_exists else ""

        sql = f"DROP STATISTICS {exists_clause}{full_name}"

        return sql, ()
