# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/extended_statistics.py
"""PostgreSQL extended statistics implementation.

This module provides mixin class for PostgreSQL extended statistics,
which help the query planner make better estimates for combined column values.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import PostgresCreateStatisticsExpression, PostgresDropStatisticsExpression


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

    def format_create_statistics_statement(self, expr: "PostgresCreateStatisticsExpression") -> Tuple[str, tuple]:
        """Format CREATE STATISTICS statement for extended statistics.

        - ``expr.schema`` — optional schema qualifier.
        - ``expr.name`` — statistics object name.
        - ``expr.table_name`` — source table name.
        - ``expr.if_not_exists`` — add ``IF NOT EXISTS``.
        - ``expr.statistics_type`` — optional type (``ndistinct``, ``dependencies``, ``mcv``; MCV requires PG 12+).
        - ``expr.columns`` — list of column names.

        Args:
            expr: PostgresCreateStatisticsExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        Raises:
            ValueError: If statistics type is unsupported or MCV is used before PG 12.

        """
        if not self.supports_create_statistics():
            raise ValueError("CREATE STATISTICS requires PostgreSQL 10+")

        if expr.schema:
            full_name = f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.name)}"
            table_full = f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.table_name)}"
        else:
            full_name = self.format_identifier(expr.name)
            table_full = self.format_identifier(expr.table_name)

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

    def format_drop_statistics_statement(self, expr: "PostgresDropStatisticsExpression") -> Tuple[str, tuple]:
        """Format DROP STATISTICS statement.

        - ``expr.schema`` — optional schema qualifier.
        - ``expr.name`` — statistics object name.
        - ``expr.if_exists`` — add ``IF EXISTS``.

        Args:
            expr: PostgresDropStatisticsExpression instance

        Returns:
            Tuple of (SQL string, empty params tuple)

        """
        full_name = (
            f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.name)}"
            if expr.schema
            else self.format_identifier(expr.name)
        )
        exists_clause = "IF EXISTS " if expr.if_exists else ""

        sql = f"DROP STATISTICS {exists_clause}{full_name}"

        return sql, ()
