# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/multirange.py
"""PostgreSQL multirange support — capability probes and node formatters."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Table

if TYPE_CHECKING:
    from ...expression.statements.ddl.multirange import (
        PostgresCreateMultirangeTypeExpression,
        PostgresMultirangeAggFunctionExpression,
    )


__all__ = ["MultirangeMixin"]


class MultirangeMixin:
    if TYPE_CHECKING:
        name: str
        version: Tuple[int, int, int]

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str: ...

    def supports_multirange(self) -> bool:
        return self.version_at_least((14, 0, 0))

    def supports_multirange_constructor(self) -> bool:
        return self.version_at_least((14, 0, 0))

    def supports_multirange_agg(self) -> bool:
        return self.version_at_least((14, 0, 0))

    def format_postgres_create_multirange_type_expression(
        self, expr: "PostgresCreateMultirangeTypeExpression"
    ) -> Tuple[str, tuple]:
        raise UnsupportedFeatureError(
            self.name,
            "CREATE TYPE AS MULTIRANGE",
            suggestion="set MULTIRANGE_TYPE_NAME on a RANGE definition",
        )

    def format_postgres_multirange_agg_function_expression(
        self, expr: "PostgresMultirangeAggFunctionExpression"
    ) -> Tuple[str, tuple]:
        if not self.supports_multirange_agg():
            raise UnsupportedFeatureError(
                self.name,
                "multirange_agg",
                suggestion="requires PostgreSQL 14+",
            )
        table_sql, _ = self.format_table_object(
            Table(self, expr.table_name, schema_name=expr.schema)
        )
        column_sql = self.format_identifier(expr.range_column)
        sql = f"SELECT multirange_agg({column_sql}) FROM {table_sql}"
        if expr.where_clause:
            sql += f" WHERE {expr.where_clause}"
        return sql, ()
