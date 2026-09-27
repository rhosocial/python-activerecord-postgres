# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/multirange.py
"""PostgreSQL multirange support."""

from typing import Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.impl.postgres.expression.ddl.multirange import (
        CreateMultirangeTypeExpression,
        MultirangeAggFunctionExpression,
    )


__all__ = ["MultirangeMixin"]


class MultirangeMixin:
    if TYPE_CHECKING:
        name: str
        version: Tuple[int, int, int]

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str: ...

    def supports_multirange(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_multirange_constructor(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_multirange_agg(self) -> bool:
        return self.version >= (14, 0, 0)

    def format_create_multirange_type_statement(
        self,
        name: str,
        range_type: str,
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        raise UnsupportedFeatureError(
            self.name,
            "CREATE TYPE AS MULTIRANGE",
            suggestion="set MULTIRANGE_TYPE_NAME on a RANGE definition",
        )

    def format_multirange_agg_function(
        self,
        range_column: str,
        table_name: str,
        where_clause: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Tuple[str, tuple]:
        if not self.supports_multirange_agg():
            raise UnsupportedFeatureError(
                self.name,
                "multirange_agg",
                suggestion="requires PostgreSQL 14+",
            )
        full_name = f"{schema}.{table_name}" if schema is not None else table_name
        table_sql = ".".join(self.format_identifier(part) for part in full_name.split("."))
        column_sql = self.format_identifier(range_column)
        sql = f"SELECT multirange_agg({column_sql}) FROM {table_sql}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        return sql, ()

    def format_create_multirange_type_statement_expression(
        self,
        expr: "CreateMultirangeTypeExpression",
    ) -> Tuple[str, tuple]:
        return self.format_create_multirange_type_statement(
            expr.name,
            expr.range_type,
            expr.schema,
        )

    def format_multirange_agg_function_expression(
        self,
        expr: "MultirangeAggFunctionExpression",
    ) -> Tuple[str, tuple]:
        return self.format_multirange_agg_function(
            expr.range_column,
            expr.table_name,
            expr.where_clause,
            expr.schema,
        )
