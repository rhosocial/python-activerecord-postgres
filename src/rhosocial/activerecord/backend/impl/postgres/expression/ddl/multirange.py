# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/multirange.py
"""PostgreSQL multirange expressions."""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "CreateMultirangeTypeExpression",
    "MultirangeAggFunctionExpression",
]


class CreateMultirangeTypeExpression(BaseExpression):
    """Compatibility expression for the removed explicit multirange DDL form."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        range_type: str,
        schema: Optional[str] = None,
    ) -> None:
        super().__init__(dialect)
        self.name = name
        self.range_type = range_type
        self.schema = schema

    @property
    def format_method(self) -> str:
        return "format_create_multirange_type_statement_expression"


class MultirangeAggFunctionExpression(BaseExpression):
    def __init__(
        self,
        dialect: "SQLDialectBase",
        range_column: str,
        table_name: str,
        where_clause: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> None:
        super().__init__(dialect)
        self.range_column = range_column
        self.table_name = table_name
        self.where_clause = where_clause
        self.schema = schema

    @property
    def format_method(self) -> str:
        return "format_multirange_agg_function_expression"
