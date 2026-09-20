# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/multirange.py
"""PostgreSQL multirange type DDL expressions.

Version Requirements:
- CREATE TYPE ... AS MULTIRANGE: PostgreSQL 14+
- multirange_agg(): PostgreSQL 14+
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class CreateMultirangeTypeExpression(BaseExpression):
    """Expression for CREATE TYPE ... AS MULTIRANGE statement.

    PostgreSQL automatically creates multirange types when you create
    a range type. This expression is for explicit creation if needed.

    Attributes:
        name: Multirange type name.
        range_type: Associated range type.
        schema: Optional schema name.

    Example:
        >>> expr = CreateMultirangeTypeExpression(
        ...     dialect, name="my_multirange", range_type="my_range",
        ... )
        >>> sql, params = expr.to_sql()
        >>> sql
        'CREATE TYPE my_multirange AS MULTIRANGE (my_range)'

    Note:
        Requires PostgreSQL 14+.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        range_type: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.range_type = range_type
        self.schema = schema

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_create_multirange_type_statement_expression"


class MultirangeAggFunctionExpression(BaseExpression):
    """Expression for multirange_agg aggregate function call.

    Aggregates multiple ranges into a multirange.

    Attributes:
        range_column: Range column to aggregate.
        table_name: Table name.
        where_clause: Optional WHERE clause.
        schema: Optional schema name.

    Example:
        >>> expr = MultirangeAggFunctionExpression(
        ...     dialect, range_column="period", table_name="events",
        ... )
        >>> sql, params = expr.to_sql()
        >>> sql
        'SELECT multirange_agg(period) FROM events'

    Note:
        Requires PostgreSQL 14+.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        range_column: str,
        table_name: str,
        where_clause: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.range_column = range_column
        self.table_name = table_name
        self.where_clause = where_clause
        self.schema = schema

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_multirange_agg_function_expression"
