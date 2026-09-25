# src/rhosocial/activerecord/backend/impl/postgres/expression/introspection.py
"""PostgreSQL-specific introspection expressions.

These expressions reuse the core parameter container contract
(``IntrospectionExpression.get_params()``) and only redirect SQL generation to
PostgreSQL-specific formatters, so the core introspection layer stays untouched.
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.introspection import (
    ViewInfoExpression,
    ViewListExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresMaterializedViewInfoExpression",
    "PostgresMaterializedViewListExpression",
]


class PostgresMaterializedViewListExpression(ViewListExpression):
    """List materialized views (``pg_class.relkind = 'm'``).

    Example:
        >>> expr = PostgresMaterializedViewListExpression(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        include_system: bool = False,
    ):
        super().__init__(dialect, schema=schema, include_system=include_system)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_materialized_view_list_query"


class PostgresMaterializedViewInfoExpression(ViewInfoExpression):
    """Fetch a single materialized view by name.

    Example:
        >>> expr = PostgresMaterializedViewInfoExpression(dialect, "sales_summary")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        view_name: str,
        schema: Optional[str] = None,
        include_columns: bool = True,
    ):
        super().__init__(
            dialect,
            view_name=view_name,
            schema=schema,
            include_columns=include_columns,
        )

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_materialized_view_info_query"
