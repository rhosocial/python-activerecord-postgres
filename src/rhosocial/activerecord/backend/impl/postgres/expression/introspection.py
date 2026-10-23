# src/rhosocial/activerecord/backend/impl/postgres/expression/introspection.py
"""PostgreSQL-specific introspection expressions.

These expressions reuse the core parameter container contract
(``IntrospectionExpression.get_params()``) and only redirect SQL generation to
PostgreSQL-specific formatters, so the core introspection layer stays untouched.
The two ``pg_database_size`` nodes follow the same split for a query core has no
class for: the function node carries the call, the query node carries the row
shape the caller reads.
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.introspection import (
    ViewInfoQueryStatement,
    ViewListQueryStatement,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresMaterializedViewInfoQueryStatement",
    "PostgresMaterializedViewListQueryStatement",
    "PostgresDatabaseSizeFunction",
    "PostgresDatabaseSizeQueryStatement",
]


class PostgresMaterializedViewListQueryStatement(ViewListQueryStatement):
    """List materialized views (``pg_class.relkind = 'm'``).

    Example:
        >>> expr = PostgresMaterializedViewListQueryStatement(dialect, schema="public")
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
        return "format_materialized_view_list_query_statement"


class PostgresMaterializedViewInfoQueryStatement(ViewInfoQueryStatement):
    """Fetch a single materialized view by name.

    Example:
        >>> expr = PostgresMaterializedViewInfoQueryStatement(dialect, "sales_summary")
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
        return "format_materialized_view_info_query_statement"


class PostgresDatabaseSizeFunction(BaseExpression):
    """The ``pg_database_size(name)`` call as a node of its own.

    PostgreSQL spells the on-disk size of a database as this one-argument
    function. The backend declares it rather than reaching for the generic
    ``FunctionCall``, which core reserves for user-defined functions: a
    standard function the server ships is a named node, so an artifact naming
    it resolves to exactly one class instead of a catch-all whose name is
    whatever the caller typed.

    The database name is a bind parameter: PostgreSQL parses the function's
    argument, so the value travels through the driver's placeholder rather
    than being spelled into the SQL text.

    Example:
        >>> expr = PostgresDatabaseSizeFunction(dialect, database_name="app")
        >>> sql, params = expr.to_sql()
        >>> sql
        'PG_DATABASE_SIZE(%s)'
        >>> params
        ('app',)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        database_name: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            database_name: Name of the database whose size is measured.
        """
        super().__init__(dialect)
        self.database_name = database_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_database_size_function"


class PostgresDatabaseSizeQueryStatement(BaseExpression):
    """The one-row query that reports a database's size in bytes.

    Wraps :class:`PostgresDatabaseSizeFunction` in the ``SELECT ... AS
    size_bytes`` shape the status introspector reads, so the introspector asks
    a node for the whole statement instead of assembling the SELECT itself.
    The alias is part of this query's contract: callers read the row by that
    column name.

    Example:
        >>> expr = PostgresDatabaseSizeQueryStatement(dialect, database_name="app")
        >>> sql, params = expr.to_sql()
        >>> sql
        'SELECT PG_DATABASE_SIZE(%s) AS "size_bytes"'
        >>> params
        ('app',)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        database_name: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            database_name: Name of the database whose size is measured.
        """
        super().__init__(dialect)
        self.database_name = database_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_database_size_query_statement"
