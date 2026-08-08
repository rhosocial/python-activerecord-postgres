# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/routine.py
"""
PostgreSQL DDL expressions: FUNCTION and AGGREGATE routines.

PostgreSQL Documentation:
- CREATE FUNCTION: https://www.postgresql.org/docs/current/sql-createfunction.html
- DROP FUNCTION:   https://www.postgresql.org/docs/current/sql-dropfunction.html
- CREATE AGGREGATE: https://www.postgresql.org/docs/current/sql-createaggregate.html
- DROP AGGREGATE:   https://www.postgresql.org/docs/current/sql-dropaggregate.html

Version Requirements:
- CREATE/DROP FUNCTION: PostgreSQL 9.6+
- CREATE/DROP AGGREGATE: PostgreSQL 9.6+

Note: CREATE/DROP PROCEDURE is covered by the existing
``PostgresStoredProcedureMixin``; this module covers FUNCTION and AGGREGATE.
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresCreateFunctionExpression",
    "PostgresDropFunctionExpression",
    "PostgresCreateAggregateExpression",
    "PostgresDropAggregateExpression",
]


class PostgresCreateFunctionExpression(BaseExpression):
    """PostgreSQL CREATE FUNCTION statement expression.

    Attributes:
        name: Name of the function.
        args: Optional list of argument declarations (e.g. ``["a integer"]``
            or ``[]`` for no arguments).
        returns: Return type (required for functions that return a value).
        language: Procedural language (default ``plpgsql``).
        body: Function body (between the ``$$`` dollar-quoting delimiters).
        schema: Optional schema for the function.
        or_replace: Add ``OR REPLACE``.
        security: ``DEFINER`` or ``INVOKER`` (optional).
        cost: Estimated execution cost (optional).
        rows: Estimated rows returned (optional).
        is_strict: When True, add ``STRICT``.
        dialect_options: Reserved.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        return_type: str,
        body: str,
        schema: Optional[str] = None,
        args: Optional[List[str]] = None,
        language: str = "plpgsql",
        or_replace: bool = False,
        security: Optional[str] = None,
        cost: Optional[float] = None,
        rows: Optional[int] = None,
        strict: bool = False,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.return_type = return_type
        self.args = args or []
        self.body = body
        self.language = language
        self.or_replace = or_replace
        self.security = security
        self.cost = cost
        self.rows = rows
        self.strict = strict
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the CREATE FUNCTION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_create_function_ddl_statement(self)


class PostgresDropFunctionExpression(BaseExpression):
    """PostgreSQL DROP FUNCTION statement expression.

    Attributes:
        name: Name of the function to drop.
        schema: Optional schema for the function.
        args: Optional argument type list used for overload resolution
            (e.g. ``["integer"]``).
        if_exists: When True, add ``IF EXISTS``.
        cascade: When True, add ``CASCADE``.
        restrict: When True, add ``RESTRICT``. Mutually exclusive with
            ``cascade``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        args: Optional[List[str]] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.args = args or []
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict

    def to_sql(self) -> "Tuple[str, tuple]":
        """Return the DROP FUNCTION statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_function_ddl_statement(self)


class PostgresCreateAggregateExpression(BaseExpression):
    """PostgreSQL CREATE AGGREGATE statement expression (minimal).

    Attributes:
        name: Name of the aggregate.
        sfunc: State transition function name.
        stype: State data type.
        schema: Optional schema for the aggregate.
        finalfunc: Optional final function name.
        initcond: Optional initial condition value.
        dialect_options: Reserved.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        sfunc: str,
        stype: str,
        schema: Optional[str] = None,
        finalfunc: Optional[str] = None,
        initcond: Optional[str] = None,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.sfunc = sfunc
        self.stype = stype
        self.finalfunc = finalfunc
        self.initcond = initcond
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> "Tuple[str, tuple]":
        """Return the CREATE AGGREGATE statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_create_aggregate_ddl_statement(self)


class PostgresDropAggregateExpression(BaseExpression):
    """PostgreSQL DROP AGGREGATE statement expression.

    Attributes:
        name: Name of the aggregate to drop.
        arg_type: Aggregated argument type (needed to identify the
            aggregate, e.g. ``integer``).
        schema: Optional schema for the aggregate.
        if_exists: When True, add ``IF EXISTS``.
        cascade: When True, add ``CASCADE``.
        restrict: When True, add ``RESTRICT``. Mutually exclusive with
            ``cascade``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        arg_type: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ):
        super().__init__(dialect)
        self.name = name
        self.arg_type = arg_type
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict

    def to_sql(self) -> "Tuple[str, tuple]":
        """Return the DROP AGGREGATE statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_aggregate_ddl_statement(self)