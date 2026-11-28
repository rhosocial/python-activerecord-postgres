# src/rhosocial/activerecord/backend/impl/postgres/expression/introspection/enum_.py
"""PostgreSQL ENUM introspection query nodes.

The type reference expression lives in ``..statements.enum_``; the queries that
ask the catalog about an enum live here, in the introspection family.
"""

from typing import TYPE_CHECKING, Optional

from rhosocial.activerecord.backend.expression.bases.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresEnumTypeExistsQuery", "PostgresEnumTypeValuesQuery"]


class PostgresEnumTypeExistsQuery(BaseExpression):
    """Whether an enum type exists in ``pg_type``/``pg_namespace``.

    The type name and optional schema travel as bind values: a name is a data
    position, and the enum manager used to interpolate it through the dialect's
    placeholder into a hand-assembled string.

    Example:
        >>> expr = PostgresEnumTypeExistsQuery(dialect, "mood")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_enum_type_exists_query"


class PostgresEnumTypeValuesQuery(BaseExpression):
    """The labels of an enum type, in sort order."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_enum_type_values_query"
