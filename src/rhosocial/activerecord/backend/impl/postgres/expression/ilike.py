# src/rhosocial/activerecord/backend/impl/postgres/expression/ilike.py
"""PostgreSQL ILIKE expression class."""

from typing import Any, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class ILIKEExpression(BaseExpression):
    """PostgreSQL ILIKE expression for case-insensitive pattern matching.

    Attributes:
        column: Column name or expression with ``to_sql()``.
        pattern: Pattern string (replaced with ``%s`` placeholder).
        negate: If True, produces ``NOT ILIKE`` instead of ``ILIKE``.

    Example:
        >>> expr = ILIKEExpression(dialect, column="name", pattern="%foo%")
        >>> sql, params = expr.to_sql()
        >>> sql
        '"name" ILIKE %s'
        >>> params
        ('%foo%',)

    Note:
        Requires PostgreSQL backend. The ``ILIKE`` operator is
        PostgreSQL-specific and not portable to other databases.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        column: Any,
        pattern: str,
        negate: bool = False,
    ):
        super().__init__(dialect)
        self.column = column
        self.pattern = pattern
        self.negate = negate

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_ilike_expression"
