# src/rhosocial/activerecord/backend/impl/postgres/expression/transaction.py
"""PostgreSQL transaction statement expressions.

Core's transaction family covers the statements every backend shares (BEGIN,
COMMIT, ROLLBACK, SAVEPOINT, SET TRANSACTION). ``SET CONSTRAINTS`` is
PostgreSQL's spelling for deferring constraint checking inside a transaction,
and it has no core node, so this backend declares one rather than having the
transaction manager assemble the statement at the call site.

Reference: https://www.postgresql.org/docs/current/sql-set-constraints.html
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresSetConstraintsStatement",
]


class PostgresSetConstraintsStatement(BaseExpression):
    """``SET CONSTRAINTS name DEFERRED`` as a statement of its own.

    PostgreSQL checks a constraint marked ``DEFERRABLE`` at transaction commit
    unless the session asks for it earlier; this statement asks for it later,
    which is what deferring a named constraint inside an open transaction
    means.

    The constraint name is an identifier, not a value: the server parses it as
    a name, so a bind parameter cannot stand in for it and the name goes
    through the dialect's identifier quoting. Writing it into the text as it
    stood -- what the transaction manager used to do with an f-string -- meant
    a name carrying a quote or a space either broke the statement or escaped
    it.

    Example:
        >>> expr = PostgresSetConstraintsStatement(
        ...     dialect, constraint_name="orders_cust_fkey"
        ... )
        >>> sql, params = expr.to_sql()
        >>> sql
        'SET CONSTRAINTS "orders_cust_fkey" DEFERRED'
        >>> params
        ()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        constraint_name: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            constraint_name: Name of the constraint whose checking is deferred.
        """
        super().__init__(dialect)
        self.constraint_name = constraint_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_set_constraints_statement"
