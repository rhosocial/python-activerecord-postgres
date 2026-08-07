# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/policy.py
"""
PostgreSQL DDL expressions: Row-Level Security POLICY operations.

PostgreSQL Documentation:
- CREATE POLICY: https://www.postgresql.org/docs/current/sql-createpolicy.html
- ALTER POLICY:  https://www.postgresql.org/docs/current/sql-alterpolicy.html
- DROP POLICY:   https://www.postgresql.org/docs/current/sql-droppolicy.html

Version Requirements:
- CREATE/ALTER/DROP POLICY (basic): PostgreSQL 9.5+
- AS RESTRICTIVE / explicit AS PERMISSIVE: PostgreSQL 10+
- CURRENT_ROLE keyword in TO role list: PostgreSQL 15+

Note: These statements are PostgreSQL extensions (non-SQL-standard). They live in
the postgres backend package, not in the core backend/expression/statements/.
"""

from enum import Enum
from typing import List, Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import (
    BaseExpression,
    SQLPredicate,
    SQLQueryAndParams,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PolicyType",
    "PolicyCommand",
    "AlterPolicyMode",
    "PostgresCreatePolicyExpression",
    "PostgresAlterPolicyExpression",
    "PostgresDropPolicyExpression",
]


class PolicyType(Enum):
    """Row-Level Security policy type."""

    PERMISSIVE = "PERMISSIVE"
    RESTRICTIVE = "RESTRICTIVE"


class PolicyCommand(Enum):
    """SQL command a policy applies to."""

    ALL = "ALL"
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class AlterPolicyMode(Enum):
    """The two syntactic forms of ALTER POLICY.

    REPLACE: alter roles / USING / WITH CHECK (several may be omitted).
    RENAME: ALTER POLICY name ON table RENAME TO new_name.
    """

    REPLACE = "REPLACE"
    RENAME = "RENAME"


class PostgresCreatePolicyExpression(BaseExpression):
    """PostgreSQL CREATE POLICY statement expression.

    Defines a new row-level security policy for a table.

    Attributes:
        name: Name of the policy (distinct per table).
        table_name: Name of the table the policy applies to.
        schema: Optional schema for the table.
        policy_type: Optional ``AS PERMISSIVE``/``AS RESTRICTIVE`` clause.
            RESTRICTIVE requires PostgreSQL 10+; PERMISSIVE keyword is only
            emitted on PostgreSQL 10+ (silently omitted on 9.5).
        command: Optional ``FOR {ALL|SELECT|INSERT|UPDATE|DELETE}`` clause.
            If None, the clause is omitted (PostgreSQL default = ALL).
        roles: Optional ``TO role [, ...]`` clause list. Special literals
            ``PUBLIC`` / ``CURRENT_ROLE`` / ``CURRENT_USER`` / ``SESSION_USER``
            are emitted verbatim (no quoting); any other value is treated as
            an identifier. CURRENT_ROLE requires PostgreSQL 15+.
        using: Optional ``USING (predicate)`` expression. Forbidden when
            ``command == INSERT``.
        with_check: Optional ``WITH CHECK (predicate)`` expression. Forbidden
            when ``command`` is SELECT or DELETE.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> expr = PostgresCreatePolicyExpression(
        ...     dialect=dialect,
        ...     name="user_select_own",
        ...     table_name="orders",
        ...     command=PolicyCommand.SELECT,
        ...     using=Column(dialect, "user_id") == 1,
        ... )
        >>> sql, params = expr.to_sql()  # doctest: +SKIP

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        table_name: str,
        schema: Optional[str] = None,
        policy_type: Optional[PolicyType] = None,
        command: Optional[PolicyCommand] = None,
        roles: Optional[List[str]] = None,
        using: Optional[SQLPredicate] = None,
        with_check: Optional[SQLPredicate] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.table_name = table_name
        self.schema = schema
        self.policy_type = policy_type
        self.command = command
        self.roles = roles
        self.using = using
        self.with_check = with_check

    def to_sql(self) -> "SQLQueryAndParams":
        """Generate CREATE POLICY SQL statement.

        Returns:
            Tuple of (SQL string, params tuple).

        """
        return self.dialect.format_create_policy_statement(self)


class PostgresAlterPolicyExpression(BaseExpression):
    """PostgreSQL ALTER POLICY statement expression.

    PostgreSQL supports two syntactic forms:

    1. ``ALTER POLICY name ON table RENAME TO new_name``
    2. ``ALTER POLICY name ON table [TO roles] [USING (...)] [WITH CHECK (...)]``

    Each clause of form 2 may be independently specified or omitted; clauses not
    given are unchanged on the existing policy.

    Attributes:
        name: Name of the existing policy to alter.
        table_name: Name of the table the policy is on.
        schema: Optional schema for the table.
        new_name: If given, switches the expression to form 1 (RENAME TO).
            Mutually exclusive with roles/using/with_check.
        roles: Optional new ``TO role [, ...]`` clause (form 2 only).
        using: Optional new ``USING (...)`` expression (form 2 only).
        with_check: Optional new ``WITH CHECK (...)`` expression (form 2 only).

    Raises:
        ValueError: if ``new_name`` is set together with any of roles/using/
            with_check (the two forms are mutually exclusive).
        UnsupportedFeatureError: when the dialect version predates
            PostgreSQL 9.5.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        table_name: str,
        schema: Optional[str] = None,
        new_name: Optional[str] = None,
        roles: Optional[List[str]] = None,
        using: Optional[SQLPredicate] = None,
        with_check: Optional[SQLPredicate] = None,
    ):
        super().__init__(dialect)
        self.name = name
        self.table_name = table_name
        self.schema = schema
        self.new_name = new_name
        self.roles = roles
        self.using = using
        self.with_check = with_check

    @property
    def mode(self) -> AlterPolicyMode:
        """Determine the ALTER POLICY form implied by the parameters."""
        if self.new_name is not None:
            return AlterPolicyMode.RENAME
        return AlterPolicyMode.REPLACE

    def to_sql(self) -> "SQLQueryAndParams":
        """Generate ALTER POLICY SQL statement.

        Returns:
            Tuple of (SQL string, params tuple).

        """
        return self.dialect.format_alter_policy_statement(self)


class PostgresDropPolicyExpression(BaseExpression):
    """PostgreSQL DROP POLICY statement expression.

    Removes a row-level security policy from a table.

    Attributes:
        name: Name of the policy to drop.
        table_name: Name of the table the policy is on.
        schema: Optional schema for the table.
        if_exists: When True, add ``IF EXISTS`` (PostgreSQL 9.5+, always
            supported). A notice — not an error — is issued if missing.
        cascade: When True, append ``CASCADE``. Note: per PostgreSQL
            documentation, CASCADE/RESTRICT have no effect for DROP POLICY
            (no dependencies) but are accepted syntactically.
        restrict: When True, append ``RESTRICT``. Mutually exclusive with
            ``cascade``.

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ):
        super().__init__(dialect)
        self.name = name
        self.table_name = table_name
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict

    def to_sql(self) -> "SQLQueryAndParams":
        """Generate DROP POLICY SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_drop_policy_statement(self)
