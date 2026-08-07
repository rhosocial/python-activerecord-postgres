# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/policy.py
"""PostgreSQL Row-Level Security POLICY protocol definition.

This module contains the :class:`PostgresPolicySupport` protocol which
defines the interface for PostgreSQL's native POLICY DDL features.

POLICY (CREATE/ALTER/DROP) is a PostgreSQL extension — not SQL standard.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.policy import (
        PostgresAlterPolicyExpression,
        PostgresCreatePolicyExpression,
        PostgresDropPolicyExpression,
    )


@runtime_checkable
class PostgresPolicySupport(Protocol):
    """PostgreSQL Row-Level Security POLICY protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - CREATE POLICY: https://www.postgresql.org/docs/current/sql-createpolicy.html
    - ALTER POLICY:  https://www.postgresql.org/docs/current/sql-alterpolicy.html
    - DROP POLICY:   https://www.postgresql.org/docs/current/sql-droppolicy.html

    Version Requirements:
    - CREATE/ALTER/DROP POLICY (basic): PostgreSQL 9.5+
    - AS RESTRICTIVE / explicit AS PERMISSIVE keyword: PostgreSQL 10+
    - DROP POLICY IF EXISTS: PostgreSQL 9.5+ (always supported here)
    - ALTER POLICY ... RENAME TO: PostgreSQL 9.5+
    - CURRENT_ROLE keyword in TO role list: PostgreSQL 15+
    - CURRENT_USER / SESSION_USER / PUBLIC: all versions
    """

    def supports_create_policy(self) -> bool:
        """Whether CREATE/ALTER/DROP POLICY basic syntax is supported (9.5+)."""
        ...

    def supports_policy_restrictive(self) -> bool:
        """Whether ``AS RESTRICTIVE`` clause is supported (10+)."""
        ...

    def supports_explicit_permissive_keyword(self) -> bool:
        """Whether the literal ``AS PERMISSIVE`` keyword is accepted (10+).

        On 9.5 the permissive mode is implicit (the parser rejects the
        keyword itself); it must be omitted silently.
        """
        ...

    def supports_drop_policy_if_exists(self) -> bool:
        """Whether ``DROP POLICY IF EXISTS`` is supported (9.5+)."""
        ...

    def supports_alter_policy_rename(self) -> bool:
        """Whether ``ALTER POLICY ... RENAME TO`` is supported (9.5+)."""
        ...

    def supports_alter_policy_replace(self) -> bool:
        """Whether ``ALTER POLICY ... TO/USING/WITH CHECK`` is supported (9.5+)."""
        ...

    def supports_current_role_keyword(self) -> bool:
        """Whether ``CURRENT_ROLE`` may appear in the ``TO`` role list (15+)."""
        ...

    def format_create_policy_statement(
        self, expr: "PostgresCreatePolicyExpression"
    ) -> Tuple[str, tuple]:
        """Format ``CREATE POLICY`` statement (PostgreSQL-specific syntax).

        Args:
            expr: ``PostgresCreatePolicyExpression`` defining the policy.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.5, or
                ``AS RESTRICTIVE`` on a version below 10, or an unsupported
                role keyword for the active version.
            ValueError: an incompatible ``command`` / USING / WITH CHECK
                combination (e.g. INSERT+USING, SELECT+WITH CHECK).
        """
        ...

    def format_alter_policy_statement(
        self, expr: "PostgresAlterPolicyExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER POLICY`` statement (PostgreSQL-specific syntax).

        Handles two forms via :attr:`expr.mode`:

        - ``RENAME``: ``ALTER POLICY name ON table RENAME TO new_name``
        - ``REPLACE``: ``ALTER POLICY name ON table [TO ...]
          [USING (...)] [WITH CHECK (...)]``

        Args:
            expr: ``PostgresAlterPolicyExpression``.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.5 or
                role keyword needs a newer version.
            ValueError: both RENAME and REPLACE clauses present, or no
                REPLACE clause present at all.
        """
        ...

    def format_drop_policy_statement(
        self, expr: "PostgresDropPolicyExpression"
    ) -> Tuple[str, tuple]:
        """Format ``DROP POLICY`` statement (PostgreSQL-specific syntax).

        Args:
            expr: ``PostgresDropPolicyExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.5.
            ValueError: both ``cascade`` and ``restrict`` are True.

        Note:
            Per PostgreSQL documentation, ``CASCADE``/``RESTRICT`` have no
            effect on DROP POLICY (no dependencies), but are accepted
            syntactically.
        """
        ...
