# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/policy.py
"""PostgreSQL Row-Level Security POLICY DDL implementation.

Implements CREATE/ALTER/DROP POLICY formatting for the postgres dialect.
All three statements are PostgreSQL extensions (non-SQL-standard).

Version Requirements:
- CREATE/ALTER/DROP POLICY (basic): PostgreSQL 9.5+
- AS RESTRICTIVE / explicit AS PERMISSIVE keyword: PostgreSQL 10+
- DROP POLICY IF EXISTS: PostgreSQL 9.5+ (always supported here)
- CURRENT_ROLE keyword in TO role list: PostgreSQL 15+
- CURRENT_USER / SESSION_USER / PUBLIC: all versions
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.bases import SQLPredicate

from ...expression.ddl.policy import (
    AlterPolicyMode,
    PolicyCommand,
    PolicyType,
)

if TYPE_CHECKING:
    from ...expression.ddl.policy import (
        PostgresAlterPolicyExpression,
        PostgresCreatePolicyExpression,
        PostgresDropPolicyExpression,
    )


# Role tokens that MUST NOT be wrapped by format_identifier — they are
# PostgreSQL keywords / reserved role references, not identifiers.
_ROLE_KEYWORDS = frozenset(
    {"PUBLIC", "CURRENT_ROLE", "CURRENT_USER", "SESSION_USER"}
)

# CURRENT_ROLE was introduced in PostgreSQL 15; the other three role keywords
# exist in all supported 9.5+ versions.
_ROLE_KEYWORD_MIN_VERSION = {
    "CURRENT_ROLE": (15, 0, 0),
    "CURRENT_USER": (9, 5, 0),
    "SESSION_USER": (9, 5, 0),
    "PUBLIC": (9, 5, 0),
}


class PostgresPolicyMixin:
    """PostgreSQL Row-Level Security POLICY implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_create_policy(self) -> bool:
        """CREATE/ALTER/DROP POLICY basic syntax requires PostgreSQL 9.5+."""
        return self.version >= (9, 5, 0)

    def supports_policy_restrictive(self) -> bool:
        """AS RESTRICTIVE clause requires PostgreSQL 10+."""
        return self.version >= (10, 0, 0)

    def supports_explicit_permissive_keyword(self) -> bool:
        """The literal ``AS PERMISSIVE`` keyword requires PostgreSQL 10+.

        On PostgreSQL 9.5, permissive is the implicit default and the
        keyword itself is not accepted by the parser; it must be omitted.
        """
        return self.version >= (10, 0, 0)

    def supports_drop_policy_if_exists(self) -> bool:
        """DROP POLICY IF EXISTS is supported since PostgreSQL 9.5."""
        return self.version >= (9, 5, 0)

    def supports_alter_policy_rename(self) -> bool:
        """ALTER POLICY ... RENAME TO ... is supported since PostgreSQL 9.5."""
        return self.version >= (9, 5, 0)

    def supports_alter_policy_replace(self) -> bool:
        """ALTER POLICY (roles/USING/WITH CHECK) is supported since 9.5."""
        return self.version >= (9, 5, 0)

    def supports_current_role_keyword(self) -> bool:
        """CURRENT_ROLE keyword in TO role list requires PostgreSQL 15+."""
        return self.version >= (15, 0, 0)

    # ------------------------------------------------------------------ #
    # Shared helpers
    # ------------------------------------------------------------------ #
    def _format_table_ref(self, schema: Optional[str], table_name: str) -> str:
        """Format ``table_name`` (optionally schema-qualified) as identifier(s)."""
        if schema:
            return f"{self.format_identifier(schema)}.{self.format_identifier(table_name)}"
        return self.format_identifier(table_name)

    def _format_role_list(self, roles: List[str]) -> Tuple[str, tuple]:
        """Format the ``TO role [, ...]`` clause.

        Returns ``(formatted_role_list, params)`` — params are always empty
        for the role list itself (roles are identifiers or keywords, never
        parameterised).
        """
        parts: List[str] = []
        for r in roles:
            upper = r.upper()
            if upper in _ROLE_KEYWORDS:
                min_ver = _ROLE_KEYWORD_MIN_VERSION[upper]
                if self.version < min_ver:
                    raise UnsupportedFeatureError(
                        self.name,
                        f"role keyword {upper}",
                        suggestion=f"requires PostgreSQL "
                                   f"{min_ver[0]}.{min_ver[1]}+",
                    )
                parts.append(upper)
            else:
                parts.append(self.format_identifier(r))
        return ", ".join(parts), ()

    def _format_predicate(
        self, expr: SQLPredicate, clause_keyword: str
    ) -> Tuple[str, tuple]:
        """Format ````USING``/``WITH CHECK`` ( predicate )`` clause."""
        sql, params = expr.to_sql()
        return f"{clause_keyword} ({sql})", params

    # ------------------------------------------------------------------ #
    # CREATE POLICY
    # ------------------------------------------------------------------ #
    def format_create_policy_statement(
        self, expr: "PostgresCreatePolicyExpression"
    ) -> Tuple[str, tuple]:
        """Format CREATE POLICY statement (PostgreSQL-specific).

        Performs version gates and per-command validation in line with
        PostgreSQL's policy semantics, then assembles the SQL.

        Args:
            expr: :class:`PostgresCreatePolicyExpression` with the policy
                definition.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: when the dialect predates PostgreSQL 9.5
                or when ``AS RESTRICTIVE`` is used on a version below 10.
            ValueError: when a command/expression combination is invalid
                (e.g. INSERT policy with a USING expression).

        """
        if not self.supports_create_policy():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE POLICY",
                suggestion="requires PostgreSQL 9.5+",
            )

        # AS clause handling
        if expr.policy_type is PolicyType.RESTRICTIVE:
            if not self.supports_policy_restrictive():
                raise UnsupportedFeatureError(
                    self.name,
                    "AS RESTRICTIVE policy",
                    suggestion="requires PostgreSQL 10+",
                )
            as_clause = "AS RESTRICTIVE"
        elif expr.policy_type is PolicyType.PERMISSIVE:
            # On 9.5 the AS PERMISSIVE keyword is not accepted; silently omit.
            if self.supports_explicit_permissive_keyword():
                as_clause = "AS PERMISSIVE"
            else:
                as_clause = ""
        else:
            as_clause = ""

        # FOR command clause
        command_clause = ""
        if expr.command is not None:
            cmd = expr.command
            cmd_combined_clause_errors(cmd, expr.using, expr.with_check)
            command_clause = f"FOR {cmd.value}"

        # TO role list
        to_clause = ""
        if expr.roles is not None:
            role_list, _ = self._format_role_list(list(expr.roles))
            to_clause = f"TO {role_list}"

        # USING / WITH CHECK
        all_params: List[object] = []
        using_clause = ""
        if expr.using is not None:
            using_clause, params = self._format_predicate(expr.using, "USING")
            all_params.extend(params)
        with_check_clause = ""
        if expr.with_check is not None:
            with_check_clause, params = self._format_predicate(
                expr.with_check, "WITH CHECK"
            )
            all_params.extend(params)

        parts: List[str] = ["CREATE POLICY"]
        parts.append(self.format_identifier(expr.name))
        parts.append("ON")
        parts.append(self._format_table_ref(expr.schema, expr.table_name))
        if as_clause:
            parts.append(as_clause)
        if command_clause:
            parts.append(command_clause)
        if to_clause:
            parts.append(to_clause)
        if using_clause:
            parts.append(using_clause)
        if with_check_clause:
            parts.append(with_check_clause)

        return " ".join(parts), tuple(all_params)

    # ------------------------------------------------------------------ #
    # ALTER POLICY
    # ------------------------------------------------------------------ #
    def format_alter_policy_statement(
        self, expr: "PostgresAlterPolicyExpression"
    ) -> Tuple[str, tuple]:
        """Format ALTER POLICY statement (PostgreSQL-specific).

        Handles two syntactic forms via :attr:`expr.mode`:

        - ``RENAME``: ``ALTER POLICY name ON table RENAME TO new_name``
        - ``REPLACE``: ``ALTER POLICY name ON table [TO ...]
          [USING (...)] [WITH CHECK (...)]``

        For REPLACE, each clause is emitted independently if specified;
        omitted clauses leave the existing policy unchanged (no SQL token).

        Args:
            expr: :class:`PostgresAlterPolicyExpression`.

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.5 or
                when a role keyword needs a version not satisfied.
            ValueError: if RENAME and REPLACE clauses are both specified, or
                if no REPLACE clause is specified at all.

        """
        if not self.supports_create_policy():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER POLICY",
                suggestion="requires PostgreSQL 9.5+",
            )

        mode = expr.mode
        if mode is AlterPolicyMode.RENAME:
            if expr.roles is not None or expr.using is not None \
                    or expr.with_check is not None:
                raise ValueError(
                    "ALTER POLICY RENAME TO is mutually exclusive with "
                    "TO/USING/WITH CHECK clauses"
                )
            if not self.supports_alter_policy_rename():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER POLICY RENAME",
                    suggestion="requires PostgreSQL 9.5+",
                )
            parts = [
                "ALTER POLICY",
                self.format_identifier(expr.name),
                "ON",
                self._format_table_ref(expr.schema, expr.table_name),
                "RENAME TO",
                self.format_identifier(expr.new_name),
            ]
            return " ".join(parts), ()

        # REPLACE form
        if not self.supports_alter_policy_replace():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER POLICY ... TO/USING/WITH CHECK",
                suggestion="requires PostgreSQL 9.5+",
            )
        if expr.roles is None and expr.using is None \
                and expr.with_check is None:
            raise ValueError(
                "ALTER POLICY (REPLACE form) requires at least one of "
                "TO / USING / WITH CHECK"
            )

        all_params: List[object] = []
        parts: List[str] = [
            "ALTER POLICY",
            self.format_identifier(expr.name),
            "ON",
            self._format_table_ref(expr.schema, expr.table_name),
        ]
        if expr.roles is not None:
            role_list, _ = self._format_role_list(list(expr.roles))
            parts.append(f"TO {role_list}")
        if expr.using is not None:
            using_clause, params = self._format_predicate(expr.using, "USING")
            parts.append(using_clause)
            all_params.extend(params)
        if expr.with_check is not None:
            wc_clause, params = self._format_predicate(
                expr.with_check, "WITH CHECK"
            )
            parts.append(wc_clause)
            all_params.extend(params)

        return " ".join(parts), tuple(all_params)

    # ------------------------------------------------------------------ #
    # DROP POLICY
    # ------------------------------------------------------------------ #
    def format_drop_policy_statement(
        self, expr: "PostgresDropPolicyExpression"
    ) -> Tuple[str, tuple]:
        """Format DROP POLICY statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropPolicyExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.5.
            ValueError: if both ``cascade`` and ``restrict`` are True.

        Note:
            Per PostgreSQL documentation, ``CASCADE``/``RESTRICT`` have no
            effect on DROP POLICY (the policy has no dependents), but are
            accepted syntactically — we honour the user's choice and emit
            the token, leaving semantics to the server.

        """
        if not self.supports_create_policy():
            raise UnsupportedFeatureError(
                self.name,
                "DROP POLICY",
                suggestion="requires PostgreSQL 9.5+",
            )
        if expr.cascade and expr.restrict:
            raise ValueError(
                "DROP POLICY: CASCADE and RESTRICT are mutually exclusive"
            )

        parts: List[str] = ["DROP POLICY"]
        if expr.if_exists and self.supports_drop_policy_if_exists():
            parts.append("IF EXISTS")
        parts.append(self.format_identifier(expr.name))
        parts.append("ON")
        parts.append(self._format_table_ref(expr.schema, expr.table_name))
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()


# ------------------------------------------------------------------ #
# Module-level helper (no class binding so it can be reused and unit
# tested in isolation; intentionally avoids any dialect reference).
# ------------------------------------------------------------------ #
def cmd_combined_clause_errors(
    cmd: PolicyCommand,
    using: Optional[SQLPredicate],
    with_check: Optional[SQLPredicate],
) -> None:
    """Validate USING/WITH CHECK against the chosen FOR command type.

    PostgreSQL semantics:
    - INSERT policy: must NOT have USING, may have WITH CHECK.
    - SELECT / DELETE policy: must NOT have WITH CHECK; USING allowed.
    - ALL / UPDATE: both allowed.

    Raises:
        ValueError: describing the mismatch.

    """
    if cmd is PolicyCommand.INSERT and using is not None:
        raise ValueError(
            "INSERT policy cannot have a USING expression "
            "(use WITH CHECK for new-row validation)"
        )
    if cmd in (PolicyCommand.SELECT, PolicyCommand.DELETE) \
            and with_check is not None:
        target = "SELECT" if cmd is PolicyCommand.SELECT else "DELETE"
        raise ValueError(
            f"{target} policy cannot have a WITH CHECK expression"
        )
