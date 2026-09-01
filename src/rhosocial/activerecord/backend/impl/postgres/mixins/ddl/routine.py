# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/routine.py
"""PostgreSQL FUNCTION / AGGREGATE DDL implementation.

Implements CREATE/DROP FUNCTION and CREATE/DROP AGGREGATE formatting for
the postgres dialect. (CREATE/DROP PROCEDURE lives in the existing
``PostgresStoredProcedureMixin``.)

Version Requirements:
- CREATE/DROP FUNCTION: PostgreSQL 9.6+
- CREATE/DROP AGGREGATE: PostgreSQL 9.6+
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ...expression.ddl.routine import (
        PostgresCreateAggregateExpression,
        PostgresCreateFunctionExpression,
        PostgresDropAggregateExpression,
        PostgresDropFunctionExpression,
    )


class PostgresRoutineMixin:
    """PostgreSQL FUNCTION / AGGREGATE DDL implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_function_ddl(self) -> bool:
        """CREATE/DROP FUNCTION require PostgreSQL 9.6+."""
        return self.version >= (9, 6, 0)

    def supports_aggregate_ddl(self) -> bool:
        """CREATE/DROP AGGREGATE require PostgreSQL 9.6+."""
        return self.version >= (9, 6, 0)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def format_routine_ref(self, schema: Optional[str], name: str) -> str:
        """Format ``name`` (optionally schema-qualified) as identifier(s)."""
        if schema:
            return (
                f"{self.format_identifier(schema)}."
                f"{self.format_identifier(name)}"
            )
        return self.format_identifier(name)

    # ------------------------------------------------------------------ #
    # CREATE FUNCTION
    # ------------------------------------------------------------------ #
    def format_create_function_ddl_statement(
        self, expr: "PostgresCreateFunctionExpression"
    ) -> Tuple[str, tuple]:
        """Format a CREATE FUNCTION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresCreateFunctionExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.

        """
        if not self.supports_function_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE FUNCTION",
                suggestion="requires PostgreSQL 9.6+",
            )

        replace = "OR REPLACE " if expr.or_replace else ""
        base = (
            f"CREATE {replace}FUNCTION "
            f"{self.format_routine_ref(expr.schema, expr.name)}"
        )
        args = ", ".join(expr.args) if expr.args else ""
        parts: List[str] = [f"{base}({args})"]
        parts.append(f"RETURNS {expr.return_type}")
        if expr.strict:
            parts.append("STRICT")
        if expr.security is not None:
            security = expr.security.upper()
            if security not in ("DEFINER", "INVOKER"):
                raise ValueError("security must be 'DEFINER' or 'INVOKER'")
            parts.append(f"SECURITY {security}")
        if expr.cost is not None:
            parts.append(f"COST {expr.cost}")
        if expr.rows is not None:
            parts.append(f"ROWS {expr.rows}")
        parts.append(f"LANGUAGE {expr.language}")
        parts.append(f"AS $$ {expr.body} $$")
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # DROP FUNCTION
    # ------------------------------------------------------------------ #
    def format_drop_function_ddl_statement(
        self, expr: "PostgresDropFunctionExpression"
    ) -> Tuple[str, tuple]:
        """Format a DROP FUNCTION statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropFunctionExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.
            ValueError: if both ``cascade`` and ``restrict`` are True.

        """
        if not self.supports_function_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "DROP FUNCTION",
                suggestion="requires PostgreSQL 9.6+",
            )
        if expr.cascade and expr.restrict:
            raise ValueError(
                "DROP FUNCTION: CASCADE and RESTRICT are mutually exclusive"
            )
        parts: List[str] = ["DROP FUNCTION"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self.format_routine_ref(expr.schema, expr.name))
        if expr.args:
            parts.append("(" + ", ".join(expr.args) + ")")
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # CREATE AGGREGATE
    # ------------------------------------------------------------------ #
    def format_create_aggregate_ddl_statement(
        self, expr: "PostgresCreateAggregateExpression"
    ) -> Tuple[str, tuple]:
        """Format a CREATE AGGREGATE statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresCreateAggregateExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.

        """
        if not self.supports_aggregate_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE AGGREGATE",
                suggestion="requires PostgreSQL 9.6+",
            )
        base = (
            f"CREATE AGGREGATE "
            f"{self.format_routine_ref(expr.schema, expr.name)}"
        )
        options = [f"SFUNC={expr.sfunc}", f"STYPE={expr.stype}"]
        if expr.finalfunc:
            options.append(f"FINALFUNC={expr.finalfunc}")
        if expr.initcond is not None:
            options.append(f"INITCOND={expr.initcond}")
        return f"{base} ({', '.join(options)})", ()

    # ------------------------------------------------------------------ #
    # DROP AGGREGATE
    # ------------------------------------------------------------------ #
    def format_drop_aggregate_ddl_statement(
        self, expr: "PostgresDropAggregateExpression"
    ) -> Tuple[str, tuple]:
        """Format a DROP AGGREGATE statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresDropAggregateExpression`.

        Returns:
            Tuple of (string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.
            ValueError: if both ``cascade`` and ``restrict`` are True.

        """
        if not self.supports_aggregate_ddl():
            raise UnsupportedFeatureError(
                self.name,
                "DROP AGGREGATE",
                suggestion="requires PostgreSQL 9.6+",
            )
        if expr.cascade and expr.restrict:
            raise ValueError(
                "DROP AGGREGATE: CASCADE and RESTRICT are mutually exclusive"
            )
        parts: List[str] = ["DROP AGGREGATE"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self.format_routine_ref(expr.schema, expr.name))
        parts.append(f"({expr.arg_type})")
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()