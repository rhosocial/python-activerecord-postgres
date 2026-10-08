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
from rhosocial.activerecord.backend.dialect.mixins.function import FunctionMixin
from rhosocial.activerecord.backend.expression.objects import Function

if TYPE_CHECKING:
    from ...expression.ddl.routine import (
        PostgresCreateAggregateExpression,
        PostgresCreateFunctionExpression,
        PostgresDropAggregateExpression,
        PostgresDropFunctionExpression,
    )
    from rhosocial.activerecord.backend.expression.statements import (
        CreateFunctionExpression,
        DropFunctionExpression,
    )


class PostgresRoutineMixin:
    """PostgreSQL FUNCTION / AGGREGATE DDL implementation.

    Two spellings of each statement live in this backend and they are not the
    same thing. ``PostgresCreateFunctionExpression`` and
    ``PostgresDropFunctionExpression`` are the PostgreSQL-owned ones, with the
    richer option set (``SECURITY``, ``COST``, ``ROWS``, ``STRICT``) rendered by
    :meth:`format_create_function_ddl_statement` below. Core's own
    ``CreateFunctionExpression`` / ``DropFunctionExpression`` carry the portable
    SQL/PSM subset, and are rendered by delegating to core's ``FunctionMixin``.

    Both are needed: :class:`PostgresDialect` declares ``CreateRoutineSupport``
    and ``DropRoutineSupport`` in its base list, and a ``runtime_checkable``
    protocol is satisfied by its own ``...`` bodies. With no implementation
    anywhere, ``CreateFunctionExpression(...).to_sql()`` called the stub and
    returned ``None``, and ``supports_function()`` returned ``None`` too -- both
    read as falsy to every caller, and neither raised. The switches and the two
    delegating formatters below are what make that declaration true.
    """

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_function_ddl(self) -> bool:
        """CREATE/DROP FUNCTION require PostgreSQL 9.6+."""
        return self.version >= (9, 6, 0)

    def supports_aggregate_ddl(self) -> bool:
        """CREATE/DROP AGGREGATE require PostgreSQL 9.6+."""
        return self.version >= (9, 6, 0)

    def supports_function(self) -> bool:
        """PostgreSQL has user-defined functions.

        Unconditional. ``CREATE FUNCTION`` predates every version this backend
        targets; :meth:`supports_function_ddl` gates the *tracked* form, not the
        statement, and reporting ``False`` here would make
        ``isinstance(dialect, CreateRoutineSupport)``-style checks pass while
        every function statement raised.
        """
        return True

    def supports_create_function(self) -> bool:
        """Whether CREATE FUNCTION is supported.

        Tied to :meth:`supports_function_ddl` so the two cannot disagree: the
        core renderer asks this before emitting anything.
        """
        return self.supports_function_ddl()

    def supports_drop_function(self) -> bool:
        """Whether DROP FUNCTION is supported. Same gate as CREATE."""
        return self.supports_function_ddl()

    def supports_function_or_replace(self) -> bool:
        """Whether CREATE OR REPLACE FUNCTION is supported."""
        return self.supports_function_ddl()

    def supports_function_parameters(self) -> bool:
        """Whether a named parameter list is supported."""
        return self.supports_function_ddl()

    def supports_drop_function_if_exists(self) -> bool:
        """Whether DROP FUNCTION IF EXISTS is supported."""
        return self.supports_function_ddl()

    def supports_drop_function_cascade(self) -> bool:
        """Whether DROP FUNCTION CASCADE is supported."""
        return self.supports_function_ddl()

    def supports_drop_function_restrict(self) -> bool:
        """Whether DROP FUNCTION RESTRICT is supported.

        ``RESTRICT`` is the default behavior and is accepted by every version
        this backend claims; the statement itself is gated on
        :meth:`supports_function_ddl`, so the answer follows that gate.
        """
        return self.supports_function_ddl()

    # ------------------------------------------------------------------ #
    # Core's portable CREATE/DROP FUNCTION
    # ------------------------------------------------------------------ #
    def format_create_function_statement(
        self, expr: "CreateFunctionExpression"
    ) -> Tuple[str, tuple]:
        """Render core's ``CreateFunctionExpression``.

        Delegates to core's renderer instead of restating it, so the object-kind
        check and the parameter validation stay in one place -- core is where
        that logic lives, and a second copy here would be free to drift.

        Args:
            expr: Core's :class:`CreateFunctionExpression`, holding a
                :class:`~...expression.objects.Function` rather than a bare name.

        Returns:
            A ``(sql, params)`` tuple; ``params`` is empty, because a function
            body is DDL and carries no bind parameters.

        Raises:
            TypeError: ``expr.function`` is not a ``Function``.
            UnsupportedFeatureError: The configured PostgreSQL version predates
                9.6, so this backend does not claim CREATE FUNCTION.
        """
        if not isinstance(expr.function, Function):
            raise TypeError(
                f"CreateFunctionExpression.function must be a Function, "
                f"got {type(expr.function).__name__}"
            )
        return FunctionMixin.format_create_function_statement(self, expr)

    def format_drop_function_statement(
        self, expr: "DropFunctionExpression"
    ) -> Tuple[str, tuple]:
        """Render core's ``DropFunctionExpression``.

        Delegates to core's renderer for the reason given on
        :meth:`format_create_function_statement`.

        Raises:
            TypeError: ``expr.function`` is not a ``Function``.
            UnsupportedFeatureError: The configured PostgreSQL version predates
                9.6, so this backend does not claim DROP FUNCTION.
        """
        if not isinstance(expr.function, Function):
            raise TypeError(
                f"DropFunctionExpression.function must be a Function, "
                f"got {type(expr.function).__name__}"
            )
        return FunctionMixin.format_drop_function_statement(self, expr)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _format_routine_ref(
        self,
        schema: Optional[str],
        name: str,
    ) -> str:
        """Render the routine a CREATE/DROP statement names.

        Args:
            schema: The routine's schema, or ``None`` for the default one.
            name: The routine's unqualified name.

        Returns:
            The quoted, optionally schema-qualified identifier.
        """
        sql, _ = self.format_function_object(
            Function(self, name, schema_name=schema)
        )
        return sql

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
            f"{self._format_routine_ref(expr.schema, expr.name)}"
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
        parts.append(self._format_routine_ref(expr.schema, expr.name))
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
            f"{self._format_routine_ref(expr.schema, expr.name)}"
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
        parts.append(self._format_routine_ref(expr.schema, expr.name))
        parts.append(f"({expr.arg_type})")
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            parts.append("RESTRICT")
        return " ".join(parts), ()