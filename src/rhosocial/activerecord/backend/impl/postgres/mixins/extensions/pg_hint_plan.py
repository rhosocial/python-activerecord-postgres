# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_hint_plan.py
"""PostgreSQL pg_hint_plan extension mixin.

Capability detection is version-aware via the shared extension feature map.

Hints are values carried by a statement, not nodes of their own: this module
is the only place that renders them, through the private phrase helper and the
formatter overrides of :class:`PostgresHintedStatementsMixin`.
"""

from typing import Tuple

from ...expression.hint import (
    CorrectionKind,
    HintKind,
    assert_hint_payload,
    hint_payload,
)
from ...expression.load_library import PostgresLoadLibraryExpression


class PostgresPgHintPlanMixin:
    """pg_hint_plan optimizer hint capability and rendering."""

    def supports_pg_hint_plan(self) -> bool:
        """Whether optimizer hints via ``/*+ ... */`` are supported."""
        return self.check_extension_feature("pg_hint_plan", "hints")

    def _hint_phrase(self, hint: dict) -> str:
        """Render one hint payload as a pg_hint_plan phrase.

        The payload's grammar is checked here rather than where the statement
        was built: a node must always build, and a malformed payload is a
        caller error, so this raises ``TypeError`` rather than producing
        partial hint text.  A payload read back from an artifact may carry its
        nested arrays as lists, so :func:`hint_payload` normalises it first.
        """
        hint = hint_payload(hint)
        assert_hint_payload(hint)
        kind = HintKind(hint["kind"])
        arguments = []
        if kind is HintKind.LEADING:
            for item in hint["sequence"]:
                if isinstance(item, tuple):
                    arguments.append("(" + " ".join(item) + ")")
                else:
                    arguments.append(item)
        arguments.extend(hint["tables"])
        arguments.extend(hint["indexes"])
        if hint["correction_kind"] is not None:
            arguments.append(
                str(CorrectionKind(hint["correction_kind"]))
                + str(hint["correction_amount"])
            )
        if hint["workers"] is not None:
            arguments.append(str(hint["workers"]))
            if hint["soft"] is not None:
                arguments.append("soft" if hint["soft"] else "hard")
        if hint["guc"] is not None:
            arguments.append(hint["guc"])
            if hint["guc_value"] is not None:
                arguments.append(str(hint["guc_value"]))
        return str(kind) + "(" + " ".join(arguments) + ")"

    def _hint_comment(self, hints) -> str:
        """Render the ``/*+ ... */ `` comment prefix for a statement's hints."""
        return "/*+ " + " ".join(self._hint_phrase(hint) for hint in hints) + " */ "

    def format_pg_load_library(self, expr: PostgresLoadLibraryExpression) -> Tuple[str, tuple]:
        """Render ``LOAD '<library>'`` (the only place building this SQL)."""
        if not isinstance(expr, PostgresLoadLibraryExpression):
            raise TypeError(
                f"format_pg_load_library expects PostgresLoadLibraryExpression, "
                f"got {type(expr).__name__}"
            )
        literal = self.format_literal(expr.library)
        return "LOAD " + literal, ()


class PostgresHintedStatementsMixin:
    """Prefix a statement's SQL with its pg_hint_plan comment.

    Each override calls the formatter below it through ``super()`` -- core's
    for the DQL/DML statements and for CTAS, PostgreSQL's own for the two
    materialized-view statements -- and only adds the comment when the
    statement carries hints.  This mixin therefore has to precede every class
    it overrides in the dialect's base list: ``TableMixin`` (which carries the
    core CTAS formatter), ``PostgresMaterializedViewMixin`` and the core
    DQL/DML mixins.  See the bundle rule in ``dialect.py``.

    pg_hint_plan honours a head hint on all seven: a DQL or DML statement,
    ``CREATE TABLE ... AS``, ``CREATE MATERIALIZED VIEW ... AS``, ``REFRESH
    MATERIALIZED VIEW`` and ``EXPLAIN``.  ``CREATE VIEW`` is absent on purpose:
    it is not planned at creation time, so a hint there would be inert.
    """

    def format_query_statement(self, expr) -> Tuple[str, tuple]:
        sql, params = super().format_query_statement(expr)
        hints = getattr(expr, "hints", ())
        if not hints:
            return sql, params
        return self._hint_comment(hints) + sql, params

    def format_insert_statement(self, expr) -> Tuple[str, tuple]:
        sql, params = super().format_insert_statement(expr)
        hints = getattr(expr, "hints", ())
        if not hints:
            return sql, params
        return self._hint_comment(hints) + sql, params

    def format_update_statement(self, expr) -> Tuple[str, tuple]:
        sql, params = super().format_update_statement(expr)
        hints = getattr(expr, "hints", ())
        if not hints:
            return sql, params
        return self._hint_comment(hints) + sql, params

    def format_delete_statement(self, expr) -> Tuple[str, tuple]:
        sql, params = super().format_delete_statement(expr)
        hints = getattr(expr, "hints", ())
        if not hints:
            return sql, params
        return self._hint_comment(hints) + sql, params

    def format_create_table_as_statement(self, expr) -> Tuple[str, tuple]:
        """Prefix ``CREATE TABLE ... AS`` with its head hint.

        PostgreSQL has no formatter of its own for CTAS, so ``super()`` is
        core's portable implementation.  The head hint is honoured by the
        planner because the statement is planned when it runs (measured
        against pg_hint_plan 1.8.1 on PG 18: ``used hint`` and the plan
        changes).
        """
        sql, params = super().format_create_table_as_statement(expr)
        hints = getattr(expr, "hints", ())
        if not hints:
            return sql, params
        return self._hint_comment(hints) + sql, params

    def format_create_materialized_view_statement(self, expr) -> Tuple[str, tuple]:
        """Prefix ``CREATE MATERIALIZED VIEW ... AS`` with its head hint.

        ``super()`` is ``PostgresMaterializedViewMixin``'s PostgreSQL-specific
        builder (grammar order, storage parameters, tablespace), so the SQL
        this wraps is the same bytes that formatter produced.
        """
        sql, params = super().format_create_materialized_view_statement(expr)
        hints = getattr(expr, "hints", ())
        if not hints:
            return sql, params
        return self._hint_comment(hints) + sql, params

    def format_refresh_materialized_view_statement(self, expr) -> Tuple[str, tuple]:
        """Prefix ``REFRESH MATERIALIZED VIEW`` with its head hint.

        ``super()`` is ``PostgresMaterializedViewMixin``'s version-gated
        version, which delegates to core for the statement text.
        """
        sql, params = super().format_refresh_materialized_view_statement(expr)
        hints = getattr(expr, "hints", ())
        if not hints:
            return sql, params
        return self._hint_comment(hints) + sql, params


__all__ = [
    "PostgresHintedStatementsMixin",
    "PostgresPgHintPlanMixin",
]
