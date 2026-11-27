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
)
from ...expression.load_library import PostgresLoadLibraryExpression


class PostgresPgHintPlanMixin:
    """pg_hint_plan optimizer hint capability and rendering."""

    def supports_pg_hint_plan(self) -> bool:
        """Whether optimizer hints via ``/*+ ... */`` are supported."""
        return self.check_extension_feature("pg_hint_plan", "hints")

    def _hint_phrase(self, hint: dict) -> str:
        """Render one hint payload as a pg_hint_plan phrase.

        The payload's grammar is checked here as well as in the statement's
        ``check_renderable``: a malformed payload is a caller error, so this
        raises ``TypeError`` rather than producing partial hint text.
        """
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

    Each override calls the core formatter through ``super()`` and only adds
    the comment when the statement carries hints.  This mixin therefore has to
    precede the core DQL/DML mixins in the dialect's base list; see the bundle
    rule in ``dialect.py``.
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


__all__ = [
    "PostgresHintedStatementsMixin",
    "PostgresPgHintPlanMixin",
]
