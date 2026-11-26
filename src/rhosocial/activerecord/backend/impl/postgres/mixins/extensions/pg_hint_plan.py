# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_hint_plan.py
"""PostgreSQL pg_hint_plan extension mixin.

Capability detection is version-aware via the shared extension feature map.
Hint phrases are typed expression nodes; this module is the only place that
renders them into SQL, through the two formatter methods below (one expression,
one formatter).
"""

from typing import Iterable, Tuple

from ...expression.hint import (
    HintGroup,
    HintKind,
    PostgresHintedQueryExpression,
    PostgresHintExpression,
)
from ...expression.load_library import PostgresLoadLibraryExpression


class PostgresPgHintPlanMixin:
    """pg_hint_plan optimizer hint capability and expression building."""

    def supports_pg_hint_plan(self) -> bool:
        """Whether optimizer hints via ``/*+ ... */`` are supported."""
        return self.check_extension_feature("pg_hint_plan", "hints")

    def build_hinted_query(
        self,
        query,
        hints: Iterable[PostgresHintExpression],
    ) -> PostgresHintedQueryExpression:
        """Prefix ``query`` with validated, typed pg_hint_plan hints."""
        return PostgresHintedQueryExpression(self, query, tuple(hints))

    def format_pg_load_library(self, expr: PostgresLoadLibraryExpression) -> Tuple[str, tuple]:
        """Render ``LOAD '<library>'`` (the only place building this SQL)."""
        if not isinstance(expr, PostgresLoadLibraryExpression):
            raise TypeError(
                f"format_pg_load_library expects PostgresLoadLibraryExpression, "
                f"got {type(expr).__name__}"
            )
        literal = self.format_literal(expr.library)
        return "LOAD " + literal, ()

    def format_pg_hint(self, expr: PostgresHintExpression) -> Tuple[str, tuple]:
        """Render one typed hint phrase (the only string assembly for hints)."""
        if not isinstance(expr, PostgresHintExpression):
            raise TypeError(
                f"format_pg_hint expects a PostgresHintExpression, got {type(expr).__name__}"
            )
        arguments = []
        if expr.kind is HintKind.LEADING:
            for item in expr.sequence:
                if isinstance(item, HintGroup):
                    arguments.append("(" + " ".join(item.tables) + ")")
                else:
                    arguments.append(item)
        arguments.extend(expr.tables)
        arguments.extend(expr.indexes)
        if expr.correction_kind is not None:
            arguments.append(str(expr.correction_kind) + str(expr.correction_amount))
        if expr.workers is not None:
            arguments.append(str(expr.workers))
            if expr.soft is not None:
                arguments.append("soft" if expr.soft else "hard")
        if expr.guc is not None:
            arguments.append(expr.guc)
            if expr.guc_value is not None:
                arguments.append(str(expr.guc_value))
        if not arguments:
            raise ValueError(f"{expr.kind.value} needs at least one parameter")
        return str(expr.kind) + "(" + " ".join(arguments) + ")", ()

    def format_pg_hint_plan_hinted_query(
        self, expr: PostgresHintedQueryExpression
    ) -> Tuple[str, tuple]:
        """Render ``/*+ ... */ <statement>`` with the inner statement's params."""
        if not isinstance(expr, PostgresHintedQueryExpression):
            raise TypeError(
                "format_pg_hint_plan_hinted_query expects a PostgresHintedQueryExpression, "
                f"got {type(expr).__name__}"
            )
        query_sql, query_params = expr.query.to_sql()
        hint_sql = " ".join(hint.to_sql()[0] for hint in expr.hints)
        return "/*+ " + hint_sql + " */ " + query_sql, tuple(query_params)
