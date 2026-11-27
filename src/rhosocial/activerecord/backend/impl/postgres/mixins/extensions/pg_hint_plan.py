# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_hint_plan.py
"""PostgreSQL pg_hint_plan extension mixin.

Capability detection is version-aware via the shared extension feature map.

Hints are values carried by a *derived* statement, not nodes of their own: the
``hints`` parameter lives on the PostgreSQL-derived classes in
:mod:`...expression.hinted_statements`, so core never sees it.  This module is
the only place that renders it, through the private phrase helper and the
formatter overrides of :class:`PostgresHintedStatementsMixin`.  Each override
handles both the generic expression and the derived one (``fmt^c : {e, e'} -> S``
of backend-expression-families §5.3): the generic expression renders exactly as
before, and only a derived instance carrying hints is prefixed.
"""

from typing import Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

from ...expression.hint import (
    CorrectionKind,
    HintKind,
    assert_hint_payload,
    hint_payload,
)
from ...expression.ddl.mv import (
    PostgresCreateMaterializedViewStatement,
    PostgresRefreshMaterializedViewStatement,
)
from ...expression.hinted_statements import (
    PostgresCreateTableAsStatement,
    PostgresDeleteStatement,
    PostgresExplainStatement,
    PostgresInsertStatement,
    PostgresQueryStatement,
    PostgresUpdateStatement,
)
from ...expression.load_library import PostgresLoadLibraryExpression

#: The hinted statement kinds an outer statement may wrap. A hint belongs to
#: the outermost statement, so finding one of these inside another statement's
#: query slot is a structural error, not a second comment position.
_NESTED_HINTED_STATEMENTS = (
    PostgresQueryStatement,
    PostgresInsertStatement,
    PostgresUpdateStatement,
    PostgresDeleteStatement,
)


#: Hints that name a join-level plan node: gated by both the PostgreSQL 17+
#: server and the pg_hint_plan release that introduced them (1.4.0).
_MEMOIZE_KINDS = frozenset({HintKind.MEMOIZE, HintKind.NO_MEMOIZE})


class PostgresPgHintPlanMixin:
    """pg_hint_plan optimizer hint capability and rendering."""

    def supports_pg_hint_plan(self) -> bool:
        """Whether optimizer hints via ``/*+ ... */`` are supported."""
        return self.check_extension_feature("pg_hint_plan", "hints")

    def _assert_hint_capability(self, kind: HintKind) -> None:
        """Refuse a kind the server or loaded extension cannot spell.

        Capability is probed at render time, not when the payload is built: the
        same payload can travel to a newer server, and a node must always build.
        ``version_at_least`` and ``check_extension_feature`` are the dialect's
        existing probes; the installed version and the required minimum are
        named in the error so it is clear which side of the gate failed.
        Gate minimums come from upstream release notes and were re-verified on
        pg-ts18 (PG 18.7 + pg_hint_plan 1.8.1).
        """
        if kind in _MEMOIZE_KINDS:
            if not self.version_at_least((17, 0, 0)):
                version = self.version_or_none
                raise UnsupportedFeatureError(
                    self.name,
                    f"{kind.value} hint (PostgreSQL 17+)",
                    f"the Memoize node is absent on the current server version "
                    f"{'.'.join(str(part) for part in version) if version else 'unknown'}; "
                    f"connect to PostgreSQL 17 or newer, or drop the hint.",
                )
            if not self.check_extension_feature("pg_hint_plan", "memoize"):
                installed = self.get_extension_version("pg_hint_plan")
                minimum = self.get_extension_min_version_for_feature(
                    "pg_hint_plan", "memoize"
                )
                raise UnsupportedFeatureError(
                    self.name,
                    f"{kind.value} hint (pg_hint_plan {minimum}+)",
                    f"the loaded pg_hint_plan version is {installed or 'unknown'}; "
                    f"load pg_hint_plan {minimum} or newer, or drop the hint.",
                )
        if kind is HintKind.DISABLE_INDEX and not self.check_extension_feature(
            "pg_hint_plan", "disable_index"
        ):
            installed = self.get_extension_version("pg_hint_plan")
            minimum = self.get_extension_min_version_for_feature(
                "pg_hint_plan", "disable_index"
            )
            raise UnsupportedFeatureError(
                self.name,
                f"DisableIndex hint (pg_hint_plan {minimum}+)",
                f"the loaded pg_hint_plan version is {installed or 'unknown'}; "
                f"load pg_hint_plan {minimum} or newer, or drop the hint.",
            )

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
        self._assert_hint_capability(kind)
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
                str(CorrectionKind(hint["correction_kind"])) + str(hint["correction_amount"])
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
    """Prefix a hinted statement's SQL with its pg_hint_plan comment.

    Each override calls the formatter below it through ``super()`` and only
    adds the comment when the expression is the PostgreSQL-derived class that
    carries the parameter.  A generic statement has no ``hints`` attribute at
    all, so the type -- not a probe -- says whether the node carries one.

    This mixin must precede every class it overrides in the dialect's base list:
    ``PostgresExplainMixin`` and ``PostgresViewMixin`` (the EXPLAIN and CREATE
    VIEW formatters it wraps), ``PostgresMaterializedViewMixin`` (the two
    materialized-view formatters), ``TableMixin`` (core's CTAS formatter) and
    the core DQL/DML mixins.  See the bundle rule in ``dialect.py``.

    pg_hint_plan honours a head hint on all eight statements it plans: a DQL
    or DML statement, ``CREATE TABLE ... AS``, ``CREATE MATERIALIZED VIEW ... AS``,
    ``REFRESH MATERIALIZED VIEW`` and ``EXPLAIN``.  ``CREATE VIEW`` is absent
    on purpose: it is not planned at creation time, so a hint there is inert
    (and is refused rather than dropped).
    """

    def _require_hint_plan(self) -> None:
        """Refuse a hinted statement on a dialect that cannot spell the comment."""
        if not self.supports_pg_hint_plan():
            raise UnsupportedFeatureError(
                self.name,
                "optimizer hints",
                "install and enable the pg_hint_plan extension, or drop the hints.",
            )

    @staticmethod
    def _refuse_nested_hint(inner) -> None:
        """A hint belongs to the statement whose SQL is planned, not a nested one."""
        raise TypeError(
            f"a pg_hint_plan hint belongs to the outermost statement: "
            f"{type(inner).__name__} carries hints inside another statement. "
            f"Give the hint to the outermost statement instead"
        )

    def format_query_statement(self, expr) -> Tuple[str, tuple]:
        sql, params = super().format_query_statement(expr)
        if not isinstance(expr, PostgresQueryStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_insert_statement(self, expr) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression.statements.dml import (
            SelectSource,
        )

        source = expr.source
        if (
            isinstance(source, SelectSource)
            and isinstance(source.select_query, PostgresQueryStatement)
            and source.select_query.hints
        ):
            self._refuse_nested_hint(source.select_query)
        sql, params = super().format_insert_statement(expr)
        if not isinstance(expr, PostgresInsertStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_update_statement(self, expr) -> Tuple[str, tuple]:
        sql, params = super().format_update_statement(expr)
        if not isinstance(expr, PostgresUpdateStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_delete_statement(self, expr) -> Tuple[str, tuple]:
        sql, params = super().format_delete_statement(expr)
        if not isinstance(expr, PostgresDeleteStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_explain_statement(self, expr) -> Tuple[str, tuple]:
        # The comment goes ahead of the EXPLAIN keyword, which is the position
        # pg_hint_plan documents; the wrapped statement must not carry its own.
        if isinstance(expr.statement, _NESTED_HINTED_STATEMENTS) and expr.statement.hints:
            self._refuse_nested_hint(expr.statement)
        sql, params = super().format_explain_statement(expr)
        if not isinstance(expr, PostgresExplainStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_create_table_as_statement(self, expr) -> Tuple[str, tuple]:
        """Prefix ``CREATE TABLE ... AS`` with its head hint.

        PostgreSQL has no formatter of its own for CTAS, so ``super()`` is
        core's portable implementation.  The head hint is honoured by the
        planner because the statement is planned when it runs (measured
        against pg_hint_plan 1.8.1 on PG 18: ``used hint`` and the plan
        changes).  Give the hint to this statement, not to the query it wraps.
        """
        if isinstance(expr.as_query, PostgresQueryStatement) and expr.as_query.hints:
            self._refuse_nested_hint(expr.as_query)
        sql, params = super().format_create_table_as_statement(expr)
        if not isinstance(expr, PostgresCreateTableAsStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_create_materialized_view_statement(self, expr) -> Tuple[str, tuple]:
        """Prefix ``CREATE MATERIALIZED VIEW ... AS`` with its head hint.

        ``super()`` is ``PostgresMaterializedViewMixin``'s PostgreSQL-specific
        builder (grammar order, storage parameters, tablespace), so the SQL
        this wraps is the same bytes that formatter produced.
        """
        if isinstance(expr.query, PostgresQueryStatement) and expr.query.hints:
            self._refuse_nested_hint(expr.query)
        sql, params = super().format_create_materialized_view_statement(expr)
        if not isinstance(expr, PostgresCreateMaterializedViewStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_refresh_materialized_view_pg_statement(self, expr) -> Tuple[str, tuple]:
        """Prefix ``REFRESH MATERIALIZED VIEW`` with its head hint.

        ``PostgresRefreshMaterializedViewStatement`` declares this formatter
        (it has its own PostgreSQL statement kind, with the ``concurrently``
        alias), so the override lives here rather than on the generic refresh
        formatter.  ``super()`` is ``PostgresMaterializedViewMixin``'s
        version-gated implementation.
        """
        sql, params = super().format_refresh_materialized_view_pg_statement(expr)
        if not isinstance(expr, PostgresRefreshMaterializedViewStatement):
            return sql, params
        if not expr.hints:
            return sql, params
        self._require_hint_plan()
        return self._hint_comment(expr.hints) + sql, params

    def format_create_view_statement(self, expr) -> Tuple[str, tuple]:
        """Refuse a hinted query inside ``CREATE VIEW``.

        A view is not planned when it is created, so pg_hint_plan ignores the
        comment (measured: no log line).  The hint is therefore not rendered
        here; a hinted query inside the view body is refused by name rather
        than silently made inert.
        """
        if isinstance(expr.query, PostgresQueryStatement) and expr.query.hints:
            self._refuse_nested_hint(expr.query)
        return super().format_create_view_statement(expr)


__all__ = [
    "PostgresHintedStatementsMixin",
    "PostgresPgHintPlanMixin",
]
