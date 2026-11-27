# src/rhosocial/activerecord/backend/impl/postgres/mixins/explain.py
"""PostgreSQL explain feature support implementation."""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ....expression.statements.explain import ExplainStatement


#: ExplainFormat value -> the capability switch that answers for it.
_FORMAT_SWITCHES = {
    "TEXT": "supports_explain_format_text",
    "JSON": "supports_explain_format_json",
    "XML": "supports_explain_format_xml",
    "YAML": "supports_explain_format_yaml",
    "TREE": "supports_explain_format_tree",
    "TRADITIONAL": "supports_explain_format_traditional",
}


class PostgresExplainMixin:
    """PostgreSQL EXPLAIN capability switches and formatter.

    PostgreSQL's ``EXPLAIN ( option [, ...] )`` grammar has accepted ANALYZE,
    VERBOSE, COSTS, BUFFERS, TIMING, SETTINGS, WAL and the TEXT/JSON/XML/YAML
    formats since the option list was introduced, so every switch here is a
    constant rather than a version comparison. TREE and TRADITIONAL belong to
    other engines and PostgreSQL has no such format, so those answer False.
    """

    def supports_explain_analyze(self) -> bool:
        return True

    def supports_explain_verbose(self) -> bool:
        return True

    def supports_explain_costs(self) -> bool:
        return True

    def supports_explain_buffers(self) -> bool:
        return True

    def supports_explain_timing(self) -> bool:
        return True

    def supports_explain_settings(self) -> bool:
        return True

    def supports_explain_wal(self) -> bool:
        return True

    def supports_explain_format_text(self) -> bool:
        return True

    def supports_explain_format_json(self) -> bool:
        return True

    def supports_explain_format_xml(self) -> bool:
        return True

    def supports_explain_format_yaml(self) -> bool:
        return True

    def supports_explain_format_tree(self) -> bool:
        return False

    def supports_explain_format_traditional(self) -> bool:
        return False

    def format_explain_statement(self, expr: "ExplainStatement") -> Tuple[str, tuple]:
        """Build the PostgreSQL EXPLAIN SQL string and return (sql, params).

        PostgreSQL syntax: ``EXPLAIN [ ( option [, ...] ) ] statement``

        Supported options:
        - ``ANALYZE``
        - ``VERBOSE``
        - ``COSTS OFF`` (costs are on by default, so only the off state is written)
        - ``BUFFERS``
        - ``TIMING`` (valid only alongside ANALYZE)
        - ``SETTINGS``
        - ``WAL``
        - ``FORMAT { TEXT | XML | JSON | YAML }``
        - ``QUERY PLAN`` type is silently omitted (plain EXPLAIN is equivalent).

        Raises:
            UnsupportedFeatureError: A requested option has no PostgreSQL
                grammar, judged by the capability switch that answers for it.
                Without the check an unsupported format would be spelled
                ``FORMAT TRADITIONAL`` and rejected by the server instead.
        """
        from rhosocial.activerecord.backend.expression.statements import ExplainType

        # The hint comment belongs to exactly one statement.  A hinted EXPLAIN
        # wraps a statement that may already carry its own hints, and both
        # prefixes would then be written -- so that shape is refused rather
        # than rendered ambiguously.
        if getattr(expr.statement, "hints", ()):
            raise TypeError(
                "EXPLAIN cannot carry hints for a statement that already "
                "carries its own: the comment position would be ambiguous"
            )
        statement_sql, statement_params = expr.statement.to_sql()
        # pg_hint_plan reads the hint comment before the statement, and an
        # EXPLAIN is the statement as far as the planner is concerned, so the
        # comment goes ahead of the EXPLAIN keyword.
        hints = getattr(expr, "hints", ())
        prefix = self._hint_comment(hints) if hints else ""
        options = expr.options
        if options is None:
            return prefix + f"EXPLAIN {statement_sql}", statement_params

        opts: list = []
        analyze_requested = options.analyze or options.type == ExplainType.ANALYZE

        if analyze_requested:
            if not self.supports_explain_analyze():
                raise UnsupportedFeatureError(self.name, "EXPLAIN ANALYZE")
            opts.append("ANALYZE")

        if options.format is not None:
            fmt_name = getattr(options.format, "value", str(options.format)).upper()
            switch = _FORMAT_SWITCHES.get(fmt_name)
            if switch is None or not getattr(self, switch)():
                raise UnsupportedFeatureError(
                    self.name, f"EXPLAIN FORMAT {fmt_name}"
                )
            opts.append(f"FORMAT {fmt_name}")
        elif options.type is not None and options.type == ExplainType.QUERY_PLAN:
            # PostgreSQL has no QUERY PLAN keyword; plain EXPLAIN is equivalent
            pass

        if not options.costs:
            if not self.supports_explain_costs():
                raise UnsupportedFeatureError(self.name, "EXPLAIN COSTS")
            opts.append("COSTS OFF")

        if options.buffers:
            if not self.supports_explain_buffers():
                raise UnsupportedFeatureError(self.name, "EXPLAIN BUFFERS")
            opts.append("BUFFERS")

        if options.timing and analyze_requested:
            if not self.supports_explain_timing():
                raise UnsupportedFeatureError(self.name, "EXPLAIN TIMING")
            opts.append("TIMING ON")

        if options.verbose:
            if not self.supports_explain_verbose():
                raise UnsupportedFeatureError(self.name, "EXPLAIN VERBOSE")
            opts.append("VERBOSE")

        if options.settings:
            if not self.supports_explain_settings():
                raise UnsupportedFeatureError(self.name, "EXPLAIN SETTINGS")
            opts.append("SETTINGS")

        if options.wal:
            if not self.supports_explain_wal():
                raise UnsupportedFeatureError(self.name, "EXPLAIN WAL")
            opts.append("WAL")

        if opts:
            return prefix + "EXPLAIN (" + ", ".join(opts) + ") " + statement_sql, statement_params
        return prefix + f"EXPLAIN {statement_sql}", statement_params
