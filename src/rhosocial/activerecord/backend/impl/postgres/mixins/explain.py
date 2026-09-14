# src/rhosocial/activerecord/backend/impl/postgres/mixins/explain.py
"""PostgreSQL explain feature support implementation."""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.statements.explain import ExplainExpression


class PostgresExplainMixin:
    """PostgreSQL explain override implementation.

    All features are native, using version number for detection.
    """

    def supports_explain_analyze(self) -> bool:
        return True

    def supports_explain_format(self, format_type: str) -> bool:
        format_type_upper = format_type.upper()
        supported_formats = ["TEXT", "XML", "JSON", "YAML"]
        return format_type_upper in supported_formats

    def format_explain_statement(self, expr: "ExplainExpression") -> Tuple[str, tuple]:
        """Build the PostgreSQL EXPLAIN SQL string and return (sql, params).

        PostgreSQL syntax: ``EXPLAIN [ ( option [, ...] ) ] statement``

        Supported options:
        - ``ANALYZE``
        - ``FORMAT { TEXT | XML | JSON | YAML }``
        - ``QUERY PLAN`` type — silently omitted (plain EXPLAIN is equivalent).
        """
        from rhosocial.activerecord.backend.expression.statements import ExplainType

        statement_sql, statement_params = expr.statement.to_sql()
        options = expr.options
        if options is None:
            return f"EXPLAIN {statement_sql}", statement_params

        opts: list = []

        if options.analyze:
            opts.append("ANALYZE")

        if options.format is not None:
            fmt_name = options.format.name if hasattr(options.format, "name") else str(options.format)
            opts.append(f"FORMAT {fmt_name.upper()}")
        elif options.type is not None and options.type == ExplainType.QUERY_PLAN:
            # PostgreSQL has no QUERY PLAN keyword; plain EXPLAIN is equivalent
            pass

        if opts:
            return "EXPLAIN (" + ", ".join(opts) + ") " + statement_sql, statement_params
        return f"EXPLAIN {statement_sql}", statement_params
