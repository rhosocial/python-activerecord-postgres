# src/rhosocial/activerecord/backend/impl/postgres/expression/pg_stat.py
"""A pg_stat_statements query as an expression node."""

from typing import Optional

from rhosocial.activerecord.backend.expression.bases import BaseExpression


class PostgresPgStatStatement(BaseExpression):
    """One of the standard queries against ``pg_stat_statements``."""

    QUERY_KINDS = (
        "stats", "by_id", "by_time", "by_calls", "by_rows", "io",
    )

    def __init__(
        self,
        dialect,
        query_kind: str,
        *,
        limit: Optional[int] = None,
        sort_by: str = "total_exec_time",
        descending: bool = True,
        queryid: Optional[int] = None,
    ):
        super().__init__(dialect)
        if query_kind not in self.QUERY_KINDS:
            raise ValueError(f"Unsupported pg_stat query kind: {query_kind!r}")
        self.query_kind = query_kind
        self.limit = limit
        self.sort_by = sort_by
        self.descending = descending
        self.queryid = queryid

    @property
    def format_method(self) -> str:
        return "format_postgres_pg_stat_statement"


__all__ = ["PostgresPgStatStatement"]
