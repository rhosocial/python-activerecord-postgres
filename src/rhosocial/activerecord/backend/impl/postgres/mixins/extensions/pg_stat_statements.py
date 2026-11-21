# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_stat_statements.py
"""pg_stat_statements capability probes and the single query renderer."""

from typing import Tuple

from ...expression.pg_stat import PostgresPgStatStatement


class PostgresPgStatStatementsMixin:
    """pg_stat_statements query statistics implementation."""

    def supports_pg_stat_statements_view(self) -> bool:
        return self.check_extension_feature("pg_stat_statements", "view")

    def supports_pg_stat_statements_reset(self) -> bool:
        return self.check_extension_feature("pg_stat_statements", "reset")

    def format_postgres_pg_stat_statement(
        self, expr: PostgresPgStatStatement
    ) -> Tuple[str, tuple]:
        """Render one of the standard pg_stat_statements queries."""
        kind = expr.query_kind
        if kind == "by_id":
            placeholder = self.get_parameter_placeholder()
            return (
                f"SELECT * FROM pg_stat_statements WHERE queryid = {placeholder}",
                (expr.queryid,),
            )
        if kind == "stats":
            direction = "DESC" if expr.descending else "ASC"
            sql = (
                "SELECT queryid, query, calls, total_exec_time, mean_exec_time, "
                "rows, shared_blks_hit, shared_blks_read "
                "FROM pg_stat_statements "
                f"ORDER BY {expr.sort_by} {direction}"
            )
            if expr.limit:
                sql += f" LIMIT {expr.limit}"
            return sql, ()
        if kind == "by_time":
            return (
                "SELECT queryid, query, calls, total_exec_time, mean_exec_time "
                "FROM pg_stat_statements ORDER BY total_exec_time DESC "
                f"LIMIT {expr.limit}",
                (),
            )
        if kind == "by_calls":
            return (
                "SELECT queryid, query, calls, total_exec_time "
                "FROM pg_stat_statements ORDER BY calls DESC "
                f"LIMIT {expr.limit}",
                (),
            )
        if kind == "by_rows":
            return (
                "SELECT queryid, query, calls, rows, total_exec_time "
                "FROM pg_stat_statements ORDER BY rows DESC "
                f"LIMIT {expr.limit}",
                (),
            )
        if kind == "io":
            return (
                "SELECT queryid, query, shared_blks_hit, shared_blks_read, "
                "shared_blks_dirtied FROM pg_stat_statements "
                f"ORDER BY shared_blks_read DESC LIMIT {expr.limit}",
                (),
            )
        raise ValueError(f"Unsupported pg_stat query kind: {kind!r}")
