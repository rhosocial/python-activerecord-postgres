# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_stat_statements.py
"""pg_stat_statements extension mixin implementation."""

from typing import Tuple, Optional


class PostgresPgStatStatementsMixin:
    """pg_stat_statements query statistics implementation."""

    def supports_pg_stat_statements_view(self) -> bool:
        """Check if pg_stat_statements supports view."""
        return self.check_extension_feature("pg_stat_statements", "view")

    def supports_pg_stat_statements_reset(self) -> bool:
        """Check if pg_stat_statements supports reset."""
        return self.check_extension_feature("pg_stat_statements", "reset")

    def format_query_stats_statement(
        self, limit: Optional[int] = None, sort_by: str = "total_exec_time", descending: bool = True
    ) -> Tuple[str, tuple]:
        """Format a query to retrieve statistics from pg_stat_statements.

        Args:
            limit: Maximum number of statements to return
            sort_by: Column to sort by (total_exec_time, calls, rows, etc.)
            descending: Sort in descending order

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_query_stats_statement(limit=10)
            ("SELECT queryid, query, calls, total_exec_time, rows, shared_blks_hit, "
             "shared_blks_read FROM pg_stat_statements ORDER BY total_exec_time DESC "
             "LIMIT 10", ())
        """
        direction = "DESC" if descending else "ASC"
        sql = f"""SELECT queryid, query, calls, total_exec_time, mean_exec_time,
                  rows, shared_blks_hit, shared_blks_read
                  FROM pg_stat_statements
                  ORDER BY {sort_by} {direction}"""
        if limit:
            sql += f" LIMIT {limit}"
        return (sql, ())

    def format_reset_stats_statement(self) -> Tuple[str, tuple]:
        """Format statement to reset pg_stat_statements statistics.

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_reset_stats_statement()
            ("SELECT pg_stat_statements_reset()", ())
        """
        return ("SELECT pg_stat_statements_reset()", ())

    def format_query_by_id_statement(self, queryid: int) -> Tuple[str, tuple]:
        """Format statement to get statistics for a specific query.

        Args:
            queryid: The query ID to look up

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_query_by_id_statement(12345)
            ("SELECT * FROM pg_stat_statements WHERE queryid = $1", (12345,))
        """
        return ("SELECT * FROM pg_stat_statements WHERE queryid = $1", (queryid,))

    def format_top_queries_by_time(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get top queries by total execution time.

        Args:
            limit: Number of queries to return

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_top_queries_by_time(5)
            ("SELECT queryid, query, calls, total_exec_time, mean_exec_time "
             "FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 5", ())
        """
        return (
            f"SELECT queryid, query, calls, total_exec_time, mean_exec_time "
            f"FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT {limit}",
            (),
        )

    def format_top_queries_by_calls(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get top queries by number of calls.

        Args:
            limit: Number of queries to return

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_top_queries_by_calls(5)
            ("SELECT queryid, query, calls, total_exec_time FROM pg_stat_statements ORDER BY calls DESC LIMIT 5", ())
        """
        return (
            f"SELECT queryid, query, calls, total_exec_time FROM pg_stat_statements ORDER BY calls DESC LIMIT {limit}",
            (),
        )

    def format_top_queries_by_rows(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get top queries by rows processed.

        Args:
            limit: Number of queries to return

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_top_queries_by_rows(5)
            ("SELECT queryid, query, calls, rows, total_exec_time "
             "FROM pg_stat_statements ORDER BY rows DESC LIMIT 5", ())
        """
        return (
            f"SELECT queryid, query, calls, rows, total_exec_time "
            f"FROM pg_stat_statements ORDER BY rows DESC LIMIT {limit}",
            (),
        )

    def format_io_stats_statement(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get queries with most I/O.

        Args:
            limit: Number of queries to return

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_io_stats_statement(5)
            ("SELECT queryid, query, shared_blks_hit, shared_blks_read, shared_blks_dirtied "
             "FROM pg_stat_statements ORDER BY shared_blks_read DESC LIMIT 5", ())
        """
        return (
            f"SELECT queryid, query, shared_blks_hit, shared_blks_read, shared_blks_dirtied "
            f"FROM pg_stat_statements ORDER BY shared_blks_read DESC LIMIT {limit}",
            (),
        )
