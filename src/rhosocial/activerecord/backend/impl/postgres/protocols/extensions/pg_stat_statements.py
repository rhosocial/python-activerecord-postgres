# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_stat_statements.py
"""pg_stat_statements extension support protocol.

This module defines the protocol for pg_stat_statements query statistics
functionality in PostgreSQL.

For SQL expression generation of simple function calls (e.g. pg_stat_statements_reset),
use the function factories in ``functions/pg_stat_statements.py`` instead of
the removed format_* methods.
"""

from typing import Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresPgStatStatementsSupport(Protocol):
    """pg_stat_statements query statistics protocol.

    Feature Source: Extension support (requires pg_stat_statements extension)

    pg_stat_statements provides query execution statistics:
    - pg_stat_statements view for query stats
    - Execution time tracking
    - Shared block I/O statistics
    - Query plan identification

    Extension Information:
    - Extension name: pg_stat_statements
    - Install command: CREATE EXTENSION pg_stat_statements;
    - Minimum version: 1.0
    - Requires preload: shared_preload_libraries = 'pg_stat_statements' in postgresql.conf
    - Documentation: https://www.postgresql.org/docs/current/pgstatstatements.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
    - Programmatic detection: dialect.is_extension_installed('pg_stat_statements')

    Notes:
    - Requires shared_preload_libraries configuration
    - Requires PostgreSQL server restart
    """

    def supports_pg_stat_statements_view(self) -> bool:
        """Whether pg_stat_statements view is available.

        Requires pg_stat_statements extension.
        Provides query execution statistics.
        """
        ...

    def supports_pg_stat_statements_reset(self) -> bool:
        """Whether pg_stat_statements_reset() function is available.

        Requires pg_stat_statements extension.
        Resets query statistics.
        """
        ...

    def format_query_stats_statement(
        self, limit: Optional[int] = None, sort_by: str = "total_exec_time", descending: bool = True
    ) -> Tuple[str, tuple]:
        """Format a query to retrieve statistics from pg_stat_statements."""
        ...

    def format_query_by_id_statement(self, queryid: int) -> Tuple[str, tuple]:
        """Format statement to get statistics for a specific query."""
        ...

    def format_top_queries_by_time(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get top queries by total execution time."""
        ...

    def format_top_queries_by_calls(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get top queries by number of calls."""
        ...

    def format_top_queries_by_rows(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get top queries by rows processed."""
        ...

    def format_io_stats_statement(self, limit: int = 10) -> Tuple[str, tuple]:
        """Format statement to get queries with most I/O."""
        ...
