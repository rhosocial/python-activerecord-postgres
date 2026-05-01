# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/vacuum.py
"""
PostgreSQL DDL expressions: VACUUM and ANALYZE.

PostgreSQL Documentation:
- VACUUM: https://www.postgresql.org/docs/current/sql-vacuum.html
- ANALYZE: https://www.postgresql.org/docs/current/sql-analyze.html

Version Requirements:
- VACUUM: PostgreSQL 8.0+
- ANALYZE: PostgreSQL 8.0+
- PARALLEL option: PostgreSQL 13+
- INDEX_CLEANUP, PROCESS_TOAST options: PostgreSQL 14+
- TRUNCATE option: PostgreSQL 14+
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresVacuumExpression", "PostgresAnalyzeExpression"]


class PostgresVacuumExpression(BaseExpression):
    """PostgreSQL VACUUM statement expression.

    Reclaims storage space and updates statistics for a database.
    This expression encapsulates all VACUUM options with version-specific feature support.

    Attributes:
        table_name: Optional table name to vacuum (vacuum all tables if None).
        schema: Schema name for the table.
        analyze: Also run ANALYZE after vacuuming.
        verbose: Print progress messages.
        full: Perform full vacuum (locks table).
        freeze: Freeze row transactions.
        parallel: Number of parallel workers (PG 13+, 0 disables).
        index_cleanup: Index cleanup mode: 'auto', 'on', 'off' (PG 14+).
        process_toast: Process TOAST tables: True/False/None (PG 14+).
        skip_locked: Skip tables with conflicting locks.
        truncate: Truncate empty pages (PG 14+).
        columns: Specific columns to analyze (PG 16+).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> vacuum = PostgresVacuumExpression(
        ...     dialect=dialect,
        ...     table_name="users",
        ...     analyze=True,
        ...     verbose=True,
        ... )
        >>> sql, params = vacuum.to_sql()
        >>> sql
        "VACUUM (ANALYZE, VERBOSE) users"

        >>> # Full vacuum with parallel workers (PG 13+)
        >>> vacuum = PostgresVacuumExpression(
        ...     dialect=dialect,
        ...     table_name="orders",
        ...     full=True,
        ...     parallel=4,
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        analyze: bool = False,
        verbose: bool = False,
        full: bool = False,
        freeze: bool = False,
        parallel: Optional[int] = None,
        index_cleanup: Optional[str] = None,
        process_toast: Optional[bool] = None,
        skip_locked: bool = False,
        truncate: bool = False,
        columns: Optional[List[str]] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.table_name = table_name
        self.schema = schema
        self.analyze = analyze
        self.verbose = verbose
        self.full = full
        self.freeze = freeze
        self.parallel = parallel
        self.index_cleanup = index_cleanup
        self.process_toast = process_toast
        self.skip_locked = skip_locked
        self.truncate = truncate
        self.columns = columns
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate VACUUM SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_vacuum_statement(self)


class PostgresAnalyzeExpression(BaseExpression):
    """PostgreSQL ANALYZE statement expression.

    Collects statistics about the contents of tables in the database.
    Statistics are used by the query planner to determine the most efficient execution plans.

    Attributes:
        table_name: Optional table name to analyze (analyze all tables if None).
        schema: Schema name for the table.
        verbose: Print progress messages.
        skip_locked: Skip tables with conflicting locks.
        columns: Specific columns to analyze (PG 16+).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> analyze = PostgresAnalyzeExpression(
        ...     dialect=dialect,
        ...     table_name="users",
        ...     verbose=True,
        ... )
        >>> sql, params = analyze.to_sql()
        >>> sql
        "ANALYZE VERBOSE users"

        >>> # Analyze specific columns (PG 16+)
        >>> analyze = PostgresAnalyzeExpression(
        ...     dialect=dialect,
        ...     table_name="orders",
        ...     columns=["status", "created_at"],
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        verbose: bool = False,
        skip_locked: bool = False,
        columns: Optional[List[str]] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.table_name = table_name
        self.schema = schema
        self.verbose = verbose
        self.skip_locked = skip_locked
        self.columns = columns
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate ANALYZE SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_analyze_statement(self)