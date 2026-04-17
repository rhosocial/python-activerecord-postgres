# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/index.py
"""
PostgreSQL DDL expressions: Index operations (REINDEX).

PostgreSQL Documentation:
- REINDEX: https://www.postgresql.org/docs/current/sql-reindex.html

Version Requirements:
- REINDEX: PostgreSQL 8.0+
- CONCURRENTLY option: PostgreSQL 12+
- TABLESPACE option: PostgreSQL 14+
"""

from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["ReindexExpression"]


class ReindexExpression(BaseExpression):
    """PostgreSQL REINDEX statement expression.

    Rebuilt indexes to eliminate bloat, update statistics, and recover from corruption.
    Supports version-specific features.

    Attributes:
        target_type: Target type: 'INDEX', 'TABLE', 'SCHEMA', or 'DATABASE'.
        name: Name of the index/table/schema/database to reindex.
        schema: Schema name for the target.
        tablespace: Tablespace to move index (PG 14+).
        concurrently: Build index without locks (PG 12+).
        verbose: Print progress messages.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Reindex a specific index
        >>> reindex = ReindexExpression(
        ...     dialect=dialect,
        ...     target_type="INDEX",
        ...     name="users_pkey",
        ... )
        >>> sql, params = reindex.to_sql()
        >>> sql
        "REINDEX INDEX users_pkey"

        >>> # Reindex all indexes in a table concurrently (PG 12+)
        >>> reindex = ReindexExpression(
        ...     dialect=dialect,
        ...     target_type="TABLE",
        ...     name="orders",
        ...     concurrently=True,
        ...     verbose=True,
        ... )

        >>> # Reindex database with new tablespace (PG 14+)
        >>> reindex = ReindexExpression(
        ...     dialect=dialect,
        ...     target_type="DATABASE",
        ...     name="mydb",
        ...     tablespace="fast_tablespace",
        ... )
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        target_type: str,
        name: str,
        schema: Optional[str] = None,
        tablespace: Optional[str] = None,
        concurrently: bool = False,
        verbose: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.target_type = target_type
        self.name = name
        self.schema = schema
        self.tablespace = tablespace
        self.concurrently = concurrently
        self.verbose = verbose
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Generate REINDEX SQL statement.

        Returns:
            Tuple of (SQL string, empty params tuple).
        """
        return self.dialect.format_reindex_statement(self)