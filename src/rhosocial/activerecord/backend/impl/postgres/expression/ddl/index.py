# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/index.py
"""
PostgreSQL DDL expressions: Index operations (ALTER INDEX, REINDEX).

PostgreSQL Documentation:
- ALTER INDEX: https://www.postgresql.org/docs/current/sql-alterindex.html
- REINDEX: https://www.postgresql.org/docs/current/sql-reindex.html

Version Requirements:
- REINDEX: PostgreSQL 8.0+
- TABLESPACE option: PostgreSQL 14+
- CONCURRENTLY option: PostgreSQL 12+

CREATE / DROP INDEX — use the generic expressions with ``dialect_options``
==========================================================================
This module intentionally does **not** define ``PostgresCreateIndexExpression``
or ``PostgresDropIndexExpression``.  The generic classes from the core
package already serve this purpose:

    from rhosocial.activerecord.backend.expression.statements.ddl_index import (
        CreateIndexExpression,
        DropIndexExpression,
    )

PostgreSQL‑specific options are supplied via the ``dialect_options`` dict and
consumed by ``PostgresIndexMixin.format_create_index_statement`` /
``format_drop_index_statement``.  Supported keys:

======================= =======================================================
Key                     Description
======================= =======================================================
``nulls_not_distinct``  ``bool`` — add ``NULLS NOT DISTINCT`` (PG 15+, unique
                        index only).  Passed to ``CreateIndexExpression``.
``opclasses``           ``dict[str, str]`` — operator classes per column, e.g.
                        ``{"a": "text_pattern_ops"}``.  Passed to
                        ``CreateIndexExpression``.
``with``                ``dict[str, Any]`` — storage parameters, e.g.
                        ``{"fillfactor": 70}``.  Passed to
                        ``CreateIndexExpression``.
``concurrent``          ``bool`` — add ``CONCURRENTLY`` to ``DROP INDEX``
                        (PG 18+).  Passed to ``DropIndexExpression``.
                        (For ``CREATE INDEX`` the generic class already has
                        a ``concurrent`` named parameter.)
======================= =======================================================

Example::

    from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

    d = PostgresDialect((15, 0, 0))

    expr = CreateIndexExpression(
        d, "idx_uniq_abc", "t", ["a", "b"],
        unique=True,
        dialect_options={"nulls_not_distinct": True},
    )
    sql, _ = expr.to_sql()   # → CREATE UNIQUE INDEX … NULLS NOT DISTINCT
"""

from enum import Enum
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresAlterIndexExpression",
    "PostgresAlterIndexActionType",
    "PostgresReindexExpression",
]


class PostgresAlterIndexActionType(Enum):
    """Action types for ALTER INDEX statement."""

    RENAME_TO = "RENAME TO"
    SET_TABLESPACE = "SET TABLESPACE"
    SET_STORAGE_PARAMETERS = "SET"
    RESET_STORAGE_PARAMETERS = "RESET"
    ALTER_COLUMN_STATISTICS = "ALTER COLUMN SET STATISTICS"
    ALL_IN_TABLESPACE = "ALL IN TABLESPACE"


class PostgresAlterIndexExpression(BaseExpression):
    """PostgreSQL ALTER INDEX statement expression.

    Supports all ALTER INDEX operations:
    - Rename index
    - Move index to a different tablespace
    - Set/reset storage parameters
    - Alter per-column statistics target
    - Bulk tablespace move with ALL IN TABLESPACE

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Rename index
        >>> expr = PostgresAlterIndexExpression(
        ...     dialect, "idx_old",
        ...     action_type=PostgresAlterIndexActionType.RENAME_TO,
        ...     new_name="idx_new",
        ... )
        >>> sql, params = expr.to_sql()
        >>> sql
        'ALTER INDEX "idx_old" RENAME TO "idx_new"'

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        index_name: str,
        action_type: PostgresAlterIndexActionType,
        *,
        if_exists: bool = False,
        new_name: Optional[str] = None,
        tablespace: Optional[str] = None,
        storage_parameters: Optional[Dict[str, Any]] = None,
        column_number: Optional[int] = None,
        statistics_target: Optional[int] = None,
        source_tablespace: Optional[str] = None,
        target_tablespace: Optional[str] = None,
        nowait: bool = False,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(dialect)
        self.index_name = index_name
        self.action_type = action_type
        self.if_exists = if_exists
        self.new_name = new_name
        self.tablespace = tablespace
        self.storage_parameters = storage_parameters
        self.column_number = column_number
        self.statistics_target = statistics_target
        self.source_tablespace = source_tablespace
        self.target_tablespace = target_tablespace
        self.nowait = nowait
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        return self.dialect.format_alter_index_statement(self)


class PostgresReindexExpression(BaseExpression):
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
        >>> reindex = PostgresReindexExpression(
        ...     dialect=dialect,
        ...     target_type="INDEX",
        ...     name="users_pkey",
        ... )
        >>> sql, params = reindex.to_sql()
        >>> sql
        "REINDEX INDEX users_pkey"

        >>> # Reindex all indexes in a table concurrently (PG 12+)
        >>> reindex = PostgresReindexExpression(
        ...     dialect=dialect,
        ...     target_type="TABLE",
        ...     name="orders",
        ...     concurrently=True,
        ...     verbose=True,
        ... )

        >>> # Reindex database with new tablespace (PG 14+)
        >>> reindex = PostgresReindexExpression(
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
