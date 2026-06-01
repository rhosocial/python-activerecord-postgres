# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/index.py
"""
PostgreSQL DDL expressions: Index operations (CREATE/DROP/ALTER INDEX, REINDEX).

PostgreSQL Documentation:
- CREATE INDEX: https://www.postgresql.org/docs/current/sql-createindex.html
- DROP INDEX: https://www.postgresql.org/docs/current/sql-dropindex.html
- ALTER INDEX: https://www.postgresql.org/docs/current/sql-alterindex.html
- REINDEX: https://www.postgresql.org/docs/current/sql-reindex.html

Version Requirements:
- REINDEX: PostgreSQL 8.0+
- DROP INDEX CONCURRENTLY: PostgreSQL 18+
- CONCURRENTLY option: PostgreSQL 12+
- TABLESPACE option: PostgreSQL 14+
- NULLS NOT DISTINCT: PostgreSQL 15+
- CONCURRENTLY + NULLS NOT DISTINCT: PostgreSQL 16+
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.statements.ddl_index import (
    CreateIndexExpression,
    DropIndexExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import SQLPredicate


__all__ = [
    "PostgresCreateIndexExpression",
    "PostgresDropIndexExpression",
    "PostgresAlterIndexExpression",
    "PostgresAlterIndexActionType",
    "PostgresReindexExpression",
]


class PostgresCreateIndexExpression(CreateIndexExpression):
    """PostgreSQL-specific CREATE INDEX expression.

    Extends the standard CreateIndexExpression with PostgreSQL-specific
    features such as NULLS NOT DISTINCT.

    The option is stored in dialect_options under the key ``"nulls_not_distinct"``
    and is consumed by ``PostgresIndexMixin.format_create_index_statement``.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((15, 0, 0))
        >>> idx = PostgresCreateIndexExpression(
        ...     dialect=dialect,
        ...     index_name="idx_uniq_abc",
        ...     table_name="t",
        ...     columns=["a", "b", "c"],
        ...     unique=True,
        ...     nulls_not_distinct_unique=True,
        ... )
        >>> sql, params = idx.to_sql()
        >>> sql
        'CREATE UNIQUE INDEX "idx_uniq_abc" ON "t" ("a", "b", "c") NULLS NOT DISTINCT'
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        index_name: str,
        table_name: str,
        columns: List[Union[str, "BaseExpression"]],
        unique: bool = False,
        if_not_exists: bool = False,
        index_type: Optional[str] = None,
        where: Optional["SQLPredicate"] = None,
        include: Optional[List[str]] = None,
        tablespace: Optional[str] = None,
        concurrent: bool = False,
        nulls_not_distinct_unique: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        merged_options = dict(dialect_options or {})
        if nulls_not_distinct_unique:
            merged_options["nulls_not_distinct"] = True
        super().__init__(
            dialect=dialect,
            index_name=index_name,
            table_name=table_name,
            columns=columns,
            unique=unique,
            if_not_exists=if_not_exists,
            index_type=index_type,
            where=where,
            include=include,
            tablespace=tablespace,
            concurrent=concurrent,
            dialect_options=merged_options,
        )

    def to_sql(self) -> Tuple[str, tuple]:
        return self.dialect.format_create_index_statement(self)


class PostgresDropIndexExpression(DropIndexExpression):
    """PostgreSQL-specific DROP INDEX expression.

    Extends the standard DropIndexExpression with PostgreSQL-specific
    features such as CONCURRENTLY (PG 18+).

    ``concurrent`` is stored in ``dialect_options["concurrent"]`` and
    consumed by ``PostgresIndexMixin.format_drop_index_statement``.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((18, 0, 0))
        >>> expr = PostgresDropIndexExpression(
        ...     dialect=dialect,
        ...     index_name="idx_old",
        ...     concurrent=True,
        ... )
        >>> sql, params = expr.to_sql()
        >>> sql
        'DROP INDEX CONCURRENTLY "idx_old"'
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        index_name: str,
        table_name: Optional[str] = None,
        if_exists: bool = False,
        concurrent: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None,
    ):
        merged_options = dict(dialect_options or {})
        if concurrent:
            merged_options["concurrent"] = True
        super().__init__(
            dialect=dialect,
            index_name=index_name,
            table_name=table_name,
            if_exists=if_exists,
            dialect_options=merged_options,
        )

    def to_sql(self) -> Tuple[str, tuple]:
        return self.dialect.format_drop_index_statement(self)


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