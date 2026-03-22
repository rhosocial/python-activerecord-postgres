# src/rhosocial/activerecord/backend/impl/postgres/expressions.py
"""
PostgreSQL-specific expression classes for SQL statement generation.

These expression classes encapsulate parameters for PostgreSQL-specific
DDL/DML statements and delegate SQL generation to the dialect.
"""
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# =============================================================================
# DML Expressions (VACUUM, ANALYZE)
# =============================================================================

class VacuumExpression(BaseExpression):
    """PostgreSQL VACUUM statement expression.

    Encapsulates all VACUUM options with version-specific feature support:
    - PG 13+: PARALLEL option
    - PG 14+: INDEX_CLEANUP, PROCESS_TOAST options

    Example:
        >>> vacuum = VacuumExpression(
        ...     dialect,
        ...     table_name="users",
        ...     verbose=True,
        ...     analyze=True
        ... )
        >>> sql, params = vacuum.to_sql()
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
        dialect_options: Optional[Dict[str, Any]] = None
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
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_vacuum_statement(self)


class AnalyzeExpression(BaseExpression):
    """PostgreSQL ANALYZE statement expression.

    Example:
        >>> analyze = AnalyzeExpression(
        ...     dialect,
        ...     table_name="users",
        ...     verbose=True,
        ...     columns=["id", "name"]
        ... )
        >>> sql, params = analyze.to_sql()
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
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.table_name = table_name
        self.schema = schema
        self.verbose = verbose
        self.skip_locked = skip_locked
        self.columns = columns
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_analyze_statement(self)


# =============================================================================
# DDL Expressions (Partition)
# =============================================================================

class CreatePartitionExpression(BaseExpression):
    """PostgreSQL CREATE TABLE ... PARTITION OF statement expression.

    Supports RANGE, LIST, and HASH partitioning with version-specific features:
    - PG 11+: HASH partitioning, DEFAULT partition
    - PG 12+: Partition bounds expression

    Example:
        >>> partition = CreatePartitionExpression(
        ...     dialect,
        ...     partition_name="users_2024",
        ...     parent_table="users",
        ...     partition_type="RANGE",
        ...     partition_values={"from": "'2024-01-01'", "to": "'2025-01-01'"}
        ... )
        >>> sql, params = partition.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        partition_name: str,
        parent_table: str,
        partition_type: str,
        partition_values: Dict[str, Any],
        schema: Optional[str] = None,
        tablespace: Optional[str] = None,
        if_not_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.partition_type = partition_type
        self.partition_values = partition_values
        self.schema = schema
        self.tablespace = tablespace
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_create_partition_statement(self)


class DetachPartitionExpression(BaseExpression):
    """PostgreSQL ALTER TABLE ... DETACH PARTITION statement expression.

    Supports CONCURRENTLY and FINALIZE options:
    - PG 14+: DETACH CONCURRENTLY, FINALIZE

    Example:
        >>> detach = DetachPartitionExpression(
        ...     dialect,
        ...     partition_name="users_2024",
        ...     parent_table="users",
        ...     concurrently=True
        ... )
        >>> sql, params = detach.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        partition_name: str,
        parent_table: str,
        schema: Optional[str] = None,
        concurrently: bool = False,
        finalize: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.schema = schema
        self.concurrently = concurrently
        self.finalize = finalize
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_detach_partition_statement(self)


class AttachPartitionExpression(BaseExpression):
    """PostgreSQL ALTER TABLE ... ATTACH PARTITION statement expression.

    Example:
        >>> attach = AttachPartitionExpression(
        ...     dialect,
        ...     partition_name="users_2024",
        ...     parent_table="users",
        ...     partition_type="RANGE",
        ...     partition_values={"from": "'2024-01-01'", "to": "'2025-01-01'"}
        ... )
        >>> sql, params = attach.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        partition_name: str,
        parent_table: str,
        partition_type: str,
        partition_values: Dict[str, Any],
        schema: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.partition_name = partition_name
        self.parent_table = parent_table
        self.partition_type = partition_type
        self.partition_values = partition_values
        self.schema = schema
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_attach_partition_statement(self)


# =============================================================================
# DDL Expressions (Index)
# =============================================================================

class ReindexExpression(BaseExpression):
    """PostgreSQL REINDEX statement expression.

    Supports version-specific features:
    - PG 12+: CONCURRENTLY option
    - PG 14+: TABLESPACE option

    Example:
        >>> reindex = ReindexExpression(
        ...     dialect,
        ...     target_type="INDEX",
        ...     name="idx_users_email",
        ...     concurrently=True
        ... )
        >>> sql, params = reindex.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        target_type: str,  # INDEX, TABLE, SCHEMA, DATABASE
        name: str,
        schema: Optional[str] = None,
        tablespace: Optional[str] = None,
        concurrently: bool = False,
        verbose: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
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
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_reindex_statement(self)


# =============================================================================
# DDL Expressions (Statistics)
# =============================================================================

class CreateStatisticsExpression(BaseExpression):
    """PostgreSQL CREATE STATISTICS statement expression.

    Example:
        >>> stats = CreateStatisticsExpression(
        ...     dialect,
        ...     name="user_stats",
        ...     columns=["email", "created_at"],
        ...     table_name="users",
        ...     statistics_type="ndistinct"
        ... )
        >>> sql, params = stats.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        columns: List[str],
        table_name: str,
        schema: Optional[str] = None,
        statistics_type: Optional[str] = None,  # ndistinct, dependencies, mcv
        if_not_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.name = name
        self.columns = columns
        self.table_name = table_name
        self.schema = schema
        self.statistics_type = statistics_type
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_create_statistics_statement(self)


class DropStatisticsExpression(BaseExpression):
    """PostgreSQL DROP STATISTICS statement expression.

    Example:
        >>> drop_stats = DropStatisticsExpression(
        ...     dialect,
        ...     name="user_stats",
        ...     if_exists=True
        ... )
        >>> sql, params = drop_stats.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_drop_statistics_statement(self)


# =============================================================================
# DDL Expressions (Comment)
# =============================================================================

class CommentExpression(BaseExpression):
    """PostgreSQL COMMENT ON statement expression.

    Example:
        >>> comment = CommentExpression(
        ...     dialect,
        ...     object_type="TABLE",
        ...     object_name="users",
        ...     comment="User accounts table"
        ... )
        >>> sql, params = comment.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        object_type: str,  # TABLE, COLUMN, INDEX, etc.
        object_name: str,
        comment: Optional[str],
        schema: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.object_type = object_type
        self.object_name = object_name
        self.comment = comment
        self.schema = schema
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_comment_statement(self)


# =============================================================================
# DDL Expressions (Materialized View)
# =============================================================================

class RefreshMaterializedViewPgExpression(BaseExpression):
    """PostgreSQL REFRESH MATERIALIZED VIEW statement expression.

    Supports CONCURRENTLY option (PG 9.4+).

    Example:
        >>> refresh = RefreshMaterializedViewPgExpression(
        ...     dialect,
        ...     name="user_stats_mv",
        ...     concurrently=True,
        ...     with_data=False
        ... )
        >>> sql, params = refresh.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        concurrently: bool = False,
        with_data: Optional[bool] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.concurrently = concurrently
        self.with_data = with_data
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_refresh_materialized_view_pg_statement(self)


# =============================================================================
# DDL Expressions (Enum)
# =============================================================================

class CreateEnumTypeExpression(BaseExpression):
    """PostgreSQL CREATE TYPE ... AS ENUM statement expression.

    Example:
        >>> enum_type = CreateEnumTypeExpression(
        ...     dialect,
        ...     name="status_type",
        ...     values=["active", "inactive", "pending"]
        ... )
        >>> sql, params = enum_type.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        values: List[str],
        schema: Optional[str] = None,
        if_not_exists: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.name = name
        self.values = values
        self.schema = schema
        self.if_not_exists = if_not_exists
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_create_enum_type(self)


class DropEnumTypeExpression(BaseExpression):
    """PostgreSQL DROP TYPE statement expression for enum types.

    Example:
        >>> drop_enum = DropEnumTypeExpression(
        ...     dialect,
        ...     name="status_type",
        ...     if_exists=True
        ... )
        >>> sql, params = drop_enum.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.if_exists = if_exists
        self.cascade = cascade
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_drop_enum_type(self)


class AlterEnumAddValueExpression(BaseExpression):
    """PostgreSQL ALTER TYPE ... ADD VALUE statement expression.

    Example:
        >>> alter_enum = AlterEnumAddValueExpression(
        ...     dialect,
        ...     type_name="status_type",
        ...     new_value="suspended",
        ...     before="inactive"
        ... )
        >>> sql, params = alter_enum.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        type_name: str,
        new_value: str,
        schema: Optional[str] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        *,
        dialect_options: Optional[Dict[str, Any]] = None
    ):
        super().__init__(dialect)
        self.type_name = type_name
        self.new_value = new_value
        self.schema = schema
        self.before = before
        self.after = after
        self.dialect_options = dialect_options or {}

    def to_sql(self) -> Tuple[str, tuple]:
        """Delegate SQL generation to the dialect."""
        return self.dialect.format_alter_enum_add_value(self)
