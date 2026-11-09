# src/rhosocial/activerecord/backend/impl/postgres/expression/introspection.py
"""PostgreSQL-specific introspection expressions.

These expressions reuse the core parameter container contract
(``IntrospectionExpression.get_params()``) and only redirect SQL generation to
PostgreSQL-specific formatters, so the core introspection layer stays untouched.
The ``pg_database_size`` pair and the ``pg_relation_size`` /
``pg_indexes_size`` trio follow the same split for questions core has no class
for: the function node carries the call, the query node carries the row shape
the caller reads.
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.introspection import (
    IntrospectionExpression,
    ViewInfoQueryStatement,
    ViewListQueryStatement,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresMaterializedViewInfoQueryStatement",
    "PostgresMaterializedViewListQueryStatement",
    "PostgresSequenceInfoQueryStatement",
    "PostgresSequenceListQueryStatement",
    "PostgresPartitionListQueryStatement",
    "PostgresRoutineListQueryStatement",
    "PostgresTypeListQueryStatement",
    "PostgresOperatorListQueryStatement",
    "PostgresOperatorClassListQueryStatement",
    "PostgresOperatorFamilyListQueryStatement",
    "PostgresDatabaseSizeFunction",
    "PostgresDatabaseSizeQueryStatement",
    "PostgresRelationSizeFunction",
    "PostgresIndexesSizeFunction",
    "PostgresTableStorageQueryStatement",
]


class PostgresMaterializedViewListQueryStatement(ViewListQueryStatement):
    """List materialized views (``pg_class.relkind = 'm'``).

    Example:
        >>> expr = PostgresMaterializedViewListQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        include_system: bool = False,
    ):
        super().__init__(dialect, schema=schema, include_system=include_system)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_materialized_view_list_query_statement"


class PostgresMaterializedViewInfoQueryStatement(ViewInfoQueryStatement):
    """Fetch a single materialized view by name.

    Example:
        >>> expr = PostgresMaterializedViewInfoQueryStatement(dialect, "sales_summary")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        view_name: str,
        schema: Optional[str] = None,
        include_columns: bool = True,
    ):
        super().__init__(
            dialect,
            view_name=view_name,
            schema=schema,
            include_columns=include_columns,
        )

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_materialized_view_info_query_statement"


class PostgresSequenceListQueryStatement(IntrospectionExpression):
    """List the sequences in one schema.

    ``information_schema.sequences`` is the object list; ``pg_sequences`` adds
    the runtime state it does not carry (``cache_size`` and ``last_value``).
    ``include_pg_sequences`` says whether that join is available: the catalog
    view arrived in PostgreSQL 10, so a 9.x server is asked the narrower
    question and reports those two columns as absent.

    Example:
        >>> expr = PostgresSequenceListQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        include_pg_sequences: bool = True,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            schema: Optional schema name. Defaults to the connection schema.
            include_pg_sequences: Whether the server has ``pg_sequences``
                (PostgreSQL 10+).
        """
        super().__init__(dialect, schema=schema)
        self.include_pg_sequences = include_pg_sequences

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_sequence_list_query_statement"


class PostgresSequenceInfoQueryStatement(IntrospectionExpression):
    """Fetch a single sequence by name.

    Example:
        >>> expr = PostgresSequenceInfoQueryStatement(dialect, "order_id_seq")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        sequence_name: str,
        schema: Optional[str] = None,
        include_pg_sequences: bool = True,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            sequence_name: Name of the sequence to read.
            schema: Optional schema name. Defaults to the connection schema.
            include_pg_sequences: Whether the server has ``pg_sequences``
                (PostgreSQL 10+).
        """
        super().__init__(dialect, schema=schema)
        self.sequence_name = sequence_name
        self.include_pg_sequences = include_pg_sequences

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_sequence_info_query_statement"


class PostgresPartitionListQueryStatement(IntrospectionExpression):
    """List the child partitions of one partitioned table.

    The parent is named by the caller; the catalog answers its partition key
    (``pg_get_partkeydef``) and each child's bound
    (``pg_get_expr(relpartbound)``). ``include_statistics`` adds the child's
    own row estimate and total size, which cost a catalog lookup each but let
    a caller report storage without a second question.

    Example:
        >>> expr = PostgresPartitionListQueryStatement(dialect, "orders", schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: str,
        schema: Optional[str] = None,
        include_partitions: bool = True,
        include_statistics: bool = True,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            table_name: Name of the partitioned parent table.
            schema: Optional schema name. Defaults to the connection schema.
            include_partitions: Whether to list the child partitions. ``False``
                answers only the parent's key.
            include_statistics: Whether to add each child's row estimate and
                size in bytes.
        """
        super().__init__(dialect, schema=schema)
        if not table_name:
            raise ValueError("table_name must not be empty")
        self.table_name = table_name
        self.include_partitions = include_partitions
        self.include_statistics = include_statistics

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_partition_list_query_statement"


class PostgresRoutineListQueryStatement(IntrospectionExpression):
    """List PostgreSQL routines from ``pg_proc``, optionally narrowed by name.

    Example:
        >>> expr = PostgresRoutineListQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        routine_name: Optional[str] = None,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            schema: Optional schema name. Defaults to the connection schema.
            routine_name: Optional routine name; it narrows the listing.
        """
        super().__init__(dialect, schema=schema)
        self.routine_name = routine_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_routine_list_query_statement"


class PostgresTypeListQueryStatement(IntrospectionExpression):
    """List PostgreSQL enum/domain/composite/range/multirange types.

    Example:
        >>> expr = PostgresTypeListQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        type_name: Optional[str] = None,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            schema: Optional schema name. Defaults to the connection schema.
            type_name: Optional type name; it narrows the listing.
        """
        super().__init__(dialect, schema=schema)
        self.type_name = type_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_type_list_query_statement"


class PostgresOperatorListQueryStatement(IntrospectionExpression):
    """List PostgreSQL operators from ``pg_operator``.

    Example:
        >>> expr = PostgresOperatorListQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        operator_name: Optional[str] = None,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            schema: Optional schema name. Defaults to the connection schema.
            operator_name: Optional operator name; it narrows the listing.
        """
        super().__init__(dialect, schema=schema)
        self.operator_name = operator_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_operator_list_query_statement"


class PostgresOperatorClassListQueryStatement(IntrospectionExpression):
    """List PostgreSQL operator classes from ``pg_opclass``.

    Example:
        >>> expr = PostgresOperatorClassListQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        operator_class_name: Optional[str] = None,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            schema: Optional schema name. Defaults to the connection schema.
            operator_class_name: Optional operator class name; it narrows the
                listing.
        """
        super().__init__(dialect, schema=schema)
        self.operator_class_name = operator_class_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_operator_class_list_query_statement"


class PostgresOperatorFamilyListQueryStatement(IntrospectionExpression):
    """List PostgreSQL operator families from ``pg_opfamily``.

    Example:
        >>> expr = PostgresOperatorFamilyListQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
        operator_family_name: Optional[str] = None,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            schema: Optional schema name. Defaults to the connection schema.
            operator_family_name: Optional operator family name; it narrows the
                listing.
        """
        super().__init__(dialect, schema=schema)
        self.operator_family_name = operator_family_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_operator_family_list_query_statement"


class PostgresDatabaseSizeFunction(BaseExpression):
    """The ``pg_database_size(name)`` call as a node of its own.

    PostgreSQL spells the on-disk size of a database as this one-argument
    function. The backend declares it rather than reaching for the generic
    ``FunctionCall``, which core reserves for user-defined functions: a
    standard function the server ships is a named node, so an artifact naming
    it resolves to exactly one class instead of a catch-all whose name is
    whatever the caller typed.

    The database name is a bind parameter: PostgreSQL parses the function's
    argument, so the value travels through the driver's placeholder rather
    than being spelled into the SQL text.

    Example:
        >>> expr = PostgresDatabaseSizeFunction(dialect, database_name="app")
        >>> sql, params = expr.to_sql()
        >>> sql
        'PG_DATABASE_SIZE(%s)'
        >>> params
        ('app',)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        database_name: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            database_name: Name of the database whose size is measured.
        """
        super().__init__(dialect)
        self.database_name = database_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_database_size_function"


class PostgresDatabaseSizeQueryStatement(BaseExpression):
    """The one-row query that reports a database's size in bytes.

    Wraps :class:`PostgresDatabaseSizeFunction` in the ``SELECT ... AS
    size_bytes`` shape the status introspector reads, so the introspector asks
    a node for the whole statement instead of assembling the SELECT itself.
    The alias is part of this query's contract: callers read the row by that
    column name.

    Example:
        >>> expr = PostgresDatabaseSizeQueryStatement(dialect, database_name="app")
        >>> sql, params = expr.to_sql()
        >>> sql
        'SELECT PG_DATABASE_SIZE(%s) AS "size_bytes"'
        >>> params
        ('app',)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        database_name: str,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            database_name: Name of the database whose size is measured.
        """
        super().__init__(dialect)
        self.database_name = database_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_database_size_query_statement"


class PostgresRelationSizeFunction(BaseExpression):
    """The ``pg_relation_size(relid)`` call as a node of its own.

    PostgreSQL spells the on-disk size of one relation's heap as this
    one-argument function. It is declared here rather than reached for through
    the generic ``FunctionCall``, which core reserves for user-defined
    functions: a standard function the server ships is a named node, so an
    artifact naming it resolves to exactly one class instead of a catch-all
    whose name is whatever the caller typed.

    The relation is named by its OID -- ``pg_class.oid``, the value every
    catalog read of a table already carries -- and that number is a bind
    parameter. PostgreSQL reads an ``oid`` where a ``regclass`` is expected
    without a cast, which is why the argument binds bare; the point of binding
    it is that a relation identifier is data and can never become part of the
    SQL text.

    Example:
        >>> expr = PostgresRelationSizeFunction(dialect, relid=16384)
        >>> sql, params = expr.to_sql()
        >>> sql
        'PG_RELATION_SIZE(%s)'
        >>> params
        (16384,)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        relid: int,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            relid: OID of the relation whose heap is measured (``pg_class.oid``).
        """
        super().__init__(dialect)
        self.relid = relid

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_relation_size_function"


class PostgresIndexesSizeFunction(BaseExpression):
    """The ``pg_indexes_size(relid)`` call as a node of its own.

    The index side of the same storage split ``pg_relation_size`` measures, and
    a node for the same reason: a standard function the server ships is named
    rather than spelled through the catch-all. Together with
    :class:`PostgresRelationSizeFunction` it answers the two narrow slots of
    :class:`~rhosocial.activerecord.backend.introspection.types.IntrospectionTableInfo`
    -- ``data_size_bytes`` and ``index_size_bytes`` -- which
    ``pg_total_relation_size`` cannot separate.

    The OID binds for the same reason the heap size's does: an identifier is
    data, never text.

    Example:
        >>> expr = PostgresIndexesSizeFunction(dialect, relid=16384)
        >>> sql, params = expr.to_sql()
        >>> sql
        'PG_INDEXES_SIZE(%s)'
        >>> params
        (16384,)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        relid: int,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            relid: OID of the relation whose indexes are measured
                (``pg_class.oid``).
        """
        super().__init__(dialect)
        self.relid = relid

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_indexes_size_function"


class PostgresTableStorageQueryStatement(BaseExpression):
    """The one-row query that reports one relation's storage split.

    Wraps the two calls in the ``SELECT ... AS`` shape the storage slots name,
    so a caller asks a node for the whole statement instead of assembling the
    SELECT itself. The aliases are this query's contract: they are the field
    names the parser fills, so the row shape and the record cannot drift apart.

    The total rides the table listing (``pg_total_relation_size``), and this
    statement answers the split for one relation whose OID the caller already
    holds -- the value every listing row carries. The OID binds once per call:
    an identifier is data, and two bound copies of it cannot become SQL text.

    Example:
        >>> expr = PostgresTableStorageQueryStatement(dialect, relid=16384)
        >>> sql, params = expr.to_sql()
        >>> sql
        'SELECT PG_RELATION_SIZE(%s) AS "data_size_bytes", PG_INDEXES_SIZE(%s) AS "index_size_bytes"'
        >>> params
        (16384, 16384)
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        relid: int,
    ):
        """
        Args:
            dialect: The SQL dialect to use for formatting.
            relid: OID of the relation whose storage is measured
                (``pg_class.oid``).
        """
        super().__init__(dialect)
        self.relid = relid

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_table_storage_query_statement"
