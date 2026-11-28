# src/rhosocial/activerecord/backend/impl/postgres/expression/introspection/introspection.py
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

from rhosocial.activerecord.backend.expression.bases.bases import BaseExpression
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
    "PostgresTableStateQueryStatement",
    "PostgresIndexStateQueryStatement",
    "PostgresConstraintStateQueryStatement",
    "PostgresSequenceStateQueryStatement",
    "PostgresSchemaListQueryStatement",
    "PostgresRoutineListQueryStatement",
    "PostgresTypeListQueryStatement",
    "PostgresOperatorListQueryStatement",
    "PostgresOperatorClassListQueryStatement",
    "PostgresOperatorFamilyListQueryStatement",
    "PostgresDatabaseSizeQueryStatement",
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


class PostgresTableStateQueryStatement(IntrospectionExpression):
    """Read one relation's neutral state: kind, storage options and comment.

    The listing's projection answers the type and the size; this one asks the
    facts a caller needs to reproduce the object: ``relkind`` names the kind,
    ``relpersistence`` separates an unlogged or temporary table from a regular
    one, ``reloptions`` carries the storage parameters the declaration set,
    ``reltablespace`` names the tablespace, and ``relam`` names the access
    method (PostgreSQL 12+). One row answers all of them, so the state is one
    read rather than four.

    Example:
        >>> expr = PostgresTableStateQueryStatement(dialect, "orders", schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect, schema=schema)
        if not table_name:
            raise ValueError("table_name must not be empty")
        self.table_name = table_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_table_state_query_statement"


class PostgresIndexStateQueryStatement(IntrospectionExpression):
    """Read one table's indexes, including whether each is still usable.

    ``indisvalid`` is the flag that answers usability: a ``CREATE INDEX
    CONCURRENTLY`` that failed leaves the index present but invalid, and a
    caller deciding whether the index can serve a query must see that. The
    backing constraint is joined from ``pg_constraint.conindid`` so a caller
    need not correlate the two catalogues itself.

    Example:
        >>> expr = PostgresIndexStateQueryStatement(dialect, "orders", schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect, schema=schema)
        if not table_name:
            raise ValueError("table_name must not be empty")
        self.table_name = table_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_index_state_query_statement"


class PostgresConstraintStateQueryStatement(IntrospectionExpression):
    """Read one table's constraints, including their validation state.

    ``convalidated`` is what separates a constraint the engine has checked
    against every row from one declared ``NOT VALID``: both enforce new rows,
    only one is a promise about the existing ones. ``pg_get_constraintdef`` is
    the engine's own spelling of the definition, kept in ``raw`` rather than
    flattened into columns no other engine spells the same way.

    Example:
        >>> expr = PostgresConstraintStateQueryStatement(dialect, "orders", schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: str,
        schema: Optional[str] = None,
    ):
        super().__init__(dialect, schema=schema)
        if not table_name:
            raise ValueError("table_name must not be empty")
        self.table_name = table_name

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_constraint_state_query_statement"


class PostgresSequenceStateQueryStatement(IntrospectionExpression):
    """Read one schema's sequences, with the column that owns each.

    Ownership lives in ``pg_depend`` (``deptype`` ``'a'`` for a ``serial``
    sequence, ``'i'`` for an identity one), not in ``pg_class``; the join names
    the owning table and column as ``schema.table.column`` so the answer does
    not depend on the caller already knowing the relationship.

    ``pg_sequence`` is PostgreSQL 10+; on 9.x the start and increment slots are
    reported absent rather than guessed.

    Example:
        >>> expr = PostgresSequenceStateQueryStatement(dialect, schema="public")
        >>> sql, params = expr.to_sql()
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        schema: Optional[str] = None,
    ):
        super().__init__(dialect, schema=schema)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_sequence_state_query_statement"


class PostgresSchemaListQueryStatement(IntrospectionExpression):
    """List the schema names this database exposes.

    The system namespaces and the session's temporary namespace are omitted:
    they exist on every server and are not schemas a caller created or can
    use. A caller asking whether a schema exists still gets an answer from
    :meth:`item_exists`.

    Example:
        >>> expr = PostgresSchemaListQueryStatement(dialect)
        >>> sql, params = expr.to_sql()
    """

    def __init__(self, dialect: "SQLDialectBase"):
        super().__init__(dialect)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_schema_list_query_statement"


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
