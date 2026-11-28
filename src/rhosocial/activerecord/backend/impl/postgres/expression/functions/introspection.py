# src/rhosocial/activerecord/backend/impl/postgres/expression/functions/introspection.py
"""Standard-server function nodes used by PostgreSQL introspection.

These are named nodes (rather than the generic ``FunctionCall``) for the size
functions the server ships: an artifact naming one resolves to exactly one
class. The query nodes that wrap them live in ``..introspection.introspection``.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresDatabaseSizeFunction",
    "PostgresRelationSizeFunction",
    "PostgresIndexesSizeFunction",
]


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
