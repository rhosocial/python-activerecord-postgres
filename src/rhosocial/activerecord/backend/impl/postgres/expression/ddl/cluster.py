# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/cluster.py
"""
PostgreSQL DDL expressions: CLUSTER.

PostgreSQL Documentation:
- CLUSTER: https://www.postgresql.org/docs/current/sql-cluster.html

Version Requirements:
- CLUSTER table [ USING index ]: PostgreSQL 9.6+ (all supported versions)
- VERBOSE option: PostgreSQL 9.6+ (all supported versions)
"""

from typing import Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresClusterExpression"]


class PostgresClusterExpression(BaseExpression):
    """PostgreSQL CLUSTER statement expression.

    Physically reorders a table (or all tables) to match the order of the
    index used for clustering. Commonly run inside a transaction after a
    bulk load.

    Attributes:
        table: Name of the table to cluster. If None, clusters all
            tables that have previously been clustered (``CLUSTER`` alone).
        schema: Optional schema for the table.
        using_index: Optional index name for ``CLUSTER table USING index``.
        verbose: When True, report the table being clustered (``VERBOSE``).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> expr = PostgresClusterExpression(
        ...     dialect, table="orders", using_index="orders_pkey"
        ... )
        >>> sql, params = expr.to_sql()  # doctest: +SKIP

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table: Optional[str] = None,
        schema: Optional[str] = None,
        using_index: Optional[str] = None,
        verbose: bool = False,
    ):
        super().__init__(dialect)
        self.table = table
        self.schema = schema
        self.using_index = using_index
        self.verbose = verbose

    def to_sql(self) -> "Tuple[str, tuple]":
        """Generate the CLUSTER statement.

        Returns:
            Tuple of (SQL string, empty params tuple).

        """
        return self.dialect.format_cluster_statement(self)