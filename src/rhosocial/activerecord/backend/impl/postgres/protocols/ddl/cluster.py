# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/cluster.py
"""PostgreSQL CLUSTER protocol definition.

This module contains the :class:`PostgresClusterSupport` protocol which
defines the interface for PostgreSQL's native CLUSTER command.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.cluster import PostgresClusterExpression


@runtime_checkable
class PostgresClusterSupport(Protocol):
    """PostgreSQL CLUSTER protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - CLUSTER: https://www.postgresql.org/docs/current/sql-cluster.html

    Version Requirements:
    - CLUSTER table [ USING index ]: PostgreSQL 9.6+
    """

    def supports_cluster(self) -> bool:
        """Whether CLUSTER is supported (9.6+)."""
        ...

    def format_cluster_statement(
        self, expr: "PostgresClusterExpression"
    ) -> Tuple[str, tuple]:
        """Format a ``CLUSTER`` statement.

        Args:
            expr: ``PostgresClusterExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.6.
        """
        ...