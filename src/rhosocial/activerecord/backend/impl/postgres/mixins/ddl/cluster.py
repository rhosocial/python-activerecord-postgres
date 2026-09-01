# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/cluster.py
"""PostgreSQL CLUSTER DDL implementation.

Implements the CLUSTER statement for the postgres dialect.

Version Requirements:
- CLUSTER table [ USING index ]: PostgreSQL 9.6+
"""

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ...expression.ddl.cluster import PostgresClusterExpression


class PostgresClusterMixin:
    """PostgreSQL CLUSTER implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_cluster(self) -> bool:
        """CLUSTER is supported on all supported PostgreSQL versions."""
        return self.version >= (9, 6, 0)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def format_cluster_table_ref(
        self, schema: Optional[str], table_name: str
    ) -> str:
        """Format ``table_name`` (optionally schema-qualified) as identifier(s)."""
        if schema:
            return (
                f"{self.format_identifier(schema)}."
                f"{self.format_identifier(table_name)}"
            )
        return self.format_identifier(table_name)

    # ------------------------------------------------------------------ #
    # Statement
    # ------------------------------------------------------------------ #
    def format_cluster_statement(
        self, expr: "PostgresClusterExpression"
    ) -> Tuple[str, tuple]:
        """Format a CLUSTER statement (PostgreSQL-specific).

        Args:
            expr: :class:`PostgresClusterExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.6.

        """
        if not self.supports_cluster():
            raise UnsupportedFeatureError(
                self.name,
                "CLUSTER",
                suggestion="requires PostgreSQL 9.6+",
            )

        parts: List[str] = ["CLUSTER"]
        if expr.verbose:
            parts.append("VERBOSE")
        if expr.table_name:
            parts.append(
                self.format_cluster_table_ref(expr.schema, expr.table_name)
            )
            if expr.using_index:
                parts.append("USING")
                parts.append(self.format_identifier(expr.using_index))
        return " ".join(parts), ()