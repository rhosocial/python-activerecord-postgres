# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/rls_config.py
"""PostgreSQL Row-Level Security table configuration protocol definition.

This module contains the :class:`PostgresRlsConfigSupport` protocol which
defines the interface for PostgreSQL's native ALTER TABLE ... ROW LEVEL
SECURITY configuration commands.

ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL SECURITY is a PostgreSQL extension —
not SQL standard.
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.rls_config import (
        PostgresAlterTableRlsExpression,
        PostgresForceRlsExpression,
    )


@runtime_checkable
class PostgresRlsConfigSupport(Protocol):
    """PostgreSQL Row-Level Security table configuration protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - ALTER TABLE: https://www.postgresql.org/docs/current/sql-altertable.html

    Version Requirements:
    - ENABLE / DISABLE ROW LEVEL SECURITY: PostgreSQL 9.5+
    - ENABLE ALWAYS ROW LEVEL SECURITY: PostgreSQL 9.5+
    - FORCE / NO FORCE ROW LEVEL SECURITY: PostgreSQL 9.5+
    """

    def supports_rls_enable_disable(self) -> bool:
        """Whether ``ENABLE/DISABLE ROW LEVEL SECURITY`` is supported (9.5+)."""
        ...

    def supports_rls_force(self) -> bool:
        """Whether ``FORCE/NO FORCE ROW LEVEL SECURITY`` is supported (9.5+)."""
        ...

    def supports_rls_enable_always(self) -> bool:
        """Whether ``ENABLE ALWAYS ROW LEVEL SECURITY`` is supported (9.5+)."""
        ...

    def format_alter_table_rls_statement(
        self, expr: "PostgresAlterTableRlsExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY``.

        Args:
            expr: ``PostgresAlterTableRlsExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.5.
            ValueError: ``always`` combined with ``DISABLE``.
        """
        ...

    def format_force_rls_statement(
        self, expr: "PostgresForceRlsExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... FORCE/NO FORCE ROW LEVEL SECURITY``.

        Args:
            expr: ``PostgresForceRlsExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: dialect predates PostgreSQL 9.5.
        """
        ...