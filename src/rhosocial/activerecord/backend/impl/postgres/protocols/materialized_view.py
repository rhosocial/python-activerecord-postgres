# src/rhosocial/activerecord/backend/impl/postgres/protocols/materialized_view.py
"""PostgreSQL materialized view protocol definitions.

This module defines protocols for PostgreSQL-specific materialized view features
that extend beyond the SQL standard.
"""

from typing import Any, Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import PostgresRefreshMaterializedViewExpression


@runtime_checkable
class PostgresMaterializedViewSupport(Protocol):
    """PostgreSQL materialized view extended features protocol.

    PostgreSQL's materialized view support extends beyond SQL standard, including:
    - CONCURRENTLY refresh (requires unique index)
    - TABLESPACE storage
    - WITH (storage_options) storage parameters

    Version requirements:
    - Basic materialized view: PostgreSQL 9.3+
    - CONCURRENTLY refresh: PostgreSQL 9.4+
    - TABLESPACE: PostgreSQL 9.3+

    Note: These features don't require additional plugins, they're part of
    PostgreSQL official distribution.

    Documentation: https://www.postgresql.org/docs/current/sql-creatematerializedview.html
    """

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """Whether CONCURRENTLY refresh for materialized views is supported.

        PostgreSQL 9.4+ supports the CONCURRENTLY option.
        When using CONCURRENTLY, the materialized view must have at least one UNIQUE index.
        """
        ...

    def format_refresh_materialized_view_pg_statement(
        self, expr: "PostgresRefreshMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Format REFRESH MATERIALIZED VIEW statement with PG-specific options.

        Args:
            expr: PostgresRefreshMaterializedViewExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_create_materialized_view_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format CREATE MATERIALIZED VIEW statement for PostgreSQL.

        Supports column aliases, TABLESPACE, WITH (storage_options),
        and WITH DATA / WITH NO DATA.

        Args:
            expr: CreateMaterializedViewExpression instance

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...
