# src/rhosocial/activerecord/backend/impl/postgres/protocols/materialized_view.py
"""PostgreSQL materialized view protocol definitions.

This module defines protocols for PostgreSQL-specific materialized view features
that extend beyond the SQL standard.
"""

from typing import Any, Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import (
        PostgresAlterMaterializedViewExpression,
        PostgresRefreshMaterializedViewExpression,
    )


@runtime_checkable
class PostgresMaterializedViewSupport(Protocol):
    """PostgreSQL materialized view extended features protocol.

    PostgreSQL's materialized view support extends beyond SQL standard, including:
    - CONCURRENTLY refresh (requires unique index)
    - IF NOT EXISTS on CREATE
    - TABLESPACE storage
    - WITH (storage_options) storage parameters
    - ALTER MATERIALIZED VIEW actions

    Version requirements:
    - Basic materialized view: PostgreSQL 9.3+
    - CONCURRENTLY refresh / IF NOT EXISTS: PostgreSQL 9.4+
    - TABLESPACE: PostgreSQL 9.3+

    Note: These features don't require additional plugins, they're part of
    PostgreSQL official distribution.

    Documentation: https://www.postgresql.org/docs/current/sql-creatematerializedview.html
    """

    def supports_materialized_view(self) -> bool:
        """Whether materialized views are supported (PostgreSQL 9.3+)."""
        ...

    def supports_refresh_materialized_view(self) -> bool:
        """Whether REFRESH MATERIALIZED VIEW is supported (PostgreSQL 9.3+)."""
        ...

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """Whether CONCURRENTLY refresh for materialized views is supported.

        PostgreSQL 9.4+ supports the CONCURRENTLY option.
        When using CONCURRENTLY, the materialized view must have at least one UNIQUE index.
        """
        ...

    def supports_materialized_view_if_not_exists(self) -> bool:
        """Whether CREATE MATERIALIZED VIEW IF NOT EXISTS is supported (PG 9.4+)."""
        ...

    def supports_materialized_view_tablespace(self) -> bool:
        """Whether TABLESPACE can be specified for materialized views (PG 9.3+)."""
        ...

    def supports_materialized_view_storage_options(self) -> bool:
        """Whether WITH (storage_parameter) is supported for materialized views (PG 9.3+)."""
        ...

    def supports_alter_materialized_view(self) -> bool:
        """Whether ALTER MATERIALIZED VIEW is supported (PG 9.3+)."""
        ...

    def format_create_materialized_view_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format CREATE MATERIALIZED VIEW statement for PostgreSQL.

        Supports column aliases, schema qualification, IF NOT EXISTS,
        WITH (storage_options), TABLESPACE, and WITH DATA / WITH NO DATA.

        Args:
            expr: CreateMaterializedViewExpression instance

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_drop_materialized_view_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format DROP MATERIALIZED VIEW statement for PostgreSQL.

        Supports schema qualification, IF EXISTS and CASCADE.

        Args:
            expr: DropMaterializedViewExpression instance

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_refresh_materialized_view_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format REFRESH MATERIALIZED VIEW statement, gating CONCURRENTLY on PG 9.4+.

        Args:
            expr: RefreshMaterializedViewExpression instance

        Returns:
            Tuple of (SQL statement, parameters tuple)
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

    def format_alter_materialized_view_statement(
        self, expr: "PostgresAlterMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Format ALTER MATERIALIZED VIEW statement.

        Supports RENAME TO, SET SCHEMA, SET (), RESET () and OWNER TO actions.
        Multiple actions are joined with ``";\\n"``.

        Args:
            expr: PostgresAlterMaterializedViewExpression with the actions to apply

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        ...

    def format_materialized_view_alter_action(self, expr: Any) -> str:
        """Render one ALTER MATERIALIZED VIEW action body.

        Storage parameter names must come from
        :class:`~....storage_parameters.PostgresStorageParameter`.

        Args:
            expr: A ``MaterializedViewAlterAction`` subclass instance.

        Returns:
            The action SQL without the leading ``ALTER MATERIALIZED VIEW <name>``.
        """
        ...
