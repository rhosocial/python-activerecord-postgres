# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/table_settings.py
"""PostgreSQL ALTER TABLE table-level settings protocol definition.

This module contains the :class:`PostgresAlterTableSettingsSupport` protocol
which defines the interface for PostgreSQL's native ALTER TABLE ... SET
table-level settings (LOGGED/UNLOGGED, ACCESS METHOD).
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl.table_settings import (
        PostgresAlterTableSettingsExpression,
    )


@runtime_checkable
class PostgresAlterTableSettingsSupport(Protocol):
    """PostgreSQL ALTER TABLE ... SET table-level settings protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - ALTER TABLE: https://www.postgresql.org/docs/current/sql-altertable.html

    Version Requirements:
    - SET LOGGED / SET UNLOGGED: PostgreSQL 9.6+ (exists since 9.5)
    - SET ACCESS METHOD: PostgreSQL 15+
    """

    def supports_table_logging_switch(self) -> bool:
        """Whether ``SET LOGGED/UNLOGGED`` is supported (9.6+)."""
        ...

    def supports_table_set_access_method(self) -> bool:
        """Whether ``SET ACCESS METHOD`` is supported (15+)."""
        ...

    def format_alter_table_settings_statement(
        self, expr: "PostgresAlterTableSettingsExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... SET`` table-level settings.

        Args:
            expr: ``PostgresAlterTableSettingsExpression``.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            ValueError: no setting or conflicting settings supplied.
            UnsupportedFeatureError: version-gated setting on an
                insufficient PostgreSQL version.
        """
        ...