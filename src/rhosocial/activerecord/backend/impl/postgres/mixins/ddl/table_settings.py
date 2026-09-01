# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/table_settings.py
"""PostgreSQL ALTER TABLE table-level settings DDL implementation.

Implements ``ALTER TABLE ... SET LOGGED/UNLOGGED`` and
``ALTER TABLE ... SET ACCESS METHOD ...`` for the postgres dialect.

Version Requirements:
- SET LOGGED / SET UNLOGGED: PostgreSQL 9.6+ (exists since 9.5)
- SET ACCESS METHOD: PostgreSQL 15+
"""

from typing import Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from ...expression.ddl.table_settings import LoggingMode

if TYPE_CHECKING:
    from ...expression.ddl.table_settings import (
        PostgresAlterTableSettingsExpression,
    )


class PostgresAlterTableSettingsMixin:
    """PostgreSQL ALTER TABLE ... SET table-level settings implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_table_logging_switch(self) -> bool:
        """``SET LOGGED/UNLOGGED`` requires PostgreSQL 9.6+ (since 9.5)."""
        return self.version >= (9, 6, 0)

    def supports_table_set_access_method(self) -> bool:
        """``SET ACCESS METHOD`` requires PostgreSQL 15+."""
        return self.version >= (15, 0, 0)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def format_settings_table_ref(
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
    def format_alter_table_settings_statement(
        self, expr: "PostgresAlterTableSettingsExpression"
    ) -> Tuple[str, tuple]:
        """Format an ALTER TABLE ... SET table-level settings statement.

        Args:
            expr: :class:`PostgresAlterTableSettingsExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            ValueError: if neither ``mode`` nor ``access_method`` is given,
                or if both are given.
            UnsupportedFeatureError: when a version-gated setting is requested
                on an insufficient PostgreSQL version.

        """
        has_logging = expr.mode is not None
        has_access = expr.access_method is not None
        if not has_logging and not has_access:
            raise ValueError(
                "ALTER TABLE settings requires exactly one of "
                "mode (LOGGED/UNLOGGED) or access_method"
            )
        if has_logging and has_access:
            raise ValueError(
                "ALTER TABLE settings: mode and access_method are mutually "
                "exclusive"
            )

        table_ref = self.format_settings_table_ref(expr.schema, expr.table_name)

        if has_logging:
            if not self.supports_table_logging_switch():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER TABLE SET LOGGED/UNLOGGED",
                    suggestion="requires PostgreSQL 9.6+",
                )
            if expr.mode is LoggingMode.LOGGED:
                setting = "SET LOGGED"
            else:
                setting = "SET UNLOGGED"
        else:
            if not self.supports_table_set_access_method():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER TABLE SET ACCESS METHOD",
                    suggestion="requires PostgreSQL 15+",
                )
            setting = (
                f"SET ACCESS METHOD {self.format_identifier(expr.access_method)}"
            )

        return f"ALTER TABLE {table_ref} {setting}", ()