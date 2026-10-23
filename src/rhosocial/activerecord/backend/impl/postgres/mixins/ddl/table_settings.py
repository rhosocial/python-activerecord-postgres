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
from rhosocial.activerecord.backend.expression.objects import Table
from ...expression.ddl.table_settings import LoggingMode

if TYPE_CHECKING:
    from ...expression.ddl.table_settings import (
        PostgresAlterTableSettingsStatement,
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
    def _format_settings_table_ref(
        self, schema: Optional[str], table_name: str
    ) -> str:
        """Render the table a table-level setting is applied to.

        The name is a :class:`Table` schema object rendered by the dialect.
        Both settings (``LOGGED``/``UNLOGGED`` and ``ACCESS METHOD``) go
        through it, so they cannot drift apart.
        """
        sql, _ = self.format_table_object(Table(self, table_name, schema_name=schema))
        return sql

    # ------------------------------------------------------------------ #
    # Statement
    # ------------------------------------------------------------------ #
    def format_alter_table_settings_statement(
        self, expr: "PostgresAlterTableSettingsStatement"
    ) -> Tuple[str, tuple]:
        """Format an ALTER TABLE ... SET table-level settings statement.

        Args:
            expr: :class:`PostgresAlterTableSettingsStatement`.

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

        table_ref = self._format_settings_table_ref(expr.schema, expr.table_name)

        if has_logging:
            if not self.supports_table_logging_switch():
                raise UnsupportedFeatureError(
                    self.name,
                    "ALTER TABLE SET LOGGED/UNLOGGED",
                    suggestion="requires PostgreSQL 9.6+",
                )
            # Compare the *word*, not the member: the artifact carries the word
            # (JSON has no enum) and the constructor keeps whatever it was
            # handed, so an identity test against the member silently answered
            # "not LOGGED" for a round-tripped "LOGGED" and rendered the
            # opposite setting.
            if str(expr.mode) == LoggingMode.LOGGED:
                setting = "SET LOGGED"
            elif str(expr.mode) == LoggingMode.UNLOGGED:
                setting = "SET UNLOGGED"
            else:
                raise ValueError(
                    f"ALTER TABLE settings: unknown mode {expr.mode!r}; "
                    f"expected LOGGED or UNLOGGED"
                )
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