# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/rls_config.py
"""PostgreSQL Row-Level Security table configuration DDL implementation.

Implements ``ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY`` and
``ALTER TABLE ... FORCE / NO FORCE ROW LEVEL SECURITY`` for the postgres
dialect. Together with the ``PostgresPolicyMixin`` (CREATE/ALTER/DROP
POLICY) this forms the complete Row-Level Security ecosystem.

Version Requirements:
- All four forms: PostgreSQL 9.5+
"""

from typing import Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ...expression.ddl.rls_config import (
        PostgresAlterTableRlsExpression,
        PostgresForceRlsExpression,
    )


class PostgresRlsConfigMixin:
    """PostgreSQL Row-Level Security table configuration implementation."""

    # ------------------------------------------------------------------ #
    # Capability switches
    # ------------------------------------------------------------------ #
    def supports_rls_enable_disable(self) -> bool:
        """``ENABLE/DISABLE ROW LEVEL SECURITY`` requires PostgreSQL 9.5+."""
        return self.version >= (9, 5, 0)

    def supports_rls_force(self) -> bool:
        """``FORCE / NO FORCE ROW LEVEL SECURITY`` requires PostgreSQL 9.5+."""
        return self.version >= (9, 5, 0)

    def supports_rls_enable_always(self) -> bool:
        """``ENABLE ALWAYS ROW LEVEL SECURITY`` requires PostgreSQL 9.5+."""
        return self.version >= (9, 5, 0)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def format_rls_table_ref(
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
    # ENABLE / DISABLE
    # ------------------------------------------------------------------ #
    def format_alter_table_rls_statement(
        self, expr: "PostgresAlterTableRlsExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY``.

        Args:
            expr: :class:`PostgresAlterTableRlsExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.5.
            ValueError: if ``always`` is combined with ``DISABLE``.

        """
        if not self.supports_rls_enable_disable():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY",
                suggestion="requires PostgreSQL 9.5+",
            )
        if expr.always and expr.mode.value == "DISABLE":
            raise ValueError(
                "ENABLE ALWAYS is only valid with ENABLE ROW LEVEL SECURITY"
            )

        mode = "ENABLE ALWAYS" if expr.always else expr.mode.value
        parts = [
            "ALTER TABLE",
            self.format_rls_table_ref(expr.schema, expr.table_name),
            f"{mode} ROW LEVEL SECURITY",
        ]
        return " ".join(parts), ()

    # ------------------------------------------------------------------ #
    # FORCE / NO FORCE
    # ------------------------------------------------------------------ #
    def format_force_rls_statement(
        self, expr: "PostgresForceRlsExpression"
    ) -> Tuple[str, tuple]:
        """Format ``ALTER TABLE ... FORCE/NO FORCE ROW LEVEL SECURITY``.

        Args:
            expr: :class:`PostgresForceRlsExpression`.

        Returns:
            Tuple of (SQL string, empty params tuple).

        Raises:
            UnsupportedFeatureError: on a dialect predating PostgreSQL 9.5.

        """
        if not self.supports_rls_force():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER TABLE ... FORCE/NO FORCE ROW LEVEL SECURITY",
                suggestion="requires PostgreSQL 9.5+",
            )
        mode = "FORCE ROW LEVEL SECURITY" if expr.force else \
            "NO FORCE ROW LEVEL SECURITY"
        parts = [
            "ALTER TABLE",
            self.format_rls_table_ref(expr.schema, expr.table_name),
            mode,
        ]
        return " ".join(parts), ()