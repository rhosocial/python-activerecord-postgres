# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/column.py
"""PostgreSQL ALTER TABLE IF [NOT] EXISTS qualifier formatting."""

from typing import Tuple


class PostgresAlterColumnModifierMixin:
    """PostgreSQL IF [NOT] EXISTS qualifiers for ALTER TABLE actions.

    PostgreSQL has supported these qualifiers since 9.6:
      - ADD [COLUMN] [IF NOT EXISTS]
      - DROP [COLUMN] [IF EXISTS]
      - DROP CONSTRAINT [IF EXISTS]

    The qualifiers are vendor extensions (not in ISO/IEC 9075-2 §11.10);
    applications opt in by passing ``if_not_exists`` / ``if_exists`` on
    the corresponding ``AddColumn`` / ``DropColumn`` /
    ``DropTableConstraint`` actions. ``None`` (default) means "no
    qualifier", matching the SQL standard form.
    """

    def supports_add_column_if_not_exists(self) -> bool:
        """``ADD COLUMN IF NOT EXISTS`` is supported since PostgreSQL 9.6."""
        return True

    def supports_drop_column_if_exists(self) -> bool:
        """``DROP COLUMN IF EXISTS`` is supported since PostgreSQL 9.6."""
        return True

    def supports_drop_constraint_if_exists(self) -> bool:
        """``DROP CONSTRAINT IF EXISTS`` is supported since PostgreSQL 9.6."""
        return True

    def format_add_column_action(self, action) -> Tuple[str, tuple]:
        column_sql, column_params = self.format_column_definition(action.column)
        if getattr(action, "if_not_exists", None) is True:
            return f"ADD COLUMN IF NOT EXISTS {column_sql}", column_params
        return f"ADD COLUMN {column_sql}", column_params

    def format_drop_column_action(self, action) -> Tuple[str, tuple]:
        if getattr(action, "if_exists", None) is True:
            return f"DROP COLUMN IF EXISTS {self.format_identifier(action.column_name)}", ()
        return f"DROP COLUMN {self.format_identifier(action.column_name)}", ()

    def format_drop_table_constraint_action(self, action) -> Tuple[str, tuple]:
        result = "DROP CONSTRAINT"
        if getattr(action, "if_exists", None) is True:
            result = f"{result} IF EXISTS"
        result = f"{result} {self.format_identifier(action.constraint_name)}"
        if getattr(action, "cascade", None):
            result = f"{result} CASCADE"
        return result, ()
