# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/column.py
"""PostgreSQL ALTER TABLE / ALTER COLUMN formatting extensions.

Covers PostgreSQL-specific qualifiers for ALTER TABLE actions as well as
the ``ALTER COLUMN ... SET DATA TYPE ... USING`` conversion expression,
which the core ``DDLColumnMixin`` does not emit.
"""

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

    def format_alter_column_action(self, action) -> Tuple[str, tuple]:
        """Format an ``ALTER COLUMN`` action, adding ``USING`` conversion.

        The core ``DDLColumnMixin.format_alter_column_action`` renders the
        standard subclauses (SET/DROP DEFAULT, SET/DROP NOT NULL, and
        ``SET DATA TYPE``). PostgreSQL additionally supports an optional
        ``USING (conversion_expression)`` on ``SET DATA TYPE`` — an
        extension the core dialect does not emit. This override injects it
        from ``action.dialect_options["using"]``.

        Args:
            action: An ``AlterColumn`` action (core ``ddl_alter`` module).

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            ValueError: when ``using`` is supplied for any operation other
                than ``SET DATA TYPE`` (the only one PostgreSQL allows it on).

        """
        sql, params = super().format_alter_column_action(action)
        dialect_options = getattr(action, "dialect_options", None) or {}
        using = dialect_options.get("using")
        if using is None:
            return sql, params

        op = getattr(action, "operation", None)
        op_str = op.value if hasattr(op, "value") else str(op)
        if op_str != "SET DATA TYPE":
            raise ValueError(
                "USING conversion is only valid for ALTER COLUMN SET DATA TYPE"
            )

        using_sql, using_params = using.to_sql()
        core, sep, cascade = sql.partition(" CASCADE")
        if sep:
            modified = f"{core} USING ({using_sql}){sep}{cascade}"
        else:
            modified = f"{sql} USING ({using_sql})"
        return modified, tuple(params) + tuple(using_params)
