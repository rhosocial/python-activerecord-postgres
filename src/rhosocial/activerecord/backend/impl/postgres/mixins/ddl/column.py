# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/column.py
"""PostgreSQL ALTER TABLE / ALTER COLUMN formatting extensions.

Covers PostgreSQL-specific qualifiers for ALTER TABLE actions as well as
the ``ALTER COLUMN ... SET DATA TYPE ... USING`` conversion expression,
which the core ``DDLColumnMixin`` does not emit.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
        AlterColumnAction,
    )


class PostgresAlterColumnModifierMixin:
    """PostgreSQL IF [NOT] EXISTS qualifiers for ALTER TABLE actions.

    PostgreSQL takes these qualifiers:
      - ADD [COLUMN] [IF NOT EXISTS] (added in 9.6)
      - DROP [COLUMN] [IF EXISTS] (present in the 9.5 synopsis; every
        version this backend supports has it)
      - DROP CONSTRAINT [IF EXISTS] (same as DROP COLUMN)

    The qualifiers are vendor extensions (not in ISO/IEC 9075-2 §11.10);
    applications opt in by passing ``if_not_exists`` / ``if_exists`` on
    the corresponding ``AddColumnAction`` / ``DropColumnAction`` /
    ``DropTableConstraintAction`` actions. ``None`` (default) means "no
    qualifier", matching the SQL standard form.
    """

    def supports_add_column_if_not_exists(self) -> bool:
        """``ADD COLUMN IF NOT EXISTS`` arrived in PostgreSQL 9.6."""
        return self.version_at_least((9, 6, 0))

    def supports_drop_column_if_exists(self) -> bool:
        """``DROP COLUMN IF EXISTS`` is on every version this backend supports."""
        return True

    def supports_drop_constraint_if_exists(self) -> bool:
        """``DROP CONSTRAINT IF EXISTS`` is on every version this backend supports."""
        return True

    def format_add_column_action(self, expr) -> Tuple[str, tuple]:
        column_sql, column_params = self.format_column_definition(expr.column)
        if getattr(expr, "if_not_exists", None) is True:
            return f"ADD COLUMN IF NOT EXISTS {column_sql}", column_params
        return f"ADD COLUMN {column_sql}", column_params

    def format_drop_column_action(self, expr) -> Tuple[str, tuple]:
        if getattr(expr, "if_exists", None) is True:
            return f"DROP COLUMN IF EXISTS {self.format_identifier(expr.column_name)}", ()
        return f"DROP COLUMN {self.format_identifier(expr.column_name)}", ()

    def format_drop_table_constraint_action(self, expr) -> Tuple[str, tuple]:
        # PostgreSQL spells the drop by name only: the grammar is
        # ``ALTER TABLE ... DROP CONSTRAINT [IF EXISTS] <name>``, with no
        # ``DROP PRIMARY KEY`` / ``DROP UNIQUE (<cols>)`` form.  Core's resolver
        # decides which address the action names the constraint by -- the name,
        # or the declaration's kind -- and refuses an ambiguous address (a name
        # plus a declaration that does not confirm it) before any spelling is
        # attempted.  This formatter has no kind form, so ask for the name and
        # refuse a kind address explicitly.
        address = self._resolve_drop_constraint_address(expr)
        if address != "name":
            raise NotImplementedError(
                f"{self.name} spells DROP CONSTRAINT by name only and does not "
                f"override format_drop_table_constraint_action with a kind form; "
                f"address the constraint by name instead."
            )
        result = "DROP CONSTRAINT"
        if getattr(expr, "if_exists", None) is True:
            result = f"{result} IF EXISTS"
        result = f"{result} {self.format_identifier(expr.constraint_name)}"
        if getattr(expr, "cascade", None):
            result = f"{result} CASCADE"
        return result, ()

    def format_alter_column_action(self, expr: "AlterColumnAction") -> Tuple[str, tuple]:
        """Format an ``ALTER COLUMN`` expr, adding ``USING`` conversion.

        The core ``DDLColumnMixin.format_alter_column_action`` renders the
        standard subclauses (SET/DROP DEFAULT, SET/DROP NOT NULL, and
        ``SET DATA TYPE``). PostgreSQL additionally supports an optional
        ``USING (conversion_expression)`` on ``SET DATA TYPE`` — an
        extension the core dialect does not emit. This override injects it
        from the PostgreSQL ``PostgresAlterColumnAction`` typed ``using`` field.

        Args:
            expr: An ``AlterColumnAction`` expr (core ``ddl_alter`` module).

        Returns:
            Tuple of (SQL string, params tuple).

        Raises:
            ValueError: when ``using`` is supplied for any operation other
                than ``SET DATA TYPE`` (the only one PostgreSQL allows it on).

        """
        sql, params = super().format_alter_column_action(expr)
        using = getattr(expr, "using", None)
        if using is None:
            return sql, params

        op = getattr(expr, "operation", None)
        op_str = op.value if hasattr(op, "value") else str(op)
        if op_str != "SET DATA TYPE":
            raise ValueError("USING conversion is only valid for ALTER COLUMN SET DATA TYPE")

        using_sql, using_params = using.to_sql()
        core, sep, cascade = sql.partition(" CASCADE")
        if sep:
            modified = f"{core} USING ({using_sql}){sep}{cascade}"
        else:
            modified = f"{sql} USING ({using_sql})"
        return modified, tuple(params) + tuple(using_params)

    def format_column(self, expr) -> Tuple[str, tuple]:
        """Format column reference for PostgreSQL.

        PostgreSQL rules for column references:
        - When the table has an alias (used in FROM/JOIN), column references
          must use the alias, not the schema-qualified name. For example,
          when FROM clause has ``public.users AS "users"``, the column
          reference must be ``"users"."id"``, not ``"public"."users"."id"``.
        - When no alias is present but schema is specified, use the
          full three-segment form: ``"schema"."table"."column"``.
        - Otherwise use the standard two-segment or single-segment form.
        """
        name = expr.name
        table = expr.table
        alias = expr.alias
        schema_name = expr.schema_name
        if table:
            # In PostgreSQL, when a table has an alias, column references
            # must use the alias — schema_name is irrelevant in this context.
            if schema_name and not alias:
                col_sql = (
                    f"{self.format_identifier(schema_name, expr.schema_need_quote)}."
                    f"{self.format_identifier(table, expr.table_need_quote)}."
                    f"{self.format_identifier(name, expr.name_need_quote)}"
                )
            else:
                table_sql = self.format_identifier(table, expr.table_need_quote)
                name_sql = self.format_identifier(name, expr.name_need_quote)
                col_sql = f"{table_sql}.{name_sql}"
        else:
            col_sql = self.format_identifier(name, expr.name_need_quote)

        if alias:
            col_sql = f"{col_sql} AS {self.format_identifier(alias, expr.alias_need_quote)}"

        return col_sql, ()
