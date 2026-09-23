# src/rhosocial/activerecord/backend/impl/postgres/mixins/table.py
import re
from typing import Any, List, Tuple

class PostgresTableMixin:
    """PostgreSQL table extended features implementation."""

    def supports_if_not_exists_table(self) -> bool:
        """CREATE TABLE IF NOT EXISTS is supported since PostgreSQL 9.5+."""
        return self.version >= (9, 5, 0)

    def supports_if_exists_table(self) -> bool:
        """DROP TABLE IF EXISTS is supported in all versions."""
        return True

    def supports_temporary_table(self) -> bool:
        """TEMPORARY tables are supported in all versions."""
        return True

    def supports_unlogged_table(self) -> bool:
        """CREATE UNLOGGED TABLE is supported since PostgreSQL 9.5.

        UNLOGGED tables write no WAL; they are faster for staging data but are
        not crash-safe and are truncated on crash recovery. PostgreSQL 14 added
        the ``UNLOGGED`` / ``LOGGED`` table as a distribution. The bare
        ``UNLOGGED`` qualifier on CREATE TABLE exists since 9.5, matching the
        §0 baseline.
        """
        return self.version >= (9, 5, 0)

    def supports_table_inheritance(self) -> bool:
        """PostgreSQL supports table inheritance."""
        return True

    def supports_table_tablespace(self) -> bool:
        """TABLESPACE specification is supported in all versions."""
        return True

    def supports_create_table_like(self) -> bool:
        """PostgreSQL supports CREATE TABLE (LIKE ...) with INCLUDING/EXCLUDING options."""
        return True

    def format_create_table_options(self, expr) -> Tuple[str, tuple]:
        """Format the CREATE header modifiers for PostgreSQL.

        Accepts both the generic ``CreateTableOptions`` (renders ``OR REPLACE``)
        and the PostgreSQL ``PostgresCreateTableOptions`` (adds ``UNLOGGED``).
        """
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.table_options import (
            PostgresCreateTableOptions,
        )

        base_sql, params = super().format_create_table_options(expr)
        parts = [base_sql] if base_sql else []
        if isinstance(expr, PostgresCreateTableOptions) and expr.unlogged:
            if not self.supports_unlogged_table():
                raise UnsupportedFeatureError(self.name, "CREATE UNLOGGED TABLE")
            parts.append("UNLOGGED")
        return " ".join(parts), params

    def format_create_table_statement(self, expr) -> Tuple[str, tuple]:
        """Render CREATE TABLE for PostgreSQL.

        Handles PostgreSQL-specific CREATE TABLE behavior:
        * Declarative partitioning validation for ``partition`` expressions.

        ``UNLOGGED`` is carried by :class:`CreateTableOptions` (``unlogged=True``)
        and rendered by the generic core renderer, gated on
        :meth:`supports_unlogged_table`.

        Partition validation follows PostgreSQL declarative partitioning rules:
        - RANGE and LIST require PostgreSQL 10+.
        - HASH requires PostgreSQL 11+.
        - MySQL-specific methods (KEY, RANGE COLUMNS) are rejected.
        """
        if getattr(expr, "partition", None) is not None:
            # Validate through the PartitionClause -> format_partition_clause chain.
            expr.partition.to_sql()

        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.table_options import (
            PostgresCreateTableOptions,
        )
        table_options = getattr(expr, "table_options", None)
        if (
            isinstance(table_options, PostgresCreateTableOptions)
            and table_options.unlogged
            and getattr(expr, "temporary", False)
        ):
            from rhosocial.activerecord.backend.dialect.exceptions import (
                UnsupportedFeatureError,
            )
            raise UnsupportedFeatureError(
                self.name,
                "CREATE UNLOGGED TEMPORARY TABLE",
                suggestion="UNLOGGED and TEMPORARY are mutually exclusive in PostgreSQL",
            )

        # Delegate to base implementation (TableMixin)
        return super().format_create_table_statement(expr)

    def format_create_table_like_statement(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TABLE (LIKE ...) for PostgreSQL.

        PostgreSQL uses a column-list clause rather than the generic
        ``LIKE <source>`` form::

            CREATE [TEMPORARY] TABLE [IF NOT EXISTS] <target>
                (LIKE <source> [INCLUDING <feature>]... [EXCLUDING <feature>]...)

        ``expr.like_options`` accepts:

        * ``dict`` with ``"including"`` / ``"excluding"`` lists;
        * ``list`` of feature strings (treated as ``INCLUDING``);
        * ``list`` of ``(action, feature)`` tuples (``ACTION FEATURE``).
        """
        from rhosocial.activerecord.backend.dialect.exceptions import (
            UnsupportedFeatureError,
        )

        if not self.supports_create_table_like():
            raise UnsupportedFeatureError(self.name, "CREATE TABLE ... (LIKE ...)")

        parts = ["CREATE"]
        if expr.temporary:
            parts.append("TEMPORARY")
        parts.append("TABLE")
        if expr.if_not_exists:
            parts.append("IF NOT EXISTS")

        table_sql, table_params = expr.table.to_sql()
        parts.append(table_sql)

        source_sql, source_params = expr.like_table.to_sql()

        like_parts = [f"LIKE {source_sql}"]

        like_options = expr.like_options or []
        if isinstance(like_options, dict):
            including = like_options.get("including", [])
            excluding = like_options.get("excluding", [])
            for option in including:
                like_parts.append(f"INCLUDING {option.upper()}")
            for option in excluding:
                like_parts.append(f"EXCLUDING {option.upper()}")
        elif isinstance(like_options, list):
            for option in like_options:
                if isinstance(option, tuple):
                    action, feature = option
                    like_parts.append(f"{action.upper()} {feature.upper()}")
                else:
                    like_parts.append(f"INCLUDING {option.upper()}")

        parts.append(f"({' '.join(like_parts)})")

        return " ".join(parts), tuple(table_params) + tuple(source_params)

    def format_column_definition(self, col_def) -> Tuple[str, tuple]:
        """Format a single column definition with PostgreSQL-specific syntax.

        Accepts both the generic ``ColumnDefinition`` and the PostgreSQL
        ``PostgresColumnDefinition``; the latter's PostgreSQL-only attributes
        (``compression`` / ``storage`` / ``statistics``) are rendered here.
        """
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.column import (
            PostgresColumnDefinition,
        )

        all_params: List[Any] = []
        type_sql, _ = col_def.data_type.to_sql()
        if not re.fullmatch(r"[A-Za-z0-9\s(),\[\]]+", type_sql):
            raise ValueError(
                f"Invalid data type '{type_sql}': "
                "must contain only alphanumeric characters, spaces, parentheses, commas, and brackets."
            )
        col_sql = f"{self.format_identifier(col_def.name)} {type_sql}"

        attr_sql, attr_params = self.format_column_attributes(col_def)
        col_sql += attr_sql
        all_params.extend(attr_params)
        auto_identity_added = False
        for constraint in col_def.constraints:
            if not auto_identity_added and getattr(constraint, 'is_auto_increment', False):
                col_sql += " GENERATED BY DEFAULT AS IDENTITY"
                auto_identity_added = True
            suffix, params = self.format_column_constraint(constraint)
            col_sql += suffix
            all_params.extend(params)
        if col_def.comment is not None:
            # PostgreSQL has no inline COMMENT syntax (the de-facto
            # vendor form is the standalone COMMENT ON statement); a comment
            # on a column definition is never silently dropped.
            from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
            raise UnsupportedFeatureError(
                self.name, "COLUMN COMMENT",
                "PostgreSQL has no inline column comment; use a standalone "
                "COMMENT ON COLUMN statement.",
            )
        if col_def.generated_expression is not None:
            gen_sql, gen_params = col_def.generated_expression.to_sql()
            col_sql += gen_sql
            all_params.extend(gen_params)
        if isinstance(col_def, PostgresColumnDefinition):
            if col_def.compression:
                col_sql += f" COMPRESSION {col_def.compression}"
            if col_def.storage is not None:
                col_sql += f" STORAGE {col_def.storage.value}"
            if col_def.statistics is not None:
                col_sql += f" STATISTICS {col_def.statistics}"
        return col_sql, tuple(all_params)
