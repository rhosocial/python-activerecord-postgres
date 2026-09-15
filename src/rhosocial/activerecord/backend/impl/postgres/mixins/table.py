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

    def format_create_table_statement(self, expr) -> Tuple[str, tuple]:
        """Render CREATE TABLE for PostgreSQL.

        Handles PostgreSQL-specific CREATE TABLE behavior:
        * Declarative partitioning validation for ``partition`` expressions.
        * UNLOGGED table qualifier.

        Partition validation follows PostgreSQL declarative partitioning rules:
        - RANGE and LIST require PostgreSQL 10+.
        - HASH requires PostgreSQL 11+.
        - MySQL-specific methods (KEY, RANGE COLUMNS) are rejected.
        """
        if getattr(expr, "partition", None) is not None:
            # Validate through the PartitionClause -> format_partition_clause chain.
            expr.partition.to_sql()

        # Delegate to base implementation (TableMixin)
        sql, params = super().format_create_table_statement(expr)

        # Apply UNLOGGED qualifier if requested
        if not (getattr(expr, "dialect_options", None) or {}).get("unlogged_table"):
            return sql, params
        if getattr(expr, "temporary", False):
            return sql, params
        if not self.supports_unlogged_table():
            from rhosocial.activerecord.backend.dialect.exceptions import (
                UnsupportedFeatureError,
            )
            raise UnsupportedFeatureError(
                self.name,
                "CREATE UNLOGGED TABLE",
                suggestion="requires PostgreSQL 9.5+",
            )
        prefix = "CREATE TABLE "
        if sql.startswith(prefix):
            return sql.replace(prefix, "CREATE UNLOGGED TABLE ", 1), params
        temp_prefix = "CREATE TEMPORARY TABLE "
        if sql.startswith(temp_prefix):
            return sql.replace(temp_prefix, "CREATE UNLOGGED TABLE ", 1), params
        return sql, params

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
        from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
        all_params: List[Any] = []
        type_sql, _ = col_def.data_type.to_sql()
        if not re.fullmatch(r"[A-Za-z0-9\s(),\[\]]+", type_sql):
            raise ValueError(
                f"Invalid data type '{type_sql}': "
                "must contain only alphanumeric characters, spaces, parentheses, commas, and brackets."
            )
        col_sql = f"{self.format_identifier(col_def.name)} {type_sql}"
        dialect_opts = col_def.dialect_options or {}
        identity = dialect_opts.get("identity")
        if identity:
            if identity.upper() in ("ALWAYS", "BY DEFAULT"):
                col_sql += f" GENERATED {identity.upper()} AS IDENTITY"
            else:
                raise ValueError(f"Invalid identity option '{identity}': must be 'ALWAYS' or 'BY DEFAULT'")
        auto_identity_added = False
        for constraint in col_def.constraints:
            if not auto_identity_added and getattr(constraint, 'is_auto_increment', False):
                col_sql += " GENERATED BY DEFAULT AS IDENTITY"
                auto_identity_added = True
            suffix, params = self.format_column_constraint(constraint)
            col_sql += suffix
            all_params.extend(params)
        if col_def.comment:
            escaped_comment = SQLDialectBase._escape_sql_string(col_def.comment)
            col_sql += f" COMMENT '{escaped_comment}'"
        return col_sql, tuple(all_params)
