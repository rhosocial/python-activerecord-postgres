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

    def supports_table_like_syntax(self) -> bool:
        """PostgreSQL supports CREATE TABLE (LIKE ...) with INCLUDING/EXCLUDING options."""
        return True

    def format_create_table_statement(self, expr) -> Tuple[str, tuple]:
        """Render CREATE TABLE, injecting the UNLOGGED qualifier.

        The core ``TableMixin.format_create_table_statement`` does not know
        about PostgreSQL's ``UNLOGGED`` table classes (WAL-avoiding tables).
        Callers opt in via ``dialect_options={"unlogged": True}`` on the
        ``CreateTableExpression``; the qualifier is version-gated at 9.5+.
        ``UNLOGGED`` and ``TEMPORARY`` are mutually exclusive qualifiers in
        PostgreSQL's grammar; TEMPORARY wins when both are requested.
        """
        sql, params = super().format_create_table_statement(expr)
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

    def format_create_table_like(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TABLE (LIKE ...) statement for PostgreSQL.

        Delegates to format_create_table_statement which already handles
        the 'like_table' key in dialect_options.
        """
        return self.format_create_table_statement(expr)

    def format_column_definition(self, col_def) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.dialect.base import SQLDialectBase
        all_params: List[Any] = []
        type_sql, _ = col_def.data_type.to_sql(self)
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
        for constraint in col_def.constraints:
            suffix, params = self.format_column_constraint(constraint)
            col_sql += suffix
            all_params.extend(params)
        if col_def.comment:
            escaped_comment = SQLDialectBase._escape_sql_string(col_def.comment)
            col_sql += f" COMMENT '{escaped_comment}'"
        return col_sql, tuple(all_params)
