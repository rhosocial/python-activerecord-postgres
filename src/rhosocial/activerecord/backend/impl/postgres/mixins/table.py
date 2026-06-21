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

    def supports_table_inheritance(self) -> bool:
        """PostgreSQL supports table inheritance."""
        return True

    def supports_table_tablespace(self) -> bool:
        """TABLESPACE specification is supported in all versions."""
        return True

    def supports_table_like_syntax(self) -> bool:
        """PostgreSQL supports CREATE TABLE (LIKE ...) with INCLUDING/EXCLUDING options."""
        return True

    def format_create_table_like(self, expr) -> tuple:
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
