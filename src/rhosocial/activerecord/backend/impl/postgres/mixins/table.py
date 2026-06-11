# src/rhosocial/activerecord/backend/impl/postgres/mixins/table.py
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
