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

    def supports_table_partitioning(self) -> bool:
        """Table partitioning is supported since PostgreSQL 10."""
        return self.version >= (10, 0, 0)

    def supports_table_tablespace(self) -> bool:
        """TABLESPACE specification is supported in all versions."""
        return True
