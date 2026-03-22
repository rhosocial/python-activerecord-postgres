# src/rhosocial/activerecord/backend/impl/postgres/mixins/table.py
class PostgresTableMixin:
    """PostgreSQL table extended features implementation."""

    def supports_table_inheritance(self) -> bool:
        """PostgreSQL supports table inheritance."""
        return True
