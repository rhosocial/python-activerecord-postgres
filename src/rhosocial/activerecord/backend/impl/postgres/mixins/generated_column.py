# src/rhosocial/activerecord/backend/impl/postgres/mixins/generated_column.py
"""PostgreSQL generated column feature support mixin."""


class PostgresGeneratedColumnMixin:
    """PostgreSQL generated column feature support."""

    def supports_auto_increment(self) -> bool:
        """Whether AUTO_INCREMENT/IDENTITY column attributes are supported.

        PostgreSQL supports SERIAL and GENERATED ... AS IDENTITY.
        """
        return True

    def supports_stored_generated_columns(self) -> bool:
        """Whether STORED generated columns are supported.

        PostgreSQL generated columns are always STORED.
        """
        return self.supports_generated_columns()

    def supports_virtual_generated_columns(self) -> bool:
        """Whether VIRTUAL generated columns are supported.

        PostgreSQL has no VIRTUAL generated columns.
        """
        return False
