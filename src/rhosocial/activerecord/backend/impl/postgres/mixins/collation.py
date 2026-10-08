# src/rhosocial/activerecord/backend/impl/postgres/mixins/collation.py
"""PostgreSQL collation feature support implementation."""

from rhosocial.activerecord.backend.expression.objects import SchemaObject


class PostgresCollationMixin:
    """PostgreSQL collation override implementation.

    All features are native, using version number for detection.
    """

    def supports_collate_expression(self) -> bool:
        return True

    def validate_collation_name(self, expr) -> str:
        """Validate PostgreSQL collation names and return their SQL representation."""
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from ..collation import validate_postgres_collation_name

        schema = expr.collation_options.get("schema")
        unsupported = set(expr.collation_options) - {"schema"}
        if unsupported:
            options = ", ".join(sorted(unsupported))
            raise UnsupportedFeatureError(self.name, f"COLLATE options: {options}")
        validate_postgres_collation_name(expr.collation_name, getattr(self, "version", None))
        return self._format_schema_object_name(
            SchemaObject(self, expr.collation_name, schema_name=schema)
        )
