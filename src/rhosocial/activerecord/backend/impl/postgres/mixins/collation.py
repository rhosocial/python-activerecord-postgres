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
        """Validate PostgreSQL collation names and return their SQL representation.

        The only option any backend reads is the schema qualifier, and core's
        ``CollateFunction`` declares it as ``schema`` -- there is no options bag
        to inspect. Reading one that never existed is what made every COLLATE
        render raise ``AttributeError``.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from ..collation import validate_postgres_collation_name

        schema = expr.schema
        if schema is not None and not isinstance(schema, str):
            raise UnsupportedFeatureError(
                self.name, f"COLLATE schema: {type(schema).__name__}"
            )
        validate_postgres_collation_name(expr.collation_name, self.version)
        return self._format_schema_object_name(
            SchemaObject(self, expr.collation_name, schema_name=schema)
        )
