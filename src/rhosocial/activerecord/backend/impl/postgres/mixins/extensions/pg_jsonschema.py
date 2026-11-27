# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_jsonschema.py
"""Capability mixin for the pg_jsonschema extension."""


class PostgresPgJsonSchemaMixin:
    """JSON Schema validation capability."""

    def supports_pg_jsonschema(self) -> bool:
        """Whether ``pg_jsonschema`` validation functions are available."""
        return self.check_extension_feature("pg_jsonschema", "validation")
