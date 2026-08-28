# src/rhosocial/activerecord/backend/impl/postgres/mixins/schema.py
"""PostgreSQL schema feature support implementation."""


class PostgresSchemaMixin:
    """PostgreSQL schema override implementation.

    All features are native, using version number for detection.
    """

    def supports_schema(self) -> bool:
        """PostgreSQL models named schema namespaces natively."""
        return True

    def supports_create_schema(self) -> bool:
        return True

    def supports_drop_schema(self) -> bool:
        return True

    def supports_schema_if_not_exists(self) -> bool:
        return True

    def supports_schema_if_exists(self) -> bool:
        return True

    def supports_schema_cascade(self) -> bool:
        return True

    def supports_schema_authorization(self) -> bool:
        return True
