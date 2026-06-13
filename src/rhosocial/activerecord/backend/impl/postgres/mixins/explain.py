# src/rhosocial/activerecord/backend/impl/postgres/mixins/explain.py
"""PostgreSQL explain feature support implementation."""


class PostgresExplainMixin:
    """PostgreSQL explain override implementation.

    All features are native, using version number for detection.
    """

    def supports_explain_analyze(self) -> bool:
        return True

    def supports_explain_format(self, format_type: str) -> bool:
        format_type_upper = format_type.upper()
        supported_formats = ["TEXT", "XML", "JSON", "YAML"]
        return format_type_upper in supported_formats
