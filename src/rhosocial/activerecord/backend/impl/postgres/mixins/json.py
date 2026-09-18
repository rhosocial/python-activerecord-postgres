# src/rhosocial/activerecord/backend/impl/postgres/mixins/json.py
"""PostgreSQL JSON feature support mixin."""


class PostgresJSONMixin:
    """PostgreSQL JSON feature support."""

    def supports_json_arrow_operators(self) -> bool:
        """PostgreSQL supports -> and ->> operators for JSON/JSONB access."""
        return True
