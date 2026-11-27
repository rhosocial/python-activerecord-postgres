# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_jsonschema.py
"""Protocol for the pg_jsonschema extension."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgJsonSchemaSupport(Protocol):
    """Capability probe for JSON Schema validation on json / jsonb."""

    def supports_pg_jsonschema(self) -> bool:
        """Whether JSON Schema validation functions are available."""
        ...
