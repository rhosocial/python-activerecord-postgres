# src/rhosocial/activerecord/backend/impl/postgres/protocols/schema.py
"""PostgreSQL schema feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresSchemaSupport(Protocol):
    """PostgreSQL schema feature support protocol."""

    def supports_create_schema(self)-> bool: ...

    def supports_drop_schema(self)-> bool: ...

    def supports_schema_if_not_exists(self)-> bool: ...

    def supports_schema_if_exists(self)-> bool: ...

    def supports_schema_cascade(self)-> bool: ...
