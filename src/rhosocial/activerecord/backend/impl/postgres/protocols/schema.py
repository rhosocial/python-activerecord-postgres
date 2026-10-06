# src/rhosocial/activerecord/backend/impl/postgres/protocols/schema.py
"""PostgreSQL schema DDL protocol.

Everything declared here is the DDL side: does the engine have schemas, and what
``CREATE``/``DROP SCHEMA`` can carry. The naming side -- whether a name may be
*qualified* with a schema -- is
:class:`~.catalog.PostgresNamespaceSupport`, implemented by
:class:`~...mixins.namespace.PostgresNamespaceMixin`. PostgreSQL answers both
switches the same way, which is exactly why they must stay two: an engine can
offer ``CREATE SCHEMA`` and still refuse to qualify a name, and reading the wrong
one gives the wrong answer with nothing to tell the two cases apart.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresSchemaSupport(Protocol):
    """PostgreSQL schema DDL support protocol."""

    def supports_schema(self)-> bool: ...

    def supports_create_schema(self)-> bool: ...

    def supports_drop_schema(self)-> bool: ...

    def supports_schema_if_not_exists(self)-> bool: ...

    def supports_schema_if_exists(self)-> bool: ...

    def supports_schema_cascade(self)-> bool: ...

    def supports_schema_authorization(self)-> bool: ...