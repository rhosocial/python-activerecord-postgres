# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_trgm.py
"""pg_trgm extension protocol — capability probes only.

Index DDL is an expression: ``functions.pg_trgm.trgm_index`` returns a
``PostgresCreateIndexExpression`` rendered by ``format_create_index_statement``.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgTrgmSupport(Protocol):
    """pg_trgm trigram functionality protocol."""

    def supports_pg_trgm_similarity(self) -> bool: ...

    def supports_pg_trgm_index(self) -> bool: ...

    def format_postgres_trgm_index_expression(self, expr) -> tuple: ...
