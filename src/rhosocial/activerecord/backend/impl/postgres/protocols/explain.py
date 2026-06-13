# src/rhosocial/activerecord/backend/impl/postgres/protocols/explain.py
"""PostgreSQL explain feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresExplainSupport(Protocol):
    """PostgreSQL explain feature support protocol."""

    def supports_explain_analyze(self)-> bool: ...

    def supports_explain_format(self, format_type: str)-> bool: ...
