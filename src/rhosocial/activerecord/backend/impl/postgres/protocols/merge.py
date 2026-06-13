# src/rhosocial/activerecord/backend/impl/postgres/protocols/merge.py
"""PostgreSQL merge feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresMergeSupport(Protocol):
    """PostgreSQL merge feature support protocol."""

    def supports_merge_statement(self)-> bool: ...
