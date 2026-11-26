# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pageinspect.py
"""Protocol for the pageinspect extension."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPageinspectSupport(Protocol):
    """Capability probe for pageinspect: Low-level inspection of database pages."""

    def supports_pageinspect(self) -> bool:
        """Whether this extension's capabilities are available."""
        ...
