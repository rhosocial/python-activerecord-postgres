# src/rhosocial/activerecord/backend/impl/postgres/backend/__init__.py
"""PostgreSQL backend implementations.

This module provides the synchronous PostgreSQL backend implementation. The
async counterpart lives in ``impl.postgres.async_backend``; every backend
follows that layout, so the sync class is always at ``impl.<backend>.backend``
and the async class at ``impl.<backend>.async_backend``.
"""

from ..backend_base import PostgresBackendMixin
from .sync import PostgresBackend


__all__ = [
    "PostgresBackendMixin",
    "PostgresBackend",
]
