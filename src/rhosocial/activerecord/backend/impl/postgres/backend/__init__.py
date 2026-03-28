# src/rhosocial/activerecord/backend/impl/postgres/backend/__init__.py
"""PostgreSQL backend implementations.

This module provides PostgreSQL backend implementations for both
synchronous and asynchronous database operations.

Exports:
    - PostgresBackendMixin: Shared functionality mixin for sync/async backends
    - PostgresBackend: Synchronous PostgreSQL backend
    - AsyncPostgresBackend: Asynchronous PostgreSQL backend
"""

from .base import PostgresBackendMixin
from .sync import PostgresBackend
from .async_backend import AsyncPostgresBackend


__all__ = [
    "PostgresBackendMixin",
    "PostgresBackend",
    "AsyncPostgresBackend",
]
