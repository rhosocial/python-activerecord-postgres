# src/rhosocial/activerecord/backend/impl/postgres/__init__.py
"""
PostgreSQL backend implementation for the Python ORM.

This module provides:
- PostgreSQL synchronous backend with connection management and query execution
- PostgreSQL asynchronous backend with async/await support
- PostgreSQL-specific connection configuration
- Type mapping and value conversion
- Transaction management with savepoint support (sync and async)
- PostgreSQL dialect and expression handling
- PostgreSQL-specific type definitions and mappings

Architecture:
- PostgreSQLBackend: Synchronous implementation using psycopg
- AsyncPostgreSQLBackend: Asynchronous implementation using psycopg
- Independent from ORM frameworks - uses only native drivers
"""

from .backend import PostgresBackend
from .async_backend import AsyncPostgresBackend
from .config import PostgresConnectionConfig
from .dialect import PostgresDialect
from .transaction import PostgresTransactionManager
from .async_transaction import AsyncPostgresTransactionManager
from .types import (
    PostgresTypes,
    PostgresColumnType,
    POSTGRES_TYPE_MAPPINGS,
)

__all__ = [
    # Synchronous Backend
    'PostgresBackend',

    # Asynchronous Backend
    'AsyncPostgresBackend',

    # Configuration
    'PostgresConnectionConfig',

    # Dialect related
    'PostgresDialect',

    # Transaction - Sync and Async
    'PostgresTransactionManager',
    'AsyncPostgresTransactionManager',

    # Types
    'PostgresTypes',
    'PostgresColumnType',
    'POSTGRES_TYPE_MAPPINGS',
]