# src/rhosocial/activerecord/backend/impl/postgres/__init__.py
"""
postgres backend implementation for the Python ORM.

This module provides a postgres-specific implementation including:
- Synchronous postgres backend with connection management and query execution
- Asynchronous postgres backend for async/await usage
- Advanced type mapping and value conversion including native postgres types
- Transaction management with full ACID compliance and DEFERRABLE support
- postgres dialect and expression handling
- postgres-specific type definitions and mappings

The implementation uses psycopg3 (psycopg>=3.2) which provides unified support
for both synchronous and asynchronous operations.
"""

from .backend import (
    PostgresBackend,
    AsyncPostgresBackend,
    PostgresBackendMixin,
)
from .config import PostgresConnectionConfig
from .dialect import (
    PostgresDialect,
    PostgresExpression,
    PostgresSQLBuilder,
    PostgresAggregateHandler,
    PostgresJsonHandler,
    PostgresReturningHandler,
)
from .transaction import (
    PostgresTransactionManager,
    AsyncPostgresTransactionManager,
    PostgresTransactionMixin,
)
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

    # Shared Mixin
    'PostgresBackendMixin',

    # Configuration
    'PostgresConnectionConfig',

    # Dialect related
    'PostgresDialect',
    'PostgresExpression',
    'PostgresSQLBuilder',
    'PostgresAggregateHandler',
    'PostgresJsonHandler',
    'PostgresReturningHandler',

    # Synchronous Transaction
    'PostgresTransactionManager',

    # Asynchronous Transaction
    'AsyncPostgresTransactionManager',

    # Shared Transaction Mixin
    'PostgresTransactionMixin',

    # Types
    'PostgresTypes',
    'PostgresColumnType',
    'POSTGRES_TYPE_MAPPINGS',
]