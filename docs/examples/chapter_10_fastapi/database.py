# database.py - Request-level Connection Management with BackendPool
# docs/examples/chapter_10_fastapi/database.py
"""
Request-level database connection manager using BackendPool.

Core principles:
1. Use BackendPool for connection pooling (PostgreSQL supports threadsafety=2)
2. Each request acquires connection from pool
3. Connection returned to pool after use
4. Efficient connection reuse across requests

Design rationale:
- PostgreSQL (psycopg) has threadsafety=2, connections CAN be shared across threads
- BackendPool provides true thread-safe connection management
- Connection pooling reduces connection creation overhead
- Recommended approach for PostgreSQL + FastAPI
"""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager

_src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))
if _src not in sys.path:
    sys.path.insert(0, _src)

from fastapi import Request
from rhosocial.activerecord.connection.pool import PoolConfig, AsyncBackendPool
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

from config_loader import load_config
from models import AsyncUser, AsyncPost, AsyncComment


# Global connection pool (created at startup)
_pool: AsyncBackendPool = None


async def init_pool():
    """
    Initialize connection pool at application startup.
    
    Should be called once during application initialization.
    """
    global _pool
    
    config = load_config()
    
    pool_config = PoolConfig(
        min_size=2,  # Minimum connections in pool
        max_size=10,  # Maximum connections in pool
        backend_factory=lambda: AsyncPostgresBackend(connection_config=config)
    )
    
    _pool = AsyncBackendPool(pool_config)
    print(f"Connection pool initialized (min={pool_config.min_size}, max={pool_config.max_size})")


async def close_pool():
    """
    Close connection pool at application shutdown.
    
    Should be called once during application cleanup.
    """
    global _pool
    
    if _pool:
        await _pool.close()
        _pool = None
        print("Connection pool closed.")


@asynccontextmanager
async def get_request_db():
    """
    Request-level database connection manager using BackendPool.
    
    Usage:
        @app.get("/users/{user_id}")
        async def get_user(user_id: int, db=Depends(get_request_db)):
            user = await AsyncUser.find_one(user_id)
            return {"user": user.to_dict()}
    
    Mechanism:
    1. Acquire connection from pool
    2. Bind models to acquired connection
    3. Execute database operations
    4. Return connection to pool (automatic)
    
    Performance considerations:
    - Connection reuse across requests (efficient)
    - Pool manages connection lifecycle
    - Suitable for high-concurrency scenarios
    """
    global _pool
    
    if _pool is None:
        raise RuntimeError("Connection pool not initialized. Call init_pool() at startup.")
    
    # Acquire connection from pool
    async with _pool.connection() as backend:
        # Bind models to this connection
        AsyncUser.__backend__ = backend
        AsyncPost.__backend__ = backend
        AsyncComment.__backend__ = backend
        
        yield backend
        # Connection automatically returned to pool


async def init_database():
    """
    Initialize database tables.
    
    Called at application startup to create necessary tables.
    """
    config = load_config()
    backend = AsyncPostgresBackend(connection_config=config)
    await backend.connect()
    
    # Create users table
    await backend.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(64) NOT NULL UNIQUE,
            email VARCHAR(255) NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create posts table
    await backend.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            title VARCHAR(255) NOT NULL,
            body TEXT,
            status VARCHAR(20) NOT NULL DEFAULT 'draft',
            view_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create comments table
    await backend.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id SERIAL PRIMARY KEY,
            post_id INTEGER NOT NULL REFERENCES posts(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            body TEXT,
            is_approved BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    await backend.disconnect()
    print("Database tables created successfully.")
