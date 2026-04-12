# FastAPI + PostgreSQL + Async Backend Example
# docs/examples/chapter_10_fastapi/README.md

This example demonstrates connection pooling in FastAPI async applications using BackendPool.

## Core Principle

**Connection pooling with BackendPool** - PostgreSQL supports `threadsafety=2`, making BackendPool the recommended approach for thread-safe connection management.

## Why BackendPool for PostgreSQL?

### PostgreSQL's Advantage
- `psycopg` has `threadsafety=2`
- Connections CAN be shared across threads
- BackendPool provides efficient connection reuse
- Reduced connection creation overhead

### Connection Pool Benefits
- Connection reuse across requests (efficient)
- Pool manages connection lifecycle automatically
- Suitable for high-concurrency scenarios
- True thread-safe connection management

## File Structure

```
chapter_10_fastapi/
├── README.md           # This file
├── config_loader.py    # Database configuration
├── models.py           # Async model definitions
├── database.py         # Connection pool manager
└── app.py              # FastAPI application
```

## Key Components

### 1. Connection Pool Initialization (`database.py`)

```python
# Global connection pool (created at startup)
_pool: AsyncBackendPool = None

async def init_pool():
    global _pool
    
    pool_config = PoolConfig(
        min_size=2,  # Minimum connections
        max_size=10,  # Maximum connections
        backend_factory=lambda: AsyncPostgresBackend(connection_config=config)
    )
    
    _pool = AsyncBackendPool(pool_config)
```

### 2. Request-Level Connection Acquisition (`database.py`)

```python
@asynccontextmanager
async def get_request_db():
    # Acquire connection from pool
    async with _pool.connection() as backend:
        # Bind models to this connection
        AsyncUser.__backend__ = backend
        AsyncPost.__backend__ = backend
        AsyncComment.__backend__ = backend
        
        yield backend
        # Connection automatically returned to pool
```

### 3. Dependency Injection (`app.py`)

```python
@app.get("/users/{user_id}")
async def get_user(user_id: int, db=Depends(get_request_db)):
    user = await AsyncUser.find_one(user_id)
    return {"user": user.to_dict()}
```

## Running

```bash
cd docs/examples/chapter_10_fastapi
uvicorn app:app --reload
```

## Testing

```bash
# List users
curl http://localhost:8000/users

# Create user
curl -X POST http://localhost:8000/users \
     -H "Content-Type: application/json" \
     -d '{"username": "test", "email": "test@example.com"}'

# Get user
curl http://localhost:8000/users/1

# Get user's posts
curl http://localhost:8000/users/1/posts

# Create post
curl -X POST http://localhost:8000/posts \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "title": "My First Post", "body": "Hello World"}'

# Add comment
curl -X POST http://localhost:8000/posts/1/comments \
     -H "Content-Type: application/json" \
     -d '{"user_id": 1, "body": "Great post!"}'
```

## Comparison: MySQL vs PostgreSQL

| Aspect | MySQL | PostgreSQL |
|--------|-------|------------|
| threadsafety | 1 | 2 |
| BackendPool | ❌ Not supported | ✅ Recommended |
| Recommended approach | Request-level BackendGroup | BackendPool |
| Connection reuse | Per-request (create/disconnect) | Pool-managed reuse |
| Performance | Acceptable overhead | Optimized for pooling |

## Use Cases

### Suitable
- Web API applications (high concurrency)
- Real-time applications
- Microservices
- Any scenario requiring connection pooling

### Also Suitable
- WebSocket connections (with proper pool sizing)
- Batch processing (with connection pooling)
- Background tasks (with pool access)

## Performance

- Connection reuse reduces creation overhead
- Pool manages connection health automatically
- Configurable pool size for different workloads
- Optimal for high-concurrency scenarios
