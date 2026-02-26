# Performance Issues

## Overview

This section covers PostgreSQL performance issues and optimization methods.

## Slow Query Analysis

### Enabling Slow Query Logging

```sql
-- View current configuration
SHOW log_min_duration_statement;

-- Enable logging for queries over 1000ms
ALTER SYSTEM SET log_min_duration_statement = 1000;
SELECT pg_reload_conf();
```

### Using EXPLAIN ANALYZE

```python
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend, PostgreSQLConnectionConfig

backend = PostgreSQLBackend(
    connection_config=PostgreSQLConnectionConfig(
        host='localhost',
        database='myapp',
        username='user',
        password='password',
    )
)
backend.connect()

with backend.get_connection().cursor() as cursor:
    cursor.execute("EXPLAIN ANALYZE SELECT * FROM users WHERE name = 'Tom'")
    for row in cursor:
        print(row)

backend.disconnect()
```

## Common Performance Issues

### 1. Missing Index

```sql
-- Check if index exists
SELECT indexname FROM pg_indexes WHERE tablename = 'users';

-- Add index
CREATE INDEX idx_users_name ON users(name);

-- Add partial index (PostgreSQL specific)
CREATE INDEX idx_users_active ON users(name) WHERE active = true;
```

### 2. SELECT *

```python
# Avoid SELECT *, only query required columns
users = User.query().select(User.c.id, User.c.name).all()
```

### 3. N+1 Query Problem

```python
# Use eager loading to avoid N+1
users = User.query().eager_load('posts').all()
```

### 4. VACUUM and ANALYZE

```sql
-- Run VACUUM to reclaim space
VACUUM users;

-- Run ANALYZE to update statistics
ANALYZE users;

-- Combined
VACUUM ANALYZE users;
```

## Connection Pooling

For high-concurrency applications, use connection pooling:

```python
config = PostgreSQLConnectionConfig(
    host='localhost',
    database='myapp',
    username='user',
    password='password',
    min_connections=5,
    max_connections=20,
)
```

Or use PgBouncer for external connection pooling:

```bash
# Install PgBouncer
sudo apt install pgbouncer

# Configure pgbouncer.ini
[databases]
myapp = host=localhost port=5432 dbname=myapp

[pgbouncer]
pool_mode = transaction
max_client_conn = 100
default_pool_size = 20
```

💡 *AI Prompt:* "How to optimize PostgreSQL query performance?"
