# Connection Management

## Connect-on-Use Pattern

The PostgreSQL backend uses a "connect-on-use" pattern by default:

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(connection_config=config)
# Connection is NOT established yet

user = User(name="John")  # Connection established here
user.save()
```

## Manual Connection Management

### Synchronous

```python
backend = PostgresBackend(connection_config=config)

# Explicit connection
backend.connect()

try:
    # Database operations
    users = User.query().all()
finally:
    # Explicit disconnect
    backend.disconnect()
```

### Asynchronous

```python
backend = AsyncPostgresBackend(connection_config=config)

# Explicit async connection
await backend.connect()

try:
    users = await User.query().all()
finally:
    await backend.disconnect()
```

## Connection Pooling (Optional)

For high-throughput applications, you can use external connection pooling:

```bash
pip install rhosocial-activerecord-postgres[pooling]
```

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

config = PostgresConnectionConfig(
    # ... basic config ...
    options={
        "pool_min_size": 1,
        "pool_max_size": 10,
        "pool_timeout": 30.0
    }
)
```

💡 *AI Prompt:* "When should I use connection pooling versus connect-on-use?"
