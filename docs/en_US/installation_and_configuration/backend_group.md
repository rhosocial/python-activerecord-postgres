# BackendGroup and BackendManager (PostgreSQL)

This document describes how to use `BackendGroup` and `BackendManager` with the PostgreSQL backend. For detailed API documentation, refer to the [core library documentation](../../../rhosocial-activerecord/docs/en_US/connection/connection_management.md).

## Quick Example

```python
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend, PostgresConnectionConfig
from rhosocial.activerecord.model import ActiveRecord


class User(ActiveRecord):
    name: str
    email: str


# Using context manager
with BackendGroup(
    name="main",
    models=[User],
    config=PostgresConnectionConfig(
        host="localhost",
        port=5432,
        database="myapp",
        username="app",
        password="secret",
    ),
    backend_class=PostgresBackend,
) as group:
    user = User(name="John", email="john@example.com")
    user.save()

# Using with multiple groups via BackendManager
from rhosocial.activerecord.connection import BackendManager

manager = BackendManager()
manager.create_group(
    name="main",
    models=[User],
    config=PostgresConnectionConfig(host="localhost", database="main_db"),
    backend_class=PostgresBackend,
)
manager.create_group(
    name="stats",
    config=PostgresConnectionConfig(host="localhost", database="stats_db"),
    backend_class=PostgresBackend,
)

main_backend = manager.get_group("main").get_backend()
stats_backend = manager.get_group("stats").get_backend()
```

## PostgreSQL-Specific Features

### SSL/TLS Configuration

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="myapp",
    username="app",
    password="secret",
    sslmode="require",
    sslrootcert="/path/to/ca.pem",
    sslcert="/path/to/client.pem",
    sslkey="/path/to/client.key",
)
```

### Connection Pool

PostgreSQL backend supports connection pool configuration (via `psycopg` pool):

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="myapp",
    username="app",
    password="secret",
    min_pool_size=5,
    max_pool_size=20,
)
```

### Search Path

Set default schema search path:

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="myapp",
    username="app",
    password="secret",
    options="-c search_path=public,extensions",
)
```