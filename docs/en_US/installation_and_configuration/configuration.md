# Connection Configuration

## Basic Configuration

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password"
)
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | str | "localhost" | Database server hostname |
| `port` | int | 5432 | Database server port |
| `database` | str | Required | Database name |
| `username` | str | Required | Database username |
| `password` | str | None | Database password |
| `options` | dict | None | Additional connection options |

## Advanced Options

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "sslmode": "prefer",
        "connect_timeout": 10,
        "application_name": "my_app",
        "client_encoding": "UTF8"
    }
)
```

## Environment Variables

For security, use environment variables:

```python
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE"),
    username=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)
```

💡 *AI Prompt:* "How should I securely manage database credentials in a production environment?"
