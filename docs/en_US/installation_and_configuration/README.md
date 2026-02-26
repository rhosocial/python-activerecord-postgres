# Installation & Configuration

This section covers how to install and configure the PostgreSQL backend for rhosocial-activerecord.

## Topics

- **[Installation Guide](./installation.md)**: Install via pip and environment requirements
- **[Connection Configuration](./configuration.md)**: Database connection settings
- **[SSL/TLS Configuration](./ssl.md)**: Secure connection options
- **[Connection Management](./pool.md)**: Connection lifecycle management
- **[Client Encoding](./encoding.md)**: Character encoding configuration

## Quick Start

```bash
pip install rhosocial-activerecord-postgres
```

```python
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresBackend,
    PostgresConnectionConfig
)

config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password"
)

backend = PostgresBackend(connection_config=config)
```

💡 *AI Prompt:* "What are the best practices for managing database credentials in production applications?"
