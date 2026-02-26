# SSL/TLS Configuration

## SSL Mode Options

| Mode | Description |
|------|-------------|
| `disable` | No SSL (not recommended for production) |
| `allow` | Try non-SSL first, fallback to SSL |
| `prefer` | Try SSL first, fallback to non-SSL (default) |
| `require` | Require SSL, no certificate verification |
| `verify-ca` | Require SSL with CA certificate verification |
| `verify-full` | Require SSL with full certificate verification |

## Basic SSL Configuration

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "sslmode": "require"
    }
)
```

## With Certificate Verification

```python
config = PostgresConnectionConfig(
    host="prod-db.example.com",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "sslmode": "verify-full",
        "sslrootcert": "/path/to/ca-cert.pem",
        "sslcert": "/path/to/client-cert.pem",
        "sslkey": "/path/to/client-key.pem"
    }
)
```

💡 *AI Prompt:* "What are the security implications of different SSL modes?"
