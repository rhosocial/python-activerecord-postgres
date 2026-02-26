# Client Encoding

## Default Encoding

PostgreSQL uses UTF-8 by default, which is recommended for most applications:

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "client_encoding": "UTF8"
    }
)
```

## Common Encodings

| Encoding | Description |
|----------|-------------|
| `UTF8` | Unicode UTF-8 (recommended) |
| `LATIN1` | ISO 8859-1 |
| `WIN1252` | Windows CP1252 |

## Handling Encoding Issues

If you encounter encoding errors:

1. Ensure your database uses UTF-8:
   ```sql
   SHOW server_encoding;
   ```

2. Set client encoding explicitly:
   ```python
   config = PostgresConnectionConfig(
       # ...
       options={"client_encoding": "UTF8"}
   )
   ```

3. For legacy databases with non-UTF-8 encoding:
   ```python
   # Let PostgreSQL handle conversion
   config = PostgresConnectionConfig(
       # ...
       options={"client_encoding": "LATIN1"}
   )
   ```

💡 *AI Prompt:* "What are the common causes of encoding mismatches between client and server?"
