# Introduction

## PostgreSQL Backend Overview

`rhosocial-activerecord-postgres` is the PostgreSQL database backend implementation for the rhosocial-activerecord core library. It provides complete ActiveRecord pattern support, optimized specifically for PostgreSQL database features.

💡 *AI Prompt:* "What makes PostgreSQL different from other databases? What are its key advantages?"

## Synchronous and Asynchronous

The PostgreSQL backend provides both synchronous and asynchronous APIs that are functionally equivalent. The documentation will use synchronous examples throughout, but the asynchronous API usage is identical—just replace method calls with their async equivalents.

For example:

```python
# Synchronous usage
backend = PostgresBackend(...)
backend.connect()
users = backend.find('User')

# Asynchronous usage
backend = AsyncPostgresBackend(...)
await backend.connect()
users = await backend.find('User')
```

## Quick Links

- **[Relationship with Core Library](./relationship.md)**: Learn how the PostgreSQL backend works with the core library
- **[Supported Versions](./supported_versions.md)**: View supported PostgreSQL, Python, and dependency versions

💡 *AI Prompt:* "What are the important new features in PostgreSQL 16 compared to PostgreSQL 15?"
