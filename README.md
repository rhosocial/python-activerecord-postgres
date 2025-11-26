# rhosocial ActiveRecord postgres Backend ($\rho_{\mathbf{AR}}^{postgres}$)

[![PyPI version](https://badge.fury.io/py/rhosocial-activerecord-postgres.svg)](https://badge.fury.io/py/rhosocial-activerecord-postgres)
[![Python](https://img.shields.io/pypi/pyversions/rhosocial-activerecord-postgres.svg)](https://pypi.org/project/rhosocial-activerecord-postgres/)
[![Tests](https://github.com/rhosocial/python-activerecord-postgres/actions/workflows/test.yml/badge.svg)](https://github.com/rhosocial/python-activerecord-postgres/actions)
[![Coverage Status](https://codecov.io/gh/rhosocial/python-activerecord-postgres/branch/main/graph/badge.svg)](https://app.codecov.io/gh/rhosocial/python-activerecord-postgres/tree/main)
[![License](https://img.shields.io/github/license/rhosocial/python-activerecord-postgres.svg)](https://github.com/rhosocial/python-activerecord-postgres/blob/main/LICENSE)
[![Powered by vistart](https://img.shields.io/badge/Powered_by-vistart-blue.svg)](https://github.com/vistart)

<div align="center">
    <img src="https://raw.githubusercontent.com/rhosocial/python-activerecord/main/docs/images/logo.svg" alt="rhosocial ActiveRecord Logo" width="200"/>
    <p>postgres backend implementation for rhosocial-activerecord, providing a robust and optimized postgres database support.</p>
</div>

# postgres Backend Implementation Guide

## Overview

This postgres backend implementation provides both **synchronous** and **asynchronous** support using the `psycopg` (psycopg3) driver.

## Key Features

- ✅ **Dual Implementation**: Both sync (`PostgresBackend`) and async (`AsyncPostgresBackend`)
- ✅ **Shared Logic**: Common functionality in `PostgresBackendMixin`
- ✅ **Full Transaction Support**: Including savepoints and DEFERRABLE mode
- ✅ **Rich Type Support**: Arrays, JSONB, UUID, ranges, network types, geometry
- ✅ **Complete Capability Declaration**: CTEs, window functions, JSON operations, etc.
- ✅ **Native Driver**: Uses psycopg3 directly, no ORM dependencies

## Installation

```bash
pip install rhosocial-activerecord-postgres
```

**Dependencies**:
- `rhosocial-activerecord>=1.0.0,<2.0.0`
- `psycopg>=3.2.12`

## Usage Examples

### Synchronous Usage

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresBackend,
    PostgresConnectionConfig
)

# Configure connection
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        "sslmode": "prefer",
        "connect_timeout": 10,
        "application_name": "my_app"
    }
)

# Create backend
backend = PostgresBackend(connection_config=config)

# Configure model
class User(ActiveRecord):
    __table_name__ = "users"
    name: str
    email: str

User.configure(backend)

# Use the model
user = User(name="John", email="john@example.com")
user.save()

# Query with CTEs (postgres 8.4+)
results = User.query().with_cte(
    "active_users",
    User.query().where(is_active=True)
).from_cte("active_users").all()

# Use JSONB operations (postgres 9.4+)
users = User.query().where(
    "metadata->>'role' = ?", ("admin",)
).all()
```

### Asynchronous Usage

```python
import asyncio
from rhosocial.activerecord.async_model import ActiveRecord
from rhosocial.activerecord.backend.impl.postgres import (
    AsyncPostgresBackend,
    PostgresConnectionConfig
)

async def main():
    # Configure connection
    config = PostgresConnectionConfig(
        host="localhost",
        port=5432,
        database="mydb",
        username="user",
        password="password",
    )

    # Create async backend
    backend = ActiveRecord(connection_config=config)
    
    # Connect explicitly for async
    await backend.connect()

    # Configure model
    class User(ActiveRecord):
        __table_name__ = "users"
        name: str
        email: str

    User.configure(backend)

    # Use the model asynchronously
    user = User(name="Jane", email="jane@example.com")
    await user.save()

    # Async queries
    users = await User.query().where(is_active=True).all()

    # Cleanup
    await backend.disconnect()

# Run async code
asyncio.run(main())
```

### Transaction Usage

**Synchronous Transactions**:

```python
# Get transaction manager
tm = backend.transaction_manager

# Basic transaction
with tm:
    user1 = User(name="Alice")
    user1.save()
    user2 = User(name="Bob")
    user2.save()
# Auto-commit on context exit

# Explicit control
tm.begin()
try:
    user = User(name="Charlie")
    user.save()
    tm.commit()
except Exception:
    tm.rollback()
    raise

# With savepoints
tm.begin()
user1 = User(name="Dave")
user1.save()

savepoint = tm.savepoint()
try:
    user2 = User(name="Eve")
    user2.save()
    tm.release_savepoint(savepoint)
except Exception:
    tm.rollback_savepoint(savepoint)

tm.commit()

# With isolation level and deferrable mode
tm.set_isolation_level(IsolationLevel.SERIALIZABLE)
tm.set_deferrable(True)
with tm:
    # Deferrable serializable transaction
    pass
```

**Asynchronous Transactions**:

```python
async def async_transaction_example():
    # Get async transaction manager
    tm = backend.transaction_manager

    # Basic async transaction
    async with tm:
        user1 = User(name="Alice")
        await user1.save()
        user2 = User(name="Bob")
        await user2.save()
    # Auto-commit on context exit

    # Explicit control
    await tm.begin()
    try:
        user = User(name="Charlie")
        await user.save()
        await tm.commit()
    except Exception:
        await tm.rollback()
        raise

    # With savepoints
    await tm.begin()
    user1 = User(name="Dave")
    await user1.save()

    savepoint = await tm.savepoint()
    try:
        user2 = User(name="Eve")
        await user2.save()
        await tm.release_savepoint(savepoint)
    except Exception:
        await tm.rollback_savepoint(savepoint)

    await tm.commit()
```

### Postgres-Specific Features

**Array Types**:

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list  # Will use postgres array type

article = Article(
    title="postgres Arrays",
    tags=["database", "postgres", "arrays"]
)
article.save()

# Query arrays
articles = Article.query().where(
    "? = ANY(tags)", ("postgres",)
).all()
```

**JSONB Operations**:

```python
from rhosocial.activerecord.model import ActiveRecord

class Product(ActiveRecord):
    __table_name__ = "products"
    name: str
    attributes: dict  # Will use JSONB type

product = Product(
    name="Laptop",
    attributes={
        "brand": "Dell",
        "specs": {
            "cpu": "Intel i7",
            "ram": "16GB"
        }
    }
)
product.save()

# Query JSONB
products = Product.query().where(
    "attributes->>'brand' = ?", ("Dell",)
).all()

# JSONB contains
products = Product.query().where(
    "attributes @> ?", ('{"brand": "Dell"}',)
).all()
```

**Range Types**:

```python
from datetime import date
from rhosocial.activerecord.model import ActiveRecord

class Booking(ActiveRecord):
    __table_name__ = "bookings"
    room_id: int
    date_range: tuple  # Will use DATERANGE type

booking = Booking(
    room_id=101,
    date_range=(date(2024, 1, 1), date(2024, 1, 7))
)
booking.save()

# Query ranges
bookings = Booking.query().where(
    "date_range @> ?", (date(2024, 1, 3),)
).all()
```

## Configuration Options

### Connection Options

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="mydb",
    username="user",
    password="password",
    options={
        # SSL/TLS
        "sslmode": "prefer",  # disable, allow, prefer, require, verify-ca, verify-full
        
        # Connection timeout
        "connect_timeout": 10,
        
        # Application identification
        "application_name": "my_app",
        
        # Client encoding
        "client_encoding": "UTF8",
        
        # Connection pooling (if supported)
        "pool_min_size": 1,
        "pool_max_size": 10,
        "pool_timeout": 30.0,
    }
)
```

## Postgres Version Compatibility

| Feature | Minimum Version | Notes |
|---------|----------------|-------|
| Basic operations | 8.0+ | Core functionality |
| CTEs | 8.4+ | WITH clauses |
| Window functions | 8.4+ | ROW_NUMBER, RANK, etc. |
| RETURNING clause | 8.2+ | INSERT/UPDATE/DELETE RETURNING |
| JSON type | 9.2+ | Basic JSON support |
| JSONB type | 9.4+ | Binary JSON, better performance |
| UPSERT (ON CONFLICT) | 9.5+ | INSERT ... ON CONFLICT |

**Recommended**: Postgres 12+ for optimal feature support and performance.

## Architecture Notes

### Backend Structure

```
PostgresBackendMixin (Shared Logic)
    ├── Configuration validation
    ├── Version parsing
    ├── Capability initialization
    ├── Type converter registration
    └── Error mapping

PostgresBackend (Sync)           AsyncPostgresBackend (Async)
    ├── Inherits Mixin                 ├── Inherits Mixin
    ├── Inherits StorageBackend        ├── Inherits AsyncStorageBackend
    ├── Sync connection management     ├── Async connection management
    ├── Sync query execution           ├── Async query execution
    └── Sync transaction manager       └── Async transaction manager
```

### Transaction Structure

```
PostgresTransactionMixin (Shared Logic)
    ├── Isolation level mapping
    ├── Savepoint name generation
    ├── SQL statement building
    └── Deferrable mode support

PostgresTransactionManager       AsyncPostgresTransactionManager
    ├── Inherits Mixin                 ├── Inherits Mixin
    ├── Inherits TransactionManager    ├── Inherits AsyncTransactionManager
    ├── Sync transaction operations    ├── Async transaction operations
    └── Sync constraint management     └── Async constraint management
```

## Testing

The backend includes comprehensive test coverage for both sync and async implementations:

- Connection lifecycle tests
- CRUD operation tests
- Transaction tests (with savepoints)
- Type conversion tests
- Capability declaration verification
- Error handling tests
- Concurrent operation tests

**Run sync tests**:
```bash
pytest tests/rhosocial/activerecord_test/feature/backend/postgres/test_backend_sync.py
```

**Run async tests**:
```bash
pytest tests/rhosocial/activerecord_test/feature/backend/postgres/test_backend_async.py
```

## Migration from Old Implementation

If you have an existing postgres backend implementation, here's how to migrate:

**Old code**:
```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(...)
```

**New code** (no changes needed for sync):
```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(...)  # Same API
```

**New async support**:
```python
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

backend = AsyncPostgresBackend(...)
await backend.connect()
```

## Known Limitations

1. **Connection Pooling**: Basic implementation, consider using external pooling (pgbouncer) for production
2. **Async Context**: Async backend requires explicit `await backend.connect()` call
3. **Type Converters**: Some postgres-specific types may need custom converters

## Contributing

Contributions are welcome! Please ensure:

- Tests pass for both sync and async implementations
- Code follows project style guidelines
- Documentation is updated
- Changelog fragments are created

## License

[![license](https://img.shields.io/github/license/rhosocial/python-activerecord-postgres.svg)](https://github.com/rhosocial/python-activerecord-postgres/blob/main/LICENSE)

Copyright © 2025 [vistart](https://github.com/vistart)