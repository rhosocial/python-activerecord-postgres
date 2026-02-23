# Relationship with Core Library

## Backend Architecture

The PostgreSQL backend follows the same architecture pattern as other backends in the rhosocial-activerecord ecosystem:

```
rhosocial-activerecord (Core)
    └── Defines interfaces, base classes, and backend abstraction
    └── Backend-agnostic query building
    └── Protocol-based feature detection

rhosocial-activerecord-postgres (Backend)
    └── Implements backend interfaces
    └── PostgreSQL-specific dialect
    └── PostgreSQL-specific type adapters
    └── PostgreSQL-specific optimizations
```

## Key Integration Points

### 1. Backend Registration

The PostgreSQL backend registers itself with the core library through the standard backend interface:

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

# Configure model with PostgreSQL backend
User.configure(
    connection_config=config,
    backend_class=PostgresBackend
)
```

### 2. Dialect Integration

The PostgreSQL dialect extends the core SQL generation with PostgreSQL-specific syntax:

```python
# Core query (backend-agnostic)
User.query().where(User.c.age > 18)

# PostgreSQL-specific features
User.query().where("metadata->>'role' = ?", ('admin',))  # JSONB
User.query().where("tags @> ?", (['python'],))  # Array contains
```

### 3. Type Adapter Integration

PostgreSQL-specific type adapters handle conversions between Python and PostgreSQL types:

| Python Type | PostgreSQL Type | Notes |
|-------------|-----------------|-------|
| `list` | `ARRAY` | Native array support |
| `dict` | `JSONB` | Binary JSON |
| `UUID` | `UUID` | Native UUID support |
| `date` range | `DATERANGE` | Range types |

💡 *AI Prompt:* "How does the backend pattern enable switching between different databases?"

## See Also

- [PostgreSQL Dialect Expressions](../postgres_specific_features/dialect.md)
- [Type Adapters](../type_adapters/README.md)
