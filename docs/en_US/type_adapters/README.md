# Type Adapters

Type adapters handle conversion between Python and PostgreSQL types.

## Topics

- **[PostgreSQL to Python Type Mapping](./mapping.md)**: Type conversion rules
- **[Custom Type Adapters](./custom.md)**: Extending type support
- **[Timezone Handling](./timezone.md)**: TIMESTAMP WITH TIME ZONE
- **[Array Type Handling](./arrays.md)**: PostgreSQL array support
- **[Array Type Comparison](./array_comparison.md)**: PostgreSQL vs MySQL/SQLite array handling

## Adapter Registration Status

The backend automatically registers certain type adapters during initialization. Understanding which adapters are registered by default and which require manual registration helps you use PostgreSQL-specific types correctly.

### Default Registered Adapters

The following adapters are automatically registered when the backend initializes and can be used directly:

| Python Type | PostgreSQL Type | Adapter | Description |
|-------------|-----------------|---------|-------------|
| `dict` | `JSONB` | `PostgresJSONBAdapter` | Auto serialize/deserialize |
| `list` | `ARRAY` | `PostgresListAdapter` | Supports multi-dimensional arrays |
| `Point`, `Line`, etc. | Geometric types | `PostgresGeometryAdapter` | Point, line, polygon, etc. |
| `PostgresMoney` | `MONEY` | `PostgresMoneyAdapter` | Monetary type |
| `PostgresMacaddr` | `MACADDR` | `PostgresMacaddrAdapter` | MAC address |
| `PostgresTsVector` | `TSVECTOR` | `PostgresTsVectorAdapter` | Full-text search vector |
| `PostgresTsQuery` | `TSQUERY` | `PostgresTsQueryAdapter` | Full-text search query |
| `OID`, `RegClass`, etc. | Object identifiers | `PostgresOidAdapter` | OID and registry types |
| `XID`, `TID`, etc. | Transaction identifiers | `PostgresXidAdapter` / `PostgresTidAdapter` | Transaction and tuple identifiers |
| `PostgresLsn` | `PG_LSN` | `PostgresLsnAdapter` | Log sequence number |
| `Enum` | `ENUM` | `PostgresEnumAdapter` | Enum type |

#### About PostgresJSONBAdapter

`PostgresJSONBAdapter` provides important convenience:

**psycopg native behavior**: psycopg requires explicit `Jsonb()` wrapper, otherwise an error occurs:

```python
# Using native psycopg - manual wrapping required
from psycopg.types.json import Jsonb

# ERROR: Raw dict cannot be used directly
# backend.execute("INSERT INTO t (data) VALUES (%s)", [{'key': 'value'}])
# Error: "cannot adapt type 'dict'"

# CORRECT: Must use Jsonb wrapper
backend.execute("INSERT INTO t (data) VALUES (%s)", [Jsonb({'key': 'value'})])
```

**Using our adapter**: Auto-wrapping, no need to manually call `Jsonb()`:

```python
# Using PostgresJSONBAdapter - automatic handling
# Pass dict directly, adapter auto-converts to Jsonb
backend.execute("INSERT INTO t (data) VALUES (%s)", [{'key': 'value'}])

# Or use Jsonb() wrapper (both work - fully compatible)
backend.execute("INSERT INTO t (data) VALUES (%s)", [Jsonb({'key': 'value'})])
```

**Why registered by default**:
- psycopg does not provide automatic `dict -> JSONB` adaptation
- Manual wrapping adds code complexity and is easy to forget
- The adapter provides convenience while being fully compatible with native `Jsonb()`

### Non-Default Registered Adapters

The following adapters are not automatically registered due to type conflicts or requiring user decisions:

| Python Type | PostgreSQL Type | Adapter | Reason |
|-------------|-----------------|---------|--------|
| `PostgresRange` | Range types | `PostgresRangeAdapter` | Users can choose psycopg native `Range` |
| `PostgresMultirange` | Multirange types | `PostgresMultirangeAdapter` | Same as above, requires PostgreSQL 14+ |
| `PostgresXML` | `XML` | `PostgresXMLAdapter` | str→str mapping conflict |
| `PostgresBitString` | `BIT`, `VARBIT` | `PostgresBitStringAdapter` | str→str mapping conflict |
| `PostgresJsonPath` | `JSONPATH` | `PostgresJsonPathAdapter` | str→str mapping conflict |

## Usage Recommendations

### Native Types

For Python native types, use them directly without additional configuration:

```python
from rhosocial.activerecord import ActiveRecord
from datetime import datetime
from decimal import Decimal
from uuid import UUID

class User(ActiveRecord):
    name: str           # VARCHAR/TEXT
    age: int            # INTEGER
    score: float        # REAL/DOUBLE PRECISION
    active: bool        # BOOLEAN
    data: dict          # JSON/JSONB
    tags: list          # ARRAY
    created_at: datetime  # TIMESTAMP
    balance: Decimal    # NUMERIC
    id: UUID            # UUID
```

### PostgreSQL-Specific Types (Registered)

For PostgreSQL-specific types with registered adapters, use the corresponding wrapper classes:

```python
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresMoney, Point, PostgresTsVector
)

class Order(ActiveRecord):
    total: PostgresMoney      # MONEY type
    location: Point           # POINT type
    search_vector: PostgresTsVector  # TSVECTOR type
```

### Range Types (Optional Registration)

Range types can be used in two ways:

**Method 1: Use psycopg native Range (recommended for direct backend usage)**

```python
from psycopg.types.range import Range

# Pass psycopg Range directly, no conversion overhead
backend.execute(
    "SELECT * FROM orders WHERE period @> %s",
    [Range(1, 10, '[)')]
)
```

**Method 2: Use PostgresRange wrapper (recommended for ActiveRecord models)**

```python
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresRange, PostgresRangeAdapter
)

# Manually register the adapter
backend.adapter_registry.register(PostgresRangeAdapter(), PostgresRange, str)

class Order(ActiveRecord):
    period: PostgresRange  # Range type
```

### Conflict Types (Explicit Registration Required)

For types with str→str mapping conflicts, you must explicitly register the adapter:

```python
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresXML, PostgresXMLAdapter,
    PostgresBitString, PostgresBitStringAdapter,
    PostgresJsonPath, PostgresJsonPathAdapter
)

# Method 1: Global registration on backend
backend.adapter_registry.register(PostgresXMLAdapter(), PostgresXML, str)
backend.adapter_registry.register(PostgresBitStringAdapter(), PostgresBitString, str)
backend.adapter_registry.register(PostgresJsonPathAdapter(), PostgresJsonPath, str)

# Method 2: Specify per query (recommended)
result = backend.execute(
    "INSERT INTO docs (content) VALUES (%s)",
    [PostgresXML("<root>data</root>")],
    type_adapters={PostgresXML: PostgresXMLAdapter()}
)
```

### Using Conflict Types in ActiveRecord Models

```python
from rhosocial.activerecord import ActiveRecord
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresXML, PostgresXMLAdapter,
    PostgresBitString, PostgresBitStringAdapter
)

class Document(ActiveRecord):
    content: PostgresXML      # Requires explicit adapter registration
    flags: PostgresBitString  # Requires explicit adapter registration

# Register adapters when configuring the model
Document.configure(
    backend=backend,
    type_adapters={
        PostgresXML: PostgresXMLAdapter(),
        PostgresBitString: PostgresBitStringAdapter(),
    }
)
```

## Built-in Adapters Overview

| Python Type | PostgreSQL Type | Adapter | Registration Status |
|-------------|-----------------|---------|---------------------|
| `str` | `TEXT`, `VARCHAR` | Base adapter | Always available |
| `int` | `INTEGER`, `BIGINT` | Base adapter | Always available |
| `float` | `REAL`, `DOUBLE PRECISION` | Base adapter | Always available |
| `bool` | `BOOLEAN` | Base adapter | Always available |
| `bytes` | `BYTEA` | Base adapter | Always available |
| `date` | `DATE` | Base adapter | Always available |
| `time` | `TIME` | Base adapter | Always available |
| `datetime` | `TIMESTAMP` | Base adapter | Always available |
| `UUID` | `UUID` | Base adapter | Always available |
| `dict` | `JSONB` | `PostgresJSONBAdapter` | ✅ Default registered |
| `list` | `ARRAY` | `PostgresListAdapter` | ✅ Default registered |
| Geometric types | `POINT`, `LINE`, etc. | `PostgresGeometryAdapter` | ✅ Default registered |
| `PostgresMoney` | `MONEY` | `PostgresMoneyAdapter` | ✅ Default registered |
| `PostgresMacaddr` | `MACADDR` | `PostgresMacaddrAdapter` | ✅ Default registered |
| `PostgresTsVector` | `TSVECTOR` | `PostgresTsVectorAdapter` | ✅ Default registered |
| `PostgresRange` | Range types | `PostgresRangeAdapter` | ❓ Optional |
| `PostgresXML` | `XML` | `PostgresXMLAdapter` | ⚠️ Explicit registration required |
| `PostgresBitString` | `BIT`, `VARBIT` | `PostgresBitStringAdapter` | ⚠️ Explicit registration required |
| `PostgresJsonPath` | `JSONPATH` | `PostgresJsonPathAdapter` | ⚠️ Explicit registration required |

💡 *AI Prompt:* "How do type adapters handle NULL values?"
