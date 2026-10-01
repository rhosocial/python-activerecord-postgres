# BackendGroup and BackendManager (PostgreSQL)

This document describes how to use `BackendGroup` and `BackendManager` with the PostgreSQL backend. For detailed API documentation, refer to the [core library documentation](../../../rhosocial-activerecord/docs/en_US/connection/connection_management.md).

## Quick Example

```python
from rhosocial.activerecord.connection import BackendGroup
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
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

`search_path` is the connection's schema resolution order. A model that does
not declare `__schema_name__` resolves through it, which makes it the right
knob for the common case:

```python
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="myapp",
    username="app",
    password="secret",
    search_path="app,public",
)
```

Three things to know:

- **Use the `search_path` field**, not `options="-c search_path=..."`. The
  dedicated field is validated and forwarded as a libpq connect parameter; the
  generic `options` string bypasses that and is easy to get wrong.
- **It is fixed at connect time.** It is a connection-establishment parameter, so
  it cannot be changed per query or per transaction. Switching tenants at
  runtime is not possible through this setting.
- **`default_schema` does not do this.** That field has never affected
  generated SQL; use `search_path`, or declare `__schema_name__` on the model.

See [Schema Namespaces](../../../../rhosocial/docs/en_US/modeling/schema_namespace.md)
in the core documentation for the model side of the same rule.

### Schema Namespaces

`__schema_name__` on a model selects the read/write namespace for that model.
`search_path` covers the common case, and `__schema_name__` is for the
exceptions:

```python
class Order(ActiveRecord):
    __table_name__ = "orders"           # -> "shop"."orders"
    __schema_name__ = "shop"
```

PostgreSQL-specific notes:

- **An aliased range must be referenced by its alias alone.** `FROM
  "shop"."orders" AS "o"` accepts `"o"."id"` but rejects both `"orders"."id"`
  and `"shop"."orders"."id"` with `invalid reference to FROM-clause entry`.
  The framework handles this by dropping the schema when a table alias is in
  effect.
- **`CREATE EXTENSION` now quotes its target schema**, so extensions can be
  installed into a mixed-case schema.
- **orafce functions are schema-qualified** with `oracle.` by default. Pass
  `schema=` to any function in `functions.orafce` if the extension lives
  elsewhere.
- **PostGIS types are emitted unqualified** (`GEOMETRY`, `GEOGRAPHY`), as are
  the `ST_*` calls. PostGIS must therefore be installed into a schema on
  `search_path` — typically `public`, or an `extensions` schema added to it.