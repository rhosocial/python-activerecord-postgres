# Database Introspection

The PostgreSQL backend provides complete database introspection capabilities using `pg_catalog` system tables for querying database structure metadata.

## Overview

The PostgreSQL introspection system is accessible via `backend.introspector` and provides:

- **Database Information**: Name, version, encoding, database size
- **Table Information**: Table list, table details (including materialized views, partitioned tables, foreign tables)
- **Column Information**: Column name, data type, nullability, default value
- **Index Information**: Index name, columns, uniqueness, index type (BTREE, GIN, GiST, etc.)
- **Foreign Key Information**: Referenced table, column mapping, update/delete actions
- **View Information**: View definition SQL
- **Trigger Information**: Trigger event, execution timing

## Basic Usage

### Accessing the Introspector

```python
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend

backend = PostgreSQLBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
backend.connect()

# Access via introspector attribute
introspector = backend.introspector
```

### Getting Database Information

```python
# Get basic database information
db_info = backend.introspector.get_database_info()
print(f"Database name: {db_info.name}")
print(f"Version: {db_info.version}")
print(f"Encoding: {db_info.encoding}")
print(f"Database size: {db_info.size_bytes} bytes")
```

### Listing Tables

```python
# List all user tables
tables = backend.introspector.list_tables()
for table in tables:
    print(f"Table: {table.name}, Type: {table.table_type.value}")
    if table.comment:
        print(f"  Comment: {table.comment}")

# Include system tables
all_tables = backend.introspector.list_tables(include_system=True)

# Filter by specific type
base_tables = backend.introspector.list_tables(table_type="BASE TABLE")
views = backend.introspector.list_tables(table_type="VIEW")
materialized_views = backend.introspector.list_tables(table_type="MATERIALIZED VIEW")

# Check if table exists
if backend.introspector.table_exists("users"):
    print("users table exists")

# Get table details
table_info = backend.introspector.get_table_info("users")
if table_info:
    print(f"Table name: {table_info.name}")
    print(f"Estimated rows: {table_info.row_count}")
    print(f"Table size: {table_info.size_bytes} bytes")
```

### Querying Column Information

```python
# List all columns of a table
columns = backend.introspector.list_columns("users")
for col in columns:
    nullable = "NOT NULL" if col.nullable.value == "NOT_NULL" else "NULLABLE"
    pk = " [PK]" if col.is_primary_key else ""
    print(f"{col.name}: {col.data_type} {nullable}{pk}")
    if col.comment:
        print(f"  Comment: {col.comment}")

# Get primary key information
pk = backend.introspector.get_primary_key("users")
if pk:
    print(f"Primary key: {[c.name for c in pk.columns]}")

# Get single column information
col_info = backend.introspector.get_column_info("users", "email")
```

### Querying Indexes

```python
# List all indexes of a table
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    idx_type = idx.index_type.value if idx.index_type else "BTREE"
    unique = "UNIQUE " if idx.is_unique else ""
    primary = "PRIMARY " if idx.is_primary else ""
    print(f"{primary}{unique}Index: {idx.name} ({idx_type})")
    for col in idx.columns:
        desc = "DESC" if col.is_descending else "ASC"
        nulls = ""
        if col.is_nulls_first is not None:
            nulls = " NULLS FIRST" if col.is_nulls_first else " NULLS LAST"
        print(f"  - {col.name} ({desc}{nulls})")
```

### Querying Foreign Keys

```python
# List foreign keys of a table
foreign_keys = backend.introspector.list_foreign_keys("posts")
for fk in foreign_keys:
    print(f"FK: {fk.name}")
    print(f"  Columns: {fk.columns} -> {fk.referenced_table}.{fk.referenced_columns}")
    print(f"  ON DELETE: {fk.on_delete.value}")
    print(f"  ON UPDATE: {fk.on_update.value}")
```

### Querying Views

```python
# List all views
views = backend.introspector.list_views()
for view in views:
    print(f"View: {view.name}")

# Get view details
view_info = backend.introspector.get_view_info("user_posts_summary")
if view_info:
    print(f"Definition: {view_info.definition}")
    print(f"Updatable: {view_info.is_updatable}")
```

### Querying Triggers

```python
# List all triggers
triggers = backend.introspector.list_triggers()
for trigger in triggers:
    print(f"Trigger: {trigger.name} on {trigger.table_name}")
    print(f"  Event: {trigger.event.value}")
    print(f"  Timing: {trigger.timing.value}")

# List triggers for a specific table
table_triggers = backend.introspector.list_triggers("users")
```

## PostgreSQL-Specific Behaviors

### Table Types

PostgreSQL supports multiple table types:

| Type | Description |
|------|-------------|
| `BASE TABLE` | Regular table |
| `VIEW` | View |
| `MATERIALIZED VIEW` | Materialized view |
| `FOREIGN TABLE` | Foreign table |
| `PARTITIONED TABLE` | Partitioned table |

```python
tables = backend.introspector.list_tables()
for table in tables:
    if table.table_type.value == "MATERIALIZED VIEW":
        print(f"Materialized view: {table.name}")
    elif table.table_type.value == "PARTITIONED TABLE":
        print(f"Partitioned table: {table.name}")
```

### Schema Support

PostgreSQL uses schemas to organize database objects:

```python
# Default schema is 'public'
tables = backend.introspector.list_tables()  # Queries public schema

# Specify a different schema
tables = backend.introspector.list_tables(schema="analytics")

# Get table details with specific schema
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### Index Types

PostgreSQL supports multiple advanced index types:

| Index Type | Description |
|------------|-------------|
| `BTREE` | Default index type, suitable for equality and range queries |
| `HASH` | Equality queries only |
| `GIN` | Full-text search, JSONB, arrays |
| `GIST` | Geospatial, full-text search |
| `SPGIST` | Space-partitioned GiST |
| `BRIN` | Block range index for large tables |

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("articles")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        print(f"GIN index: {idx.name} (likely for JSONB or full-text search)")
    elif idx.index_type == IndexType.GIST:
        print(f"GiST index: {idx.name} (likely for geospatial data)")
```

### Trigger Events

PostgreSQL triggers support multiple events:

| Event | Description |
|-------|-------------|
| `INSERT` | Insert operation |
| `UPDATE` | Update operation |
| `DELETE` | Delete operation |
| `TRUNCATE` | Truncate table |

```python
triggers = backend.introspector.list_triggers("users")
for trigger in triggers:
    print(f"Trigger {trigger.name}:")
    print(f"  Event: {trigger.event.value}")
    print(f"  Timing: {trigger.timing.value}")  # BEFORE, AFTER, INSTEAD OF
```

### NULLS Ordering

PostgreSQL indexes support NULLS FIRST/LAST:

```python
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    for col in idx.columns:
        if col.is_nulls_first is not None:
            nulls_order = "NULLS FIRST" if col.is_nulls_first else "NULLS LAST"
            print(f"{idx.name}.{col.name}: {nulls_order}")
```

## Async API

The async backend provides identical introspection methods with the same names as the sync version:

```python
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgreSQLBackend

backend = AsyncPostgreSQLBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
await backend.connect()

# Async introspection methods (same names as sync version)
db_info = await backend.introspector.get_database_info()
tables = await backend.introspector.list_tables()
columns = await backend.introspector.list_columns("users")
indexes = await backend.introspector.list_indexes("users")
```

## Cache Management

Introspection results are cached by default for performance:

```python
# Clear all introspection cache
backend.introspector.clear_cache()

# Invalidate specific scope
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

# Invalidate all table-related cache
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE)

# Invalidate cache for a specific table
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users"
)

# Invalidate cache for a specific schema
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users",
    table_name="users"  # Used to qualify schema
)
```

## Version Compatibility

Introspection behavior differences across PostgreSQL versions:

| Feature | PostgreSQL 12+ | PostgreSQL 11 and below |
|---------|----------------|------------------------|
| Materialized view info | Full support | Basic support |
| Partitioned table info | Full support | Limited support |
| Generated columns | Supported | Not supported |
| Stored procedure triggers | Supported | Not supported |

## Best Practices

### 1. Use Caching

Introspection operations involve complex `pg_catalog` queries. Leverage caching:

```python
# First query caches the result
tables = backend.introspector.list_tables()

# Subsequent queries return from cache
tables_again = backend.introspector.list_tables()

# Only clear cache when table structure changes
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE, name="users")
```

### 2. Specify Schema

In multi-schema environments, explicitly specify the schema:

```python
# Explicitly specify schema to avoid ambiguity
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### 3. Leverage Index Type Information

Optimize queries based on index types:

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("documents")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        # GIN indexes are suitable for JSONB or full-text search queries
        print(f"GIN index available: {idx.name}")
```

### 4. Concurrent Queries in Async Environment

```python
async def get_schema_info(backend):
    # Concurrently fetch columns for multiple tables
    import asyncio
    tables = await backend.introspector.list_tables()
    tasks = [
        backend.introspector.list_columns(table.name)
        for table in tables
    ]
    all_columns = await asyncio.gather(*tasks)
    return dict(zip([t.name for t in tables], all_columns))
```

## API Reference

### Core Methods

| Method | Description | Parameters |
|--------|-------------|------------|
| `get_database_info()` | Get database information | None |
| `list_tables()` | List tables | `include_system`, `table_type`, `schema` |
| `get_table_info(name)` | Get table details | `name`, `schema` |
| `table_exists(name)` | Check table exists | `name`, `schema` |
| `list_columns(table_name)` | List columns | `table_name`, `schema` |
| `get_column_info(table_name, column_name)` | Get column details | `table_name`, `column_name`, `schema` |
| `get_primary_key(table_name)` | Get primary key | `table_name`, `schema` |
| `list_indexes(table_name)` | List indexes | `table_name`, `schema` |
| `get_index_info(table_name, index_name)` | Get index details | `table_name`, `index_name`, `schema` |
| `index_exists(table_name, index_name)` | Check index exists | `table_name`, `index_name`, `schema` |
| `list_foreign_keys(table_name)` | List foreign keys | `table_name`, `schema` |
| `list_views()` | List views | `schema` |
| `get_view_info(name)` | Get view details | `name`, `schema` |
| `view_exists(name)` | Check view exists | `name`, `schema` |
| `list_triggers(table_name)` | List triggers | `table_name`, `schema` |
| `get_trigger_info(table_name, trigger_name)` | Get trigger details | `table_name`, `trigger_name`, `schema` |
| `clear_cache()` | Clear cache | None |
| `invalidate_cache(scope, ...)` | Invalidate cache | `scope`, `name`, `table_name` |

## Command Line Introspection

The PostgreSQL backend provides command-line introspection commands to query database metadata without writing code.

### Basic Usage

```bash
# List all tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --host localhost --port 5432 --database mydb --user postgres --password

# List all views
python -m rhosocial.activerecord.backend.impl.postgres introspect views \
  --database mydb --user postgres

# Get database information
python -m rhosocial.activerecord.backend.impl.postgres introspect database \
  --database mydb --user postgres

# Include system tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --include-system
```

### Connection Parameters

Supports configuration via command-line arguments or environment variables:

| Parameter | Environment Variable | Description |
|-----------|---------------------|-------------|
| `--host` | `POSTGRES_HOST` | Database host (default: localhost) |
| `--port` | `POSTGRES_PORT` | Database port (default: 5432) |
| `--database` | `POSTGRES_DATABASE` | Database name (required) |
| `--user` | `POSTGRES_USER` | Username (default: postgres) |
| `--password` | `POSTGRES_PASSWORD` | Password |

### Querying Table Details

```bash
# Get complete table info (columns, indexes, foreign keys)
python -m rhosocial.activerecord.backend.impl.postgres introspect table users \
  --database mydb --user postgres

# Query only column information
python -m rhosocial.activerecord.backend.impl.postgres introspect columns users \
  --database mydb --user postgres

# Query only index information
python -m rhosocial.activerecord.backend.impl.postgres introspect indexes users \
  --database mydb --user postgres

# Query only foreign key information
python -m rhosocial.activerecord.backend.impl.postgres introspect foreign-keys posts \
  --database mydb --user postgres

# Query triggers
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers \
  --database mydb --user postgres

# Query triggers for a specific table
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers users \
  --database mydb --user postgres
```

### Schema Support

PostgreSQL supports schema parameter:

```bash
# Specify schema to query tables
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --schema auth

# Query table details in specific schema
python -m rhosocial.activerecord.backend.impl.postgres introspect table users \
  --database mydb --user postgres --schema analytics
```

### Introspection Types

| Type | Description | Table Name Required |
|------|-------------|---------------------|
| `tables` | List all tables | No |
| `views` | List all views | No |
| `database` | Database information | No |
| `table` | Complete table details (columns, indexes, foreign keys) | Yes |
| `columns` | Column information | Yes |
| `indexes` | Index information | Yes |
| `foreign-keys` | Foreign key information | Yes |
| `triggers` | Trigger information | Optional |

### Output Formats

```bash
# Table format (default, requires rich library)
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres

# JSON format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output json

# CSV format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output csv

# TSV format
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output tsv
```

### Using Async Backend

```bash
# Use --use-async flag to enable async mode
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --use-async
```

### Environment Variable Configuration

You can set connection parameters in the environment to simplify command-line calls:

```bash
# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=mydb
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret

# Use command directly
python -m rhosocial.activerecord.backend.impl.postgres introspect tables
```
