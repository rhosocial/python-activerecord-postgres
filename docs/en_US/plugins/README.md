# Plugin Support

PostgreSQL supports installing additional functionality modules through the extension mechanism. This section describes how to detect and use these extensions.

## Overview

PostgreSQL extension system allows installing additional modules, such as:
- **PostGIS**: Spatial database functionality
- **pgvector**: Vector similarity search (AI applications)
- **pg_trgm**: Trigram similarity search
- **PGroonga**: Multilingual full text search (Chinese/Japanese/Korean)
- **pg_hint_plan**: Optimizer hints
- **unaccent**: Accent-insensitive text normalization
- **amcheck / pageinspect / pgstattuple / pg_logicalinspect**: Diagnostics
- **pg_jsonschema**: JSON Schema validation (PG14-18)
- **TimescaleDB**: Time-series hypertables (amd64-only image family)
- **hstore**: Key-value pair storage

## Introspection and Adaptation

The backend automatically detects server capabilities and installed extensions via `introspect_and_adapt()`.

### Basic Usage

```python
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend

backend = PostgresBackend(
    host='localhost',
    port=5432,
    database='mydb',
    username='user',
    password='password'
)
backend.connect()

# Execute introspection and adaptation
backend.introspect_and_adapt()
```

### Detection Timing

`introspect_and_adapt()` is executed:
- On first call
- When reconnecting and server version changes

### Detection Content

1. **Server version**: Determine version-dependent feature support
2. **Installed extensions**: Query `pg_extension` system table
3. **Runtime adaptation**: Adjust feature support based on results

## Detecting Extensions

### Check if Extension is Installed

```python
# Check if extension is installed
if backend.dialect.is_extension_installed('postgis'):
    print("PostGIS is installed")

# Get extension details
ext_info = backend.dialect.get_extension_info('postgis')
if ext_info:
    print(f"Version: {ext_info.version}")
    print(f"Schema: {ext_info.schema}")
```

### Get Extension Version

```python
version = backend.dialect.get_extension_version('postgis')
if version:
    print(f"PostGIS version: {version}")
```

## Version Support

| PostgreSQL Version | Plugin Support Status |
|-------------------|----------------------|
| 14.x ~ 18.x | ✅ Full Support |
| 9.6 ~ 13.x | ⚠️ Best Effort |
| 9.5 and below | ❌ Not Tested |

**Note**: Plugin features are primarily developed and tested for PostgreSQL 14+. Lower versions may work but are not guaranteed.

## Known Extensions

The dialect maintains definitions for the following known extensions:

| Extension | Min Version | Category | Description |
|-----------|-------------|----------|-------------|
| postgis | 2.0 | spatial | PostGIS spatial database extension |
| vector | 0.1 | vector | pgvector vector similarity search |
| pg_trgm | 1.0 | text | Trigram similarity search |
| pgroonga | 2.0 | text | PGroonga multilingual full text search |
| pg_hint_plan | 1.3 | performance | Optimizer hints via /*+ ... */ |
| unaccent | 1.0 | text | Accent-insensitive text normalization |
| amcheck | 1.0 | monitoring | Index integrity checking |
| pageinspect | 1.0 | monitoring | Low-level page inspection |
| pgstattuple | 1.0 | monitoring | Tuple-level statistics |
| pg_logicalinspect | 1.0 | replication | Logical snapshot inspection (PG18+) |
| pg_jsonschema | 0.3 | validation | JSON Schema validation (PG14-18) |
| timescaledb | 2.0 | time_series | Hypertables (amd64-only postgres-exts-ts) |
| hstore | 1.0 | data | Key-value pair storage |
| uuid-ossp | 1.0 | utility | UUID generation functions |
| pgcrypto | 1.0 | security | Cryptographic functions |

## Installing Extensions

Before using an extension, install it in the database:

```sql
-- Install PostGIS
CREATE EXTENSION postgis;

-- Install pgvector
CREATE EXTENSION vector;

-- Install PGroonga (server-side package required)
CREATE EXTENSION pgroonga;

-- Install hstore
CREATE EXTENSION hstore;

-- Install pg_hint_plan and unaccent
CREATE EXTENSION pg_hint_plan;
CREATE EXTENSION unaccent;
```

After installation, re-run introspection:

```python
backend.execute("CREATE EXTENSION vector")
backend.introspect_and_adapt()
```

## Related Topics

- **[PostGIS](./postgis.md)**: Spatial database functionality
- **[pgvector](./pgvector.md)**: Vector similarity search
- **[pg_trgm](./pg_trgm.md)**: Trigram similarity
- **[PGroonga](./pgroonga.md)**: Multilingual full text search
- **[pg_hint_plan](./pg_hint_plan.md)**: Optimizer hints
- **[unaccent](./unaccent.md)**: Accent-insensitive text
- **[amcheck](./amcheck.md)** / **[pageinspect](./pageinspect.md)** / **[pgstattuple](./pgstattuple.md)** / **[pg_logicalinspect](./pg_logicalinspect.md)**: Diagnostics
- **[pg_jsonschema](./pg_jsonschema.md)**: JSON Schema validation
- **[TimescaleDB](./timescaledb.md)**: Time-series hypertables
- **[hstore](./hstore.md)**: Key-value storage

💡 *AI Prompt:* "What advantages does PostgreSQL's extension mechanism have over MySQL's plugin system?"
