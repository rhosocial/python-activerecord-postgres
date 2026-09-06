# PostgreSQL Extensions

## Overview

PostgreSQL supports extending its functionality through extensions. The backend provides automatic detection and integration for many popular extensions.

## Supported Extensions

| Extension | Category | Description | Min Version |
|-----------|----------|-------------|-------------|
| PostGIS | Spatial | Spatial database functionality | 2.0 |
| pgvector | AI/ML | Vector similarity search | 0.5.0 |
| pg_trgm | Text | Trigram similarity search | 1.0 |
| hstore | Key-Value | Key-value pair storage | 1.0 |
| uuid-ossp | UUID | UUID generation functions | 1.0 |
| pgcrypto | Crypto | Cryptographic functions | 1.0 |
| ltree | Hierarchical | Label tree hierarchical data | 1.0 |
| intarray | Array | Integer array operations | 1.0 |
| citext | Text | Case-insensitive text | 1.0 |
| btree_gin | Index | B-tree GIN index support | 1.0 |
| btree_gist | Index | B-tree GiST index support | 1.0 |
| pg_partman | Partition | Partition management | 4.0 |
| pg_cron | Scheduler | Job scheduling | 1.0 |
| pg_stat_statements | Monitoring | Query statistics | 1.0 |
| pgaudit | Audit | Audit logging | 1.0 |
| pglogical | Replication | Logical replication | 2.0 |
| pg_surgery | Repair | Table surgery operations | 1.0 |
| pg_walinspect | Monitoring | WAL inspection | 1.0 |
| tablefunc | Function | Cross-tab queries | 1.0 |
| cube | Geometry | Multi-dimensional cubes | 1.0 |
| earthdistance | Geometry | Earth distance calculations | 1.0 |
| fuzzystrmatch | Text | Fuzzy string matching | 1.0 |
| orafce | Compatibility | Oracle compatibility functions | 3.0 |
| hypopg | Planning | Hypothetical indexes | 1.0 |
| bloom | Index | Bloom filter index | 1.0 |
| address_standardizer | Address | Address parsing | 1.0 |
| postgis_raster | Spatial | Raster data support | 2.0 |
| pgrouting | Routing | Geospatial routing | 2.0 |

## Extension Detection

The backend automatically detects installed extensions during connection:

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(...)
backend.connect()

# Extensions are automatically detected
# Check if a specific extension is installed
if backend.dialect.is_extension_installed('postgis'):
    print("PostGIS is available")

# Get extension version
version = backend.dialect.get_extension_version('pgvector')
print(f"pgvector version: {version}")

# List all installed extensions
extensions = backend.dialect.detect_extensions()
for name, info in extensions.items():
    print(f"{name}: {info.version}")
```

## Extension Details

### PostGIS (Spatial)

PostGIS provides complete spatial database functionality:

```python
# Requires PostGIS extension
# Install: CREATE EXTENSION postgis;

from rhosocial.activerecord.backend.impl.postgres.protocols.extensions.postgis import PostgresPostGISSupport

# Check capabilities
if backend.dialect.supports_postgis_geometry_type():
    print("Geometry type supported")

if backend.dialect.supports_postgis_spatial_index():
    print("Spatial indexing supported")
```

**Features:**
- Geometry and geography data types
- Spatial indexes (GiST)
- Spatial analysis functions
- Coordinate system transformations

### pgvector (AI/ML)

pgvector provides vector similarity search for AI applications:

```python
# Requires pgvector extension
# Install: CREATE EXTENSION vector;

from rhosocial.activerecord.backend.impl.postgres.protocols.extensions.pgvector import PostgresPgvectorSupport

# Check capabilities
if backend.dialect.supports_pgvector():
    print("pgvector supported")
```

**Features:**
- Vector data type
- L2 distance, inner product, cosine distance
- IVFFlat and HNSW indexes
- Approximate nearest neighbor search

### pg_trgm (Text Search)

pg trigram provides trigram similarity search:

```python
# Requires pg_trgm extension
# Install: CREATE EXTENSION pg_trgm;

# Check capabilities
if backend.dialect.supports_pg_trgm():
    print("pg_trgm supported")
```

**Features:**
- Trigram similarity matching
- Fuzzy text search
- GIN/GiST index support

### hstore (Key-Value)

hstore provides key-value pair storage:

```python
# Requires hstore extension
# Install: CREATE EXTENSION hstore;

# Check capabilities
if backend.dialect.supports_hstore():
    print("hstore supported")
```

**Features:**
- Key-value pair storage
- Containment operators
- Array conversion

### ltree (Hierarchical)

ltree provides label tree hierarchical data:

```python
# Requires ltree extension
# Install: CREATE EXTENSION ltree;

# Check capabilities
if backend.dialect.supports_ltree():
    print("ltree supported")
```

**Features:**
- Hierarchical label paths
- Tree traversal operators
- LCA (Lowest Common Ancestor) queries

## Extension Management

### DDL Expressions

The backend provides DDL expressions for managing extensions:

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.extension import (
    PostgresCreateExtensionExpression,
    PostgresDropExtensionExpression,
)

# Create extension
create_expr = PostgresCreateExtensionExpression(
    dialect,
    extension_name="postgis",
    if_not_exists=True,
)

# Drop extension
drop_expr = PostgresDropExtensionExpression(
    dialect,
    extension_name="postgis",
    if_exists=True,
)
```

### CLI Commands

Extensions can be managed via the CLI:

```bash
# List installed extensions
rhosocial-activerecord-postgres introspect extensions \
    --host localhost --database mydb

# Check specific extension
rhosocial-activerecord-postgres introspect extensions \
    --host localhost --database mydb | grep postgis
```

## Version Requirements

| Extension | PostgreSQL Version | Notes |
|-----------|-------------------|-------|
| PostGIS | 9.2+ | Recommended 3.0+ |
| pgvector | 12+ | Recommended 0.5.0+ |
| pg_trgm | 8.3+ | |
| hstore | 8.3+ | |
| ltree | 8.3+ | |
| uuid-ossp | 8.3+ | |
| pgcrypto | 8.3+ | |
| pg_partman | 9.4+ | Recommended 4.0+ |
| pg_cron | 9.5+ | |
| pg_stat_statements | 8.4+ | |
| pgaudit | 9.5+ | |
| pglogical | 9.4+ | |

## See Also

- [PostgreSQL Field Types](./field_types.md) — ARRAY, JSONB, UUID types
- [Advanced Indexing](./indexing.md) — GIN, GiST, BRIN indexes
- [PostgreSQL Dialect](./dialect.md) — PostgreSQL-specific SQL syntax

💡 *AI Prompt:* "How do I use PostGIS for spatial queries in Python?"
