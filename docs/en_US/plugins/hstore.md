# hstore

hstore provides key-value pair storage functionality for PostgreSQL.

## Overview

hstore provides:
- **hstore** data type
- Key-value operators
- Index support

💡 *AI Prompt:* "What is hstore and when should you use it?"

## Installation

```sql
CREATE EXTENSION hstore;
```

### Built-in Extension

hstore is a PostgreSQL contrib module and is usually installed with PostgreSQL. If not installed:

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/hstore.control": 
No such file or directory
```

Install the contrib package:

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-contrib-{version}
```

## Version Support

| hstore Version | PostgreSQL Version | Status |
|---------------|-------------------|--------|
| 1.8 | 17+ | ✅ Supported |
| 1.6 | 14-16 | ✅ Plugin support guaranteed |
| 1.4 | 9.6-13 | ✅ Supported |
| 1.3 | 9.4-9.5 | ⚠️ Limited support |

💡 *AI Prompt:* "What are the differences between hstore versions 1.3 and 1.8?"

## Feature Detection

```python
if backend.dialect.is_extension_installed('hstore'):
    print("hstore is installed")

if backend.dialect.supports_hstore_type():
    # hstore type is supported
    pass
```

💡 *AI Prompt:* "How to detect if hstore is available in Python?"

## Data Type

```sql
-- Create table with hstore column
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    attributes hstore
);

-- Insert data
INSERT INTO users (name, attributes)
VALUES ('John', '"age"=>"30", "city"=>"New York", "role"=>"admin"');
```

## Common Operations

```sql
-- Get value
SELECT attributes -> 'age' FROM users;

-- Contains key
SELECT * FROM users WHERE attributes ? 'city';

-- Contains key-value pair
SELECT * FROM users WHERE attributes @> '"city"=>"New York"';

-- Get all keys
SELECT akeys(attributes) FROM users;
```

## Index Support

```sql
-- GIN index (supports @>, ?, ?& operators)
CREATE INDEX idx_users_attrs ON users USING GIN(attributes);

-- GiST index
CREATE INDEX idx_users_attrs ON users USING GIST(attributes);
```

## hstore vs JSONB

| Feature | hstore | JSONB |
|---------|--------|-------|
| Value types | Strings only | Any JSON type |
| Nesting | Not supported | Supported |
| Indexing | GIN/GiST | GIN |
| Performance | Faster (simple structure) | More flexible |

💡 *AI Prompt:* "What are the pros and cons of hstore vs JSONB in PostgreSQL for key-value storage scenarios?"

💡 *AI Prompt:* "When should I choose hstore over JSONB?"

## Related Topics

- **[pg_trgm](./pg_trgm.md)**: Trigram similarity
- **[Plugin Support](./README.md)**: Plugin detection mechanism
