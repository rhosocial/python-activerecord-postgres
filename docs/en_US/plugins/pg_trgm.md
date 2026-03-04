# pg_trgm

pg_trgm provides trigram similarity search functionality for PostgreSQL.

## Overview

pg_trgm provides:
- Text similarity calculation
- Fuzzy search
- Similarity indexing

💡 *AI Prompt:* "What is pg_trgm and what text search problems can it solve?"

## Installation

```sql
CREATE EXTENSION pg_trgm;
```

### Built-in Extension

pg_trgm is a PostgreSQL contrib module and is usually installed with PostgreSQL. If not installed:

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/pg_trgm.control": 
No such file or directory
```

Install the contrib package:

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-contrib-{version}
```

## Version Support

| pg_trgm Version | PostgreSQL Version | Status |
|-----------------|-------------------|--------|
| 1.6 | 17+ | ✅ Supported |
| 1.5 | 14-16 | ✅ Plugin support guaranteed |
| 1.3 | 9.6-13 | ✅ Supported |
| 1.2 | 9.4-9.5 | ⚠️ Limited support |

💡 *AI Prompt:* "What are the differences between pg_trgm versions 1.2 and 1.6? What new features were added?"

## Feature Detection

```python
if backend.dialect.is_extension_installed('pg_trgm'):
    print("pg_trgm is installed")

if backend.dialect.supports_trigram_similarity():
    # Trigram similarity is supported
    pass
```

💡 *AI Prompt:* "How to detect if pg_trgm is available in Python?"

## Similarity Functions

```sql
-- Calculate similarity (0-1)
SELECT similarity('hello', 'hallo');

-- Show trigrams
SELECT show_trgm('hello');

-- Similarity threshold query
SELECT * FROM users
WHERE name % 'John'
ORDER BY similarity(name, 'John') DESC;
```

## Index Support

```sql
-- GiST index
CREATE INDEX idx_users_name_trgm ON users USING GIST(name gist_trgm_ops);

-- GIN index
CREATE INDEX idx_users_name_trgm ON users USING GIN(name gin_trgm_ops);
```

## Notes

1. GiST indexes are suitable for dynamic data, GIN indexes for static data
2. Similarity threshold can be adjusted via `pg_trgm.similarity_threshold`
3. Trigram indexes increase storage space

💡 *AI Prompt:* "What are the performance differences between GiST and GIN indexes for text search in pg_trgm?"

💡 *AI Prompt:* "How to implement efficient fuzzy search using pg_trgm?"

## Related Topics

- **[hstore](./hstore.md)**: Key-value storage
- **[Plugin Support](./README.md)**: Plugin detection mechanism
