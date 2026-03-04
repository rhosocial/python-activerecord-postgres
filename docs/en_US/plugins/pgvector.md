# pgvector

pgvector is a vector similarity search extension for PostgreSQL, designed for AI and machine learning applications.

## Overview

pgvector provides:
- **vector** data type
- Vector similarity search (`<->` operator)
- IVFFlat and HNSW indexes

💡 *AI Prompt:* "What is pgvector and what role does it play in AI applications?"

## Installation

```sql
CREATE EXTENSION vector;
```

### Error When Not Installed

If pgvector is not installed on the server, running `CREATE EXTENSION vector` will result in:

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/vector.control": 
No such file or directory
```

This indicates pgvector is not installed on the server. You need to install pgvector first:

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-{version}-pgvector

# Or build from source
git clone https://github.com/pgvector/pgvector.git
cd pgvector
make
sudo make install
```

### Permission Requirements

Installing extensions requires superuser privileges.

## Version Support

| pgvector Version | Features | Release Date |
|-----------------|----------|--------------|
| 0.8.x | Latest, more distance functions | 2024+ |
| 0.7.x | Improved HNSW index | 2024 |
| 0.6.x | Parallel index build | 2023 |
| 0.5.x | HNSW index | 2023 |
| 0.4.x | IVFFlat index improvements | 2023 |
| 0.1.x | Basic vector type | 2022 |

**Recommended Version**: 0.5.0+ (supports HNSW index)

💡 *AI Prompt:* "What are the main differences between pgvector 0.1 and the latest version 0.8? What should I consider when upgrading?"

## Feature Detection

```python
# Check if pgvector is installed
if backend.dialect.is_extension_installed('vector'):
    print("pgvector is installed")

# Get version
version = backend.dialect.get_extension_version('vector')
print(f"pgvector version: {version}")

# Check feature support
if backend.dialect.supports_vector_type():
    # vector type is supported
    pass

if backend.dialect.supports_hnsw_index():
    # HNSW index is supported (requires 0.5.0+)
    pass
```

💡 *AI Prompt:* "How to detect pgvector installation and version in Python?"

## Data Type

```sql
-- Create table with vector column
CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    content TEXT,
    embedding vector(1536)  -- OpenAI embedding dimension
);
```

## Vector Indexes

### HNSW Index (Recommended)

```sql
-- Create HNSW index (requires pgvector 0.5.0+)
CREATE INDEX idx_embeddings_hnsw ON embeddings 
USING hnsw (embedding vector_cosine_ops);
```

### IVFFlat Index

```sql
-- Create IVFFlat index
CREATE INDEX idx_embeddings_ivfflat ON embeddings 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 100);
```

## Similarity Search

```sql
-- Cosine similarity search (find top 5 most similar)
SELECT content, embedding <=> '[0.1, 0.2, ...]'::vector AS distance
FROM embeddings
ORDER BY embedding <=> '[0.1, 0.2, ...]'::vector
LIMIT 5;
```

## Notes

1. Maximum vector dimension: 2000
2. HNSW index requires pgvector 0.5.0+
3. Index creation may take time
4. Recommended to create indexes after data loading

💡 *AI Prompt:* "What are the differences between HNSW and IVFFlat indexes in pgvector? What are the use cases for each?"

💡 *AI Prompt:* "How to choose pgvector index parameters for optimal performance?"

## Related Topics

- **[PostGIS](./postgis.md)**: Spatial database functionality
- **[Plugin Support](./README.md)**: Plugin detection mechanism
