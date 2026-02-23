# Supported Versions

## PostgreSQL Version Support

| PostgreSQL Version | Support Status | Notes |
|-------------------|----------------|-------|
| 17.x | ✅ Full Support | Latest version |
| 16.x | ✅ Full Support | Current LTS |
| 15.x | ✅ Full Support | |
| 14.x | ✅ Full Support | |
| 13.x | ✅ Full Support | |
| 12.x | ✅ Full Support | Minimum recommended |
| 11.x | ⚠️ Limited Support | Some features unavailable |
| 10.x and below | ❌ Not Supported | |

## Feature Availability by PostgreSQL Version

### Basic Features (PostgreSQL 8.0+)
- Basic CRUD operations
- Simple queries
- Transaction support

### Advanced Features

| Feature | Minimum Version |
|---------|-----------------|
| CTEs (WITH clause) | 8.4+ |
| Window functions | 8.4+ |
| RETURNING clause | 8.2+ |
| JSON type | 9.2+ |
| JSONB type | 9.4+ |
| UPSERT (ON CONFLICT) | 9.5+ |
| Parallel query | 9.6+ |
| Stored generated columns | 12+ |
| JSON path queries | 12+ |

## Python Version Support

| Python Version | Support Status |
|----------------|----------------|
| 3.14 | ✅ Full Support |
| 3.14t (free-threaded) | ✅ Full Support |
| 3.13 | ✅ Full Support |
| 3.13t (free-threaded) | ✅ Full Support |
| 3.12 | ✅ Full Support |
| 3.11 | ✅ Full Support |
| 3.10 | ✅ Full Support |
| 3.9 | ✅ Full Support |
| 3.8 | ✅ Full Support |

## Driver Dependencies

| Package | Minimum Version | Notes |
|---------|-----------------|-------|
| psycopg | 3.2.12+ | PostgreSQL adapter for Python |
| psycopg-binary | 3.2.12+ | Pre-compiled binary (optional) |

## Core Library Dependencies

| Package | Version |
|---------|---------|
| rhosocial-activerecord | >=1.0.0,<2.0.0 |

💡 *AI Prompt:* "What are the key differences between psycopg2 and psycopg3?"
