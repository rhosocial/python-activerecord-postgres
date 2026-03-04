# Supported Versions

## PostgreSQL Version Support

| PostgreSQL Version | Support Status | Notes |
|-------------------|----------------|-------|
| 18.x | ✅ Full Support | Latest version |
| 17.x | ✅ Full Support | |
| 16.x | ✅ Full Support | |
| 15.x | ✅ Full Support | |
| 14.x | ✅ Full Support | Plugin support guaranteed |
| 13.x | ✅ Full Support | |
| 12.x | ✅ Full Support | |
| 11.x | ✅ Full Support | |
| 10.x | ✅ Full Support | |
| 9.6 | ✅ Full Support | Minimum tested version |
| 9.5 and below | ⚠️ Not Tested | May work but untested |

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

| Python Version | Support Status | Notes |
|----------------|----------------|-------|
| 3.14 | ✅ Full Support | |
| 3.14t (free-threaded) | ❌ Not Supported | [psycopg issue #1095](https://github.com/psycopg/psycopg/issues/1095) |
| 3.13 | ✅ Full Support | |
| 3.13t (free-threaded) | ❌ Not Supported | [psycopg issue #1095](https://github.com/psycopg/psycopg/issues/1095) |
| 3.12 | ✅ Full Support | |
| 3.11 | ✅ Full Support | |
| 3.10 | ✅ Full Support | |
| 3.9 | ✅ Full Support | |
| 3.8 | ✅ Full Support | |

**Note:** Free-threaded Python versions are not supported because psycopg does not yet support them. See [GitHub issue #1095](https://github.com/psycopg/psycopg/issues/1095) for progress.

## Driver Dependencies

| Package | Version | Python Version | Notes |
|---------|---------|----------------|-------|
| psycopg | 3.2.x | 3.8+ | Supported |
| psycopg | 3.3.x | 3.10+ | Recommended |
| psycopg-binary | 3.2.x | 3.8+ | Pre-compiled binary |
| psycopg-binary | 3.3.x | 3.10+ | Recommended |

### psycopg Version Compatibility

| psycopg Version | Minimum Python | Maximum Python |
|-----------------|----------------|----------------|
| 3.2.x | 3.8 | All current |
| 3.3.x | 3.10 | All current |

## Core Library Dependencies

| Package | Version |
|---------|---------|
| rhosocial-activerecord | >=1.0.0,<2.0.0 |

💡 *AI Prompt:* "What are the key differences between psycopg2 and psycopg3?"
