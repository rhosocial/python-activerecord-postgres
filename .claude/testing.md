# Testing Guide - python-activerecord-postgres

> AI Assistant Note: This document covers PostgreSQL backend-specific testing requirements.

## Project-Specific Information

| Item | Value |
|------|-------|
| **Python Version** | 3.8+ |
| **Database Driver** | psycopg (version 3) |
| **Free-Threading Support** | ❌ No |

## ⚠️ Important Limitation

**psycopg does NOT support free-threaded Python (Python 3.13t, 3.14t).**

This backend CANNOT be used with free-threaded Python builds. Free-threaded tests are not included.

## Dependencies

```toml
dependencies = [
    "psycopg[binary]",
    "rhosocial-activerecord"
]
```

## Quick Test Commands

```bash
# Activate virtual environment and set PYTHONPATH
cd /mnt/i/GitHubRepositories/rhosocial/python-activerecord-postgres
source .venv/bin/activate
export PYTHONPATH=src

# Run tests
pytest

# Run specific test directory
pytest tests/rhosocial/activerecord_postgres_test/feature/basic/
```

## Backend-Specific Test Markers

```python
markers = [
    "requires_functions: marks tests requiring specific database functions",
]
```

## Key Differences from Core

- Uses PostgreSQL-specific dialect in `src/rhosocial/activerecord/backend/impl/postgres/dialect.py`
- Schema files in `tests/rhosocial/activerecord_postgres_test/feature/basic/schema/`
- Provider implementation in `tests/providers/`
- Supports PostgreSQL-specific features: array types, JSONB, range types, PostGIS, etc.

## Reference

- [Core testing guide](../python-activerecord/.claude/testing.md)
- [PostgreSQL backend development](../python-activerecord/.claude/backend_development.md)