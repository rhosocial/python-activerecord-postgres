# Installation Guide

## Requirements

- Python 3.8 or higher
- PostgreSQL 8.0 or higher (12+ recommended)
- psycopg 3.2.12 or higher

## Installation

### Using pip

```bash
pip install rhosocial-activerecord-postgres
```

### With optional dependencies

```bash
# For connection pooling support
pip install rhosocial-activerecord-postgres[pooling]

# For development
pip install rhosocial-activerecord-postgres[test,dev,docs]
```

## Verifying Installation

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

print("PostgreSQL backend installed successfully!")
```

## psycopg vs psycopg-binary

The package uses `psycopg` (psycopg3) as the PostgreSQL adapter. You can optionally install `psycopg-binary` for pre-compiled binaries:

```bash
pip install psycopg-binary
```

**Note**: `psycopg-binary` is platform-specific. If unavailable for your platform, psycopg will compile from source automatically.

💡 *AI Prompt:* "What are the performance differences between psycopg and psycopg-binary?"
