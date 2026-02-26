# rhosocial-activerecord-postgres ($\rho_{\mathbf{AR}\text{-postgres}}$)

[![PyPI version](https://badge.fury.io/py/rhosocial-activerecord-postgres.svg)](https://badge.fury.io/py/rhosocial-activerecord-postgres)
[![Python](https://img.shields.io/pypi/pyversions/rhosocial-activerecord-postgres.svg)](https://pypi.org/project/rhosocial-activerecord-postgres/)
[![Tests](https://github.com/rhosocial/python-activerecord-postgres/actions/workflows/test.yml/badge.svg)](https://github.com/rhosocial/python-activerecord-postgres/actions)
[![Coverage Status](https://codecov.io/gh/rhosocial/python-activerecord-postgres/branch/main/graph/badge.svg)](https://app.codecov.io/gh/rhosocial/python-activerecord-postgres/tree/main)
[![Apache 2.0 License](https://img.shields.io/github/license/rhosocial/python-activerecord-postgres.svg)](https://github.com/rhosocial/python-activerecord-postgres/blob/main/LICENSE)
[![Powered by vistart](https://img.shields.io/badge/Powered_by-vistart-blue.svg)](https://github.com/vistart)

<div align="center">
    <img src="https://raw.githubusercontent.com/rhosocial/python-activerecord/main/docs/images/logo.svg" alt="rhosocial ActiveRecord Logo" width="200"/>
    <h3>PostgreSQL Backend for rhosocial-activerecord</h3>
    <p><b>Native Array & JSONB Support · Advanced Types · Sync & Async</b></p>
</div>

> **Note**: This is a backend implementation for [rhosocial-activerecord](https://github.com/rhosocial/python-activerecord). It cannot be used standalone.

## Why This Backend?

### 1. PostgreSQL's Unique Type System

| Feature | PostgreSQL Backend | MySQL Backend | SQLite Backend |
|---------|-------------------|---------------|----------------|
| **Native Arrays** | ✅ `INTEGER[]`, `TEXT[]` | ❌ Serialize to string | ❌ Serialize to string |
| **JSONB** | ✅ Binary JSON, indexed | ✅ JSON (text-based) | ⚠️ JSON1 extension |
| **UUID** | ✅ Native type | ⚠️ CHAR(36) | ⚠️ TEXT |
| **Range Types** | ✅ `DATERANGE`, `INT4RANGE` | ❌ | ❌ |
| **RETURNING** | ✅ All operations | ❌ | ✅ |

### 2. No Adapter Overhead for Arrays

Unlike databases without native arrays, PostgreSQL stores and queries arrays directly:

```python
# PostgreSQL: Native array - no serialization needed
class Article(ActiveRecord):
    tags: list[str]  # Maps directly to TEXT[]

# MySQL/SQLite: Requires adapter
class Article(ActiveRecord):
    tags: Annotated[list[str], UseAdapter(ListToStringAdapter(), str)]
```

### 3. Powerful Array Operators

Query arrays with native PostgreSQL operators:

```python
# Contains
Article.query().where("tags @> ARRAY[?]", ('python',)).all()

# Overlaps (any element matches)
Article.query().where("tags && ARRAY[?, ?]", ('python', 'database')).all()

# Any element equals
Article.query().where("? = ANY(tags)", ('python',)).all()
```

> 💡 **AI Prompt**: "Show me all PostgreSQL array operators with examples"

## Quick Start

### Installation

```bash
pip install rhosocial-activerecord-postgres
```

### Basic Usage

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from typing import Optional
from uuid import UUID

class User(ActiveRecord):
    __table_name__ = "users"
    id: Optional[UUID] = None  # Native UUID type
    name: str
    tags: list[str]  # Native array type
    metadata: dict  # JSONB type

# Configure
config = PostgresConnectionConfig(
    host="localhost",
    port=5432,
    database="myapp",
    username="user",
    password="password"
)
User.configure(config, PostgresBackend)

# Use
user = User(name="Alice", tags=["python", "postgres"])
user.save()

# Query with array operators
python_users = User.query().where("tags @> ARRAY[?]", ('python',)).all()
```

> 💡 **AI Prompt**: "How do I configure connection pooling for PostgreSQL?"

## PostgreSQL-Specific Features

### Array Types

Native array support without adapters:

```python
class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list[str]      # TEXT[]
    scores: list[int]    # INTEGER[]

# Query with array operators
Article.query().where("tags @> ARRAY[?]", ('python',)).all()
Article.query().where("? = ANY(tags)", ('database',)).all()
Article.query().where("array_length(tags, 1) > ?", (3,)).all()
```

> See [Array Type Comparison](docs/en_US/type_adapters/array_comparison.md) for differences with other databases.

### JSONB Operations

Binary JSON with indexing support:

```python
class Product(ActiveRecord):
    __table_name__ = "products"
    name: str
    attributes: dict  # JSONB

# Query JSONB
Product.query().where("attributes->>'brand' = ?", ("Dell",)).all()

# JSONB contains (@>)
Product.query().where("attributes @> ?", ('{"brand": "Dell"}',)).all()
```

### UUID Type

Native UUID storage and querying:

```python
from uuid import UUID, uuid4

class User(ActiveRecord):
    __table_name__ = "users"
    id: UUID
    name: str

user = User(id=uuid4(), name="Alice")
user.save()
```

### Range Types

Built-in support for range types:

```python
from datetime import date

class Booking(ActiveRecord):
    __table_name__ = "bookings"
    room_id: int
    date_range: tuple  # DATERANGE

# Query with range operators
Booking.query().where("date_range @> ?", (date(2024, 1, 15),)).all()
```

### RETURNING Clause

Retrieve data after INSERT/UPDATE/DELETE:

```python
# INSERT with RETURNING
user = User(name="Alice")
user.save()
print(user.id)  # Populated automatically via RETURNING

# Works for all operations (unlike MySQL)
```

## Requirements

- **Python**: 3.8+ (including 3.13t/3.14t free-threaded builds)
- **Core**: `rhosocial-activerecord>=1.0.0`
- **Driver**: `psycopg>=3.2.12`

## PostgreSQL Version Compatibility

| Feature | Min Version | Notes |
|---------|-------------|-------|
| Basic operations | 8.0+ | Core functionality |
| CTEs | 8.4+ | WITH clauses |
| Window functions | 8.4+ | ROW_NUMBER, RANK, etc. |
| RETURNING | 8.2+ | INSERT/UPDATE/DELETE RETURNING |
| JSON | 9.2+ | Basic JSON support |
| JSONB | 9.4+ | Binary JSON, indexed |
| UPSERT | 9.5+ | INSERT ... ON CONFLICT |

**Recommended**: PostgreSQL 12+ for optimal feature support.

## Get Started with AI Code Agents

This project supports AI-assisted development:

```bash
git clone https://github.com/rhosocial/python-activerecord-postgres.git
cd python-activerecord-postgres
```

### Example AI Prompts

- "How do I use array operators to query tags?"
- "What's the difference between JSON and JSONB in PostgreSQL?"
- "Show me how to create a GIN index on an array column"
- "How do I use range types for scheduling?"

### For Any LLM

Feed the documentation files in `docs/` for context-aware assistance.

## Testing

> ⚠️ **CRITICAL**: Tests MUST run serially. Do NOT use `pytest -n auto` or parallel execution.

```bash
# Run all tests
PYTHONPATH=src pytest tests/

# Run specific feature tests
PYTHONPATH=src pytest tests/rhosocial/activerecord_postgres_test/feature/basic/
PYTHONPATH=src pytest tests/rhosocial/activerecord_postgres_test/feature/query/
```

See the [Testing Documentation](https://github.com/rhosocial/python-activerecord/blob/main/.claude/testing.md) for details.

## Documentation

- **[Getting Started](docs/en_US/getting_started/)** — Installation and configuration
- **[PostgreSQL Features](docs/en_US/postgres_specific_features/)** — PostgreSQL-specific capabilities
- **[Type Adapters](docs/en_US/type_adapters/)** — Data type handling
- **[Array Comparison](docs/en_US/type_adapters/array_comparison.md)** — Array support across databases
- **[Transaction Support](docs/en_US/transaction_support/)** — Transaction management

## Comparison with Other Backends

| Feature | PostgreSQL | MySQL | SQLite |
|---------|------------|-------|--------|
| **Native Arrays** | ✅ | ❌ | ❌ |
| **JSONB (indexed)** | ✅ | ⚠️ JSON only | ⚠️ Extension |
| **UUID Type** | ✅ Native | ⚠️ CHAR(36) | ⚠️ TEXT |
| **Range Types** | ✅ | ❌ | ❌ |
| **RETURNING** | ✅ | ❌ | ✅ |
| **CTEs** | ✅ 8.4+ | ✅ 8.0+ | ✅ 3.8.3+ |
| **Full-Text Search** | ✅ | ✅ | ⚠️ FTS5 |

> 💡 **AI Prompt**: "When should I choose PostgreSQL over MySQL for my project?"

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

[Apache License 2.0](LICENSE) — Copyright © 2026 [vistart](https://github.com/vistart)

---

<div align="center">
    <p><b>Built with ❤️ by the rhosocial team</b></p>
    <p><a href="https://github.com/rhosocial/python-activerecord-postgres">GitHub</a> · <a href="https://docs.python-activerecord.dev.rho.social/backends/postgres.html">Documentation</a> · <a href="https://pypi.org/project/rhosocial-activerecord-postgres/">PyPI</a></p>
</div>
