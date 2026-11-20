<!-- README.md -->
# rhosocial-activerecord-postgres ($\rho_{\mathbf{AR}\text{-postgres}}$)

[![PyPI version](https://badge.fury.io/py/rhosocial-activerecord-postgres.svg)](https://badge.fury.io/py/rhosocial-activerecord-postgres)
[![Python](https://img.shields.io/pypi/pyversions/rhosocial-activerecord-postgres.svg)](https://pypi.org/project/rhosocial-activerecord-postgres/)
[![Tests](https://github.com/rhosocial/python-activerecord-postgres/actions/workflows/test.yml/badge.svg)](https://github.com/rhosocial/python-activerecord-postgres/actions)
[![Coverage Status](https://codecov.io/gh/rhosocial/python-activerecord-postgres/branch/main/graph/badge.svg)](https://app.codecov.io/gh/rhosocial/python-activerecord-postgres/tree/main)
[![Supported PostgreSQL versions](https://img.shields.io/badge/PostgreSQL-9.6%7C10%7C11%7C12%7C13%7C14%7C15%7C16%7C17%7C18%7C19-blue?logo=postgresql)](docs/en_US/introduction/supported_versions.md)
[![Apache 2.0 License](https://img.shields.io/github/license/rhosocial/python-activerecord-postgres.svg)](https://github.com/rhosocial/python-activerecord-postgres/blob/main/LICENSE)

<div align="center">
    <img src="https://raw.githubusercontent.com/rhosocial/python-activerecord/main/docs/images/logo.svg" alt="rhosocial ActiveRecord Logo" width="200"/>
    <h3>PostgreSQL Backend for rhosocial-activerecord</h3>
    <p><b>Native arrays &amp; JSONB · Range types · DDL expressions · Sync &amp; async</b></p>
</div>

This is a PostgreSQL backend implementation for [rhosocial-activerecord](https://github.com/rhosocial/python-activerecord). It cannot be used standalone.

**Keywords:** orm, activerecord, postgresql, psycopg, pydantic, python, async, type-safe

## Why This Project?

PostgreSQL is a mature, standards-compliant database with a rich type system — native arrays, JSONB, range types, UUID, full-text search, and more. This backend brings the familiar, intuitive ActiveRecord experience to PostgreSQL without trading any of that away.

- **One consistent model API.** A class represents a table; an instance represents a row. No Session object to manage — the same typed models, field mixins, and query expressions used throughout rhosocial-activerecord, now backed by PostgreSQL.
- **PostgreSQL-native SQL, not a lowest common denominator.** The dialect renders expressions as real PostgreSQL SQL with version-aware capability gating, so native features such as array operators, JSONB containment, ranges, advanced indexes, and `RETURNING` are available when your server supports them.
- **Sync and async parity.** `PostgresBackend` and `AsyncPostgresBackend` expose the same method names and patterns; the async variant just adds `await`.

## Quick Start

### Installation

```bash
pip install rhosocial-activerecord-postgres
```

Requires Python 3.8+ and a reachable PostgreSQL server (9.6+); the example below uses the synchronous `PostgresBackend` with `psycopg`.

### End-to-End Example

```python
"""Save as demo.py and run with: python demo.py"""
from typing import Annotated, ClassVar

from rhosocial.activerecord.base import FieldProxy, UseSqlType
from rhosocial.activerecord.backend.expression.types import VarCharType
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.field import DefaultTimestampMixin, IntegerPKMixin
from rhosocial.activerecord.model import ActiveRecord


class User(
    IntegerPKMixin,         # auto-increment integer primary key (`id`)
    DefaultTimestampMixin,  # injects `created_at` / `updated_at` timestamps
    ActiveRecord,           # persistence, query builder, relations
):
    name: Annotated[str, UseSqlType(VarCharType(length=100))]
    email: Annotated[str, UseSqlType(VarCharType(length=255))]
    age: int = 0

    c: ClassVar[FieldProxy] = FieldProxy()  # enables User.c.<field> expressions

    @classmethod
    def table_name(cls) -> str:
        return "users"


# Configure the PostgreSQL backend and create the table from the model declaration
User.configure(
    PostgresConnectionConfig(
        host="localhost",
        port=5432,
        database="myapp",
        username="user",
        password="password",
    ),
    PostgresBackend,
)
User.create_table()

# Create
alice = User(name="Alice", email="alice@example.com", age=30)
alice.save()
print(alice.id)  # database-assigned integer primary key

# Query with type-safe expressions
adults = User.query().where(User.c.age >= 18).all()
print([user.name for user in adults])

# Inspect the generated SQL without executing it
sql, params = User.query().where(User.c.age >= 18).to_sql()
print(sql, params)  # SELECT ... FROM "users" WHERE "users"."age" >= %s   (18,)
```

> **Async parity:** use `AsyncActiveRecord` with `AsyncPostgresBackend` for the async model — same method names, same patterns, just add `await`.

## Project Architecture

This package plugs into the rhosocial-activerecord stack as a single layer: models and query expressions come from the core library, the PostgreSQL dialect binds them to PostgreSQL SQL, and psycopg 3 carries the statements to the server. Both the synchronous (`PostgresBackend`) and asynchronous (`AsyncPostgresBackend`) implementations share the same API.

For the full picture, start with the [Documentation](docs/en_US/README.md).

**PostgreSQL highlights.** Native arrays, JSONB, UUID, range and geometric types, full-text search, advanced indexing (GIN / GiST / BRIN), and version-gated `RETURNING`. Connection settings live in `PostgresConnectionConfig`; connection management follows a connect-on-use pattern.

## Backend Support & Progress

PostgreSQL 9.6 is the minimum tested server version; releases through 19 are supported. See [Supported Versions](docs/en_US/introduction/supported_versions.md) for the full matrix and for which features are version-gated. Requires Python 3.8+ and `psycopg[binary]` 3.2.13+.

This backend ships as a separate package on top of `rhosocial-activerecord`; the core library provides the framework and this package provides the PostgreSQL implementation.

> **⚠️ Development Stage:** This project is under active development. APIs may change, and some features are not yet production-ready.

## Documentation

### Contributing

We welcome contributions! Please see [CONTRIBUTING.md](https://github.com/rhosocial/python-activerecord/blob/main/CONTRIBUTING.md) for guidelines.

### License

[Apache License 2.0](LICENSE) — Copyright © 2026 [vistart](https://github.com/vistart)

### Documentation

- **[Documentation Home](docs/en_US/README.md)** — the full documentation table of contents
- **[Supported Versions](docs/en_US/introduction/supported_versions.md)** — PostgreSQL and Python version matrix
- **[Getting started](docs/en_US/installation_and_configuration/)** — installation, connection configuration, SSL, encoding
- **[PostgreSQL features](docs/en_US/postgres_specific_features/)** — PostgreSQL-specific capabilities
- **[Type adapters](docs/en_US/type_adapters/)** — type conversion rules and array comparison
- **[Transactions](docs/en_US/transaction_support/)** — isolation levels and savepoints
- **[Troubleshooting](docs/en_US/troubleshooting/)** — connection, performance and encoding issues

---

<div align="center">
    <p><b>Built with ❤️ by the rhosocial team</b></p>
    <p><a href="https://github.com/rhosocial/python-activerecord-postgres">GitHub</a> · <a href="docs/en_US/README.md">Documentation</a> · <a href="https://pypi.org/project/rhosocial-activerecord-postgres/">PyPI</a></p>
</div>
