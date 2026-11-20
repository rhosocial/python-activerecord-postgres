# Migrations — PostgreSQL Backend

> **Scope:** This chapter covers only the PostgreSQL backend's migration-specific concerns:
> configuring a PostgreSQL backend for migrations, transactional DDL, version-gated features/types,
> and how the two migration APIs are wired to this backend. It does **not** re-explain the migration
> system itself — the two authoring APIs, ordering and dependency rules, version records, dry-run
> internals, the `migrate` CLI, and the exception hierarchy are all documented in the
> [core migration chapter](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md).
> Read that chapter first if you are new to migrations; come back here for PostgreSQL details.

> 💡 **AI Prompt:** "Show me a PostgreSQL migration that renders its DDL in a dry run, then applies it
> inside a transaction and rolls back cleanly if a later statement fails."

## 1. Configuring a PostgreSQL backend for migrations

Migrations run on a backend instance you supply. Build this backend's `PostgresConnectionConfig` and
`PostgresBackend`, then — importantly — call `introspect_and_adapt()` before running migrations. That
call detects the live server version and installed extensions so the dialect renders version-gated
DDL correctly (see §3).

```python
import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DATABASE", "test"),
    username=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()  # detect server version + installed extensions
```

Notes specific to this backend:

- `PostgresConnectionConfig` accepts PostgreSQL/libpq-style options (`sslmode`, `search_path`,
  `default_schema`, `application_name`, `target_session_attrs`, `connect_timeout`, `service`, …).
  `search_path` / `default_schema` are worth setting when migrations target a non-`public` schema.
  See [Connection configuration](../installation_and_configuration/configuration.md).
- The connection runs with driver-level autocommit; transaction boundaries are managed explicitly by
  the framework. You do **not** need to open a transaction yourself around a migration group —
  `run_migration_group` does it (§2).
- `introspect_and_adapt()` falls back to a default version when the server cannot be queried. If a
  migration needs a specific server version, check it explicitly rather than assuming (§3).

## 2. Transactional DDL — PostgreSQL's key advantage

**PostgreSQL DDL is transactional.** Schema statements such as `CREATE TABLE`, `ALTER TABLE`, and
`DROP TABLE` participate in a transaction and can be **rolled back cleanly**. This is a genuine
advantage over backends whose DDL is *non-transactional*: on those, a failure part-way through a
migration leaves a partially applied schema that must be repaired by hand. On PostgreSQL the same
failure leaves the schema exactly as it was.

How the system uses this:

- This backend's dialect declares `supports_transactional_ddl() -> True`. The
  `run_migration_group` path probes `backend.dialect.supports_transactional_ddl()` and records the
  answer on `MigrationFunctionExecutionError.transactional_ddl` when a function fails.
- With the default `TransactionMode.AUTO`, a group begins one transaction, runs every function in
  the group, and commits only if all succeed. Any exception rolls the whole group back — DDL
  included. A dry run never begins a transaction or writes anything.
- Consequently a multi-statement migration on PostgreSQL is effectively **all-or-nothing**: you can
  write `up()`/`down()` functions as a sequence of DDL steps without hand-rolled compensation for a
  failed step.

```python
from rhosocial.activerecord.backend.migration import (
    MigrationFunctionExecutionError,
    run_migration_group,
)

assert backend.dialect.supports_transactional_ddl() is True

run_migration_group(backend, [user_fqn], dry_run=True)      # renders SQL, writes nothing
run_migration_group(backend, [user_fqn], record_store=store)  # one transaction, then commit

try:
    run_migration_group(backend, [failing_fqn], record_store=store)
except MigrationFunctionExecutionError as exc:
    assert exc.transactional_ddl is True  # the failed DDL was rolled back
```

PostgreSQL-specific caveats that still apply:

- A few PostgreSQL statements cannot run inside a transaction block at all — notably
  `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, `REFRESH MATERIALIZED VIEW CONCURRENTLY`,
  and `CREATE DATABASE`/`DROP DATABASE`. Keep them out of a transactional migration group (run them
  with `transaction_mode=TransactionMode.NONE` from
  `rhosocial.activerecord.backend.named_expression`) and handle compensation yourself.
- DDL takes locks; concurrent migrations against the same relations can block or deadlock. Use the
  backend's [deadlock handling](../transaction_support/deadlock.md) guidance where appropriate.
- See [Transaction support](../transaction_support/README.md) for isolation levels, savepoints, and
  DEFERRABLE constraints.

## 3. Version-gated features and types

PostgreSQL features arrive at different server versions, and this backend renders only what the
target server supports. `introspect_and_adapt()` caches the live server version on the dialect;
migrations should gate version-specific DDL on it.

- `backend.dialect.version_at_least((major, minor, patch))` — safe predicates; returns `False` when
  the version is unknown (never raises). `backend.dialect.version_or_none` returns the tuple or
  `None`. `backend.get_server_version()` queries the server directly.
- Examples of gated capabilities: `JSONB` (9.4+), stored generated columns (12+), `multirange` types
  such as `PostgresInt4MultirangeType` (14+), `UNIQUE NULLS NOT DISTINCT` and `MERGE` (15+).
  The full table is in [Supported versions](../introduction/supported_versions.md), and this
  backend's version-specific types live under `rhosocial.activerecord.backend.impl.postgres.expression.types`.

```python
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresInt4MultirangeType,
)

def create_archive(ctx):  # decorated with @migration in a real module
    # Archive has a PostgresInt4MultirangeType column (PG 14+); skip on older servers.
    if not ctx.backend.dialect.version_at_least((14, 0, 0)):
        return
    ctx.apply(Archive.create_table_statement(backend=ctx.backend))
```

Always emit DDL through `ctx.apply(...)` so dry runs and version records behave correctly.

## 4. Function-style migrations (`run_migration_group`)

For new work, use `@migration` functions and `run_migration_group`, exactly as the core chapter
describes — only the backend differs. A complete, PostgreSQL-specific script is
[`docs/examples/migration/01_transactional_ddl.py`](../../examples/migration/01_transactional_ddl.py):
it creates a `PostgresBackend`, renders a migration with `dry_run=True`, applies it with a
`JSONFileMigrationRecordStore`, demonstrates a version-gated type, and shows the
`transactional_ddl` flag on a failure.

```bash
# from the repository root
PYTHONPATH=src python docs/examples/migration/01_transactional_ddl.py
```

A successful setup requires a reachable PostgreSQL server; configure it with the `POSTGRES_*`
environment variables. Ordering, dependency validation, and record-store semantics are the system's
business — see the core chapter, not this page.

## 5. Class-style `named-migration` CLI wiring

The class-style (`NamedMigration`) API is fully supported for existing migrations. This backend's
CLI exposes a `named-migration` subcommand whose handler lives in
[`cli/named_migration.py`](../../../src/rhosocial/activerecord/backend/impl/postgres/cli/named_migration.py)
and delegates to the shared core helper. It builds a `PostgresBackend` (or `AsyncPostgresBackend`
with `--async`) from the connection arguments and calls `introspect_and_adapt()` before running.

```bash
python -m rhosocial.activerecord.backend.impl.postgres named-migration \
    myapp.migrations.v001.CreateUsersTable --host localhost --database mydb --direction up \
    --record-store ./migrations.json
python -m rhosocial.activerecord.backend.impl.postgres named-migration \
    myapp.migrations.v001.CreateUsersTable --database mydb --direction up --dry-run
python -m rhosocial.activerecord.backend.impl.postgres named-migration \
    myapp.migrations --all --record-store ./migrations.json --single-transaction
```

`--direction up|down`, `--record-store`, `--dry-run`, `--describe`, `--list`, `--param KEY=VALUE`,
`--all`, `--single-transaction`, and `--async` are all supported. `--single-transaction` wraps a
whole batch in one transaction — which, because PostgreSQL DDL is transactional, genuinely rolls the
entire batch back on failure. The full end-to-end demos (single migration, dependency chain, batch,
async, parameters) are in
[`src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/`](../../../src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/)
and the CLI's own options are in the [CLI README](../../../src/rhosocial/activerecord/backend/impl/postgres/cli/README.md).

A programmatic class-style run is also available through `MigrationRunner`; see the second example,
[`docs/examples/migration/02_named_migration.py`](../../examples/migration/02_named_migration.py).

## 6. Where to go next

- Migration **system** (the authoritative guide):
  [core migration chapter](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md)
- Runnable examples: [local migration examples](../../examples/migration/README.md)
- PostgreSQL DDL statements: [DDL overview](../ddl/README.md)
- Server capabilities by version: [Supported versions](../introduction/supported_versions.md)
- Transaction behaviour: [Transaction support](../transaction_support/README.md)
