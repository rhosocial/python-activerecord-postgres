# PostgreSQL Migration Examples

Runnable examples for migrations **on this PostgreSQL backend**. They cover only this backend's
concerns (configuration, transactional DDL, version-gated types, and CLI wiring); the migration
system itself is documented in the
[core migration chapter](https://github.com/rhosocial/python-activerecord/blob/main/docs/en_US/migration/README.md)
and summarised in the [backend migration guide](../../en_US/migration/README.md).

All scripts need a reachable PostgreSQL server. Configure it with the `POSTGRES_*` environment
variables (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`) and run from the repository root with `PYTHONPATH=src`:

```bash
PYTHONPATH=src python docs/examples/migration/01_transactional_ddl.py
PYTHONPATH=src python docs/examples/migration/02_named_migration.py
```

| Script | Demonstrates |
|---|---|
| `01_transactional_ddl.py` | `PostgresConnectionConfig` + `PostgresBackend`; `introspect_and_adapt()`; `@migration` + `ctx.apply()`; `run_migration_group` dry run → UP → DOWN; dialect's `supports_transactional_ddl()` and `MigrationFunctionExecutionError.transactional_ddl`; a PostgreSQL 14+ `INT4MULTIRANGE` type gated by `version_at_least()` |
| `02_named_migration.py` | class-style `NamedMigration` via `MigrationRunner` with the PostgreSQL backend; class-style dry-run SQL capture; equivalent `named-migration` CLI commands |

> The longer-running class-style demos (single migration, dependency chain, batch, async, parameters)
> live in
> `src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/`.
