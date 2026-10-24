# `introspect` — PostgreSQL metadata, from the command line

The `introspect` subcommand asks a live PostgreSQL server what it holds, and
prints the answer. Every question runs through the backend's introspector, so
what the CLI reports is what the library sees — the same objects, the same
types, the same parsing — rather than a second implementation that can drift
from it.

```bash
python -m rhosocial.activerecord.backend.impl.postgres introspect <TYPE> [NAME] [OPTIONS]
```

## What it can ask about

| Type | Reads | Notes |
| :--- | :--- | :--- |
| `tables` | `information_schema.tables` | Base tables. `--include-system` adds the system catalogues. |
| `views` | `information_schema.views` | Ordinary views, with their definitions. |
| `table` | several | One table by name, with its columns, indexes and foreign keys filled in. |
| `columns` | `information_schema.columns` | One table's columns: name, type, nullability, default. |
| `indexes` | `pg_index`, `pg_class`, `pg_attribute` | One table's indexes, including the primary key. |
| `foreign-keys` | `information_schema.*` | One table's constraints and the columns each side names. |
| `triggers` | `information_schema.triggers` | Triggers, optionally narrowed to one table. |
| `database` | `information_schema.schemata`, `pg_database` | Server version, charset, and the databases visible to this role. |
| `extensions` | `pg_extension` | Installed extensions with version and comment. |
| `materialized-views` | `pg_matviews`, `pg_class`, `pg_namespace` | List, or one by name; carries the definition and whether a unique index backs `REFRESH … CONCURRENTLY`. |
| `sequences` | `information_schema.sequences`, `pg_sequences` | List, or one by name. `last_value` and `cache_size` come from `pg_sequences`, which arrived in PostgreSQL 10; on an older server both are reported as absent rather than invented. |

`NAME` is required for `table`, `columns`, `indexes` and `foreign-keys`. It is
optional for `materialized-views` and `sequences`, where it selects one object
instead of the family.

## Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `-o, --output` | `table` | `table` (rich), `json`, `csv`, `tsv`. |
| `--schema` | `public` | The schema to read. Fewer surprises than a search path. |
| `--include-system` | off | Include system objects. |
| `--host`, `--port`, `--database`, `--user`, `--password` | env or defaults | Connection. `POSTGRES_*` env vars are honoured, and arguments override them. |
| `--ssl` | off | Use SSL for the connection. |
| `--async` | off | Drive the asynchronous backend. |
| `--named-connection` | _None_ | A connection saved by the `named-connection` subcommand. |
| `--conn-param` | _None_ | Extra `key=value` connection parameters; repeatable. |
| `--rich-ascii` | off | ASCII characters for table borders. |

## Notes

*   **Read-only.** `introspect` never writes. It does not create, drop, or
    alter anything, so it is safe to run against a database whose schema
    matters.
*   **The schema is explicit.** `--schema` defaults to `public`; the catalogue
    views are then asked about that schema, rather than resolving names
    through the server's search path, which differs between roles.
*   **One question per invocation.** A CLI process is one connection that
    opens, answers, and closes — there is no cache carried between runs, so
    the answer reflects the server as it is *now*, including a change made by
    someone else since the last run.
*   **JSON is the machine-readable answer.** `-o json` emits the typed result
    objects as they are, so a script reads fields rather than parsing a
    rendered table.

## Examples

```bash
# Everything in the default schema, one table at a time
python -m rhosocial.activerecord.backend.impl.postgres introspect tables --database mydb
python -m rhosocial.activerecord.backend.impl.postgres introspect table orders --database mydb

# Sequences, and the one sequence that hands out order ids
python -m rhosocial.activerecord.backend.impl.postgres introspect sequences --database mydb
python -m rhosocial.activerecord.backend.impl.postgres introspect sequences order_id_seq --database mydb -o json

# Another schema, including its materialized views
python -m rhosocial.activerecord.backend.impl.postgres introspect materialized-views --schema reporting --database mydb

# A script asks the question and reads the answer
export POSTGRES_HOST=localhost POSTGRES_DATABASE=mydb POSTGRES_USER=postgres POSTGRES_PASSWORD=secret
python -m rhosocial.activerecord.backend.impl.postgres introspect tables -o json | jq '.[].name'
```
