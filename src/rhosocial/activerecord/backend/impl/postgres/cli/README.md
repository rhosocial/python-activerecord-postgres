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

| Type | Reads | NAME |
| :--- | :--- | :--- |
| `tables` | `information_schema.tables` | Refused. `--include-system` adds the system catalogues. |
| `views` | `information_schema.views` | Optional: one view, with its definition. |
| `columns` | `information_schema.columns` | Required: the table. Name, type, nullability, default. |
| `indexes` | `pg_index`, `pg_class`, `pg_attribute` | Required: the table. Includes the primary key. |
| `foreign-keys` | `information_schema.*` | Required: the table. The columns each side names. |
| `triggers` | `information_schema.triggers` | Optional: narrowed to one table. |
| `database` | `information_schema.schemata`, `pg_database` | Refused: it describes the connected database. |
| `extensions` | `pg_extension` | Optional: one extension, with version and comment. |
| `materialized-views` | `pg_matviews`, `pg_class`, `pg_namespace` | Optional: one view; carries the definition and whether a unique index backs `REFRESH … CONCURRENTLY`. |
| `sequences` | `information_schema.sequences`, `pg_sequences` | Optional: one sequence. `last_value` and `cache_size` come from `pg_sequences`, which arrived in PostgreSQL 10; on an older server both are reported as absent rather than invented. |

`NAME` means the object the type answers for, and each type has exactly one
answer to whether it takes one. `columns`, `indexes` and `foreign-keys` name
the table they belong to and require it; `triggers` accepts a table name and
lists every trigger when it gets none; `views`, `materialized-views`,
`sequences` and `extensions` accept an object name and list the family when
they get none. `tables` and `database` take no name: a name there is an error
rather than a filter, so a question aimed at one object cannot quietly return
the whole family.

A run exits `1` when it could not finish -- the connection failed, a query
failed, or the object a name asked for is not there -- and `2` when the
request cannot be served here: an unknown type, a name the type does not take,
or a family this server cannot answer. An empty listing is a successful
answer to a family question, never an error.

## Options

| Option | Default | Description |
| :--- | :--- | :--- |
| `-o, --output` | `table` | `table` (rich), `json`, `csv`, `tsv`. |
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
*   **The connection fixes the scope.** A run names one database, and that is
    the scope of the answer; the catalogue views are asked about the
    connection's default schema rather than resolving names through the
    server's search path, which differs between roles. There is no separate
    schema flag to pass.
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
python -m rhosocial.activerecord.backend.impl.postgres introspect columns orders --database mydb

# Views, and the one an analytics job reads
python -m rhosocial.activerecord.backend.impl.postgres introspect views --database mydb
python -m rhosocial.activerecord.backend.impl.postgres introspect views active_orders --database mydb -o json

# Extensions installed in this database, or just the one a feature needs
python -m rhosocial.activerecord.backend.impl.postgres introspect extensions --database mydb
python -m rhosocial.activerecord.backend.impl.postgres introspect extensions pgcrypto --database mydb

# Sequences, and the one sequence that hands out order ids
python -m rhosocial.activerecord.backend.impl.postgres introspect sequences --database mydb
python -m rhosocial.activerecord.backend.impl.postgres introspect sequences order_id_seq --database mydb -o json

# Materialized views, whose definitions are not in information_schema
python -m rhosocial.activerecord.backend.impl.postgres introspect materialized-views --database mydb

# A script asks the question and reads the answer
export POSTGRES_HOST=localhost POSTGRES_DATABASE=mydb POSTGRES_USER=postgres POSTGRES_PASSWORD=secret
python -m rhosocial.activerecord.backend.impl.postgres introspect tables -o json | jq '.[].name'
```
