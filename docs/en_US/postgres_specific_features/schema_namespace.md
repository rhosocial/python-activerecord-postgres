# PostgreSQL Schema Namespaces

> PostgreSQL-specific behaviour of schema handling. For the model-level API
> (declaring `__schema_name__`, when the schema appears in SQL, DDL boundaries)
> see the core guide *Schema Namespaces*.

## Aliases win over schemas

A range written as `"shop"."orders"` may be referenced **either** two-part
(`"orders"."id"`) or three-part (`"shop"."orders"."id"`). A range written as
`"shop"."orders" AS "o"` may be referenced **only** as `"o"."id"`:

| Range | Column reference | Result |
|---|---|---|
| `FROM "shop"."orders"` | `"shop"."orders"."id"` | OK |
| `FROM "shop"."orders"` | `"orders"."id"` | OK |
| `FROM "shop"."orders" AS "o"` | `"o"."id"` | OK |
| `FROM "shop"."orders" AS "o"` | `"shop"."orders"."id"` | `invalid reference to FROM-clause entry` |
| `FROM "shop"."orders" AS "o"` | `"orders"."id"` | `missing FROM-clause entry` |

The framework drops the schema qualifier from column references as soon as a
table alias is in effect, so `Model.c.with_table_alias("o")` produces `"o"."col"`
and pairs with `join(..., alias="o")`.

Note that `PostgresColumnMixin.format_column` is not what enforces this. It
inspects the *column* alias, which is unset on this path; the guarantee comes
from `FieldProxy`. A hand-built `Column` bypasses it and can emit SQL PostgreSQL
rejects.

## `search_path` is a connect-time setting

`PostgresConnectionConfig.search_path` is forwarded to `psycopg.connect()` as a
libpq parameter. It is therefore fixed for the life of the connection and cannot
be changed per query or per transaction. The backend also runs with
`autocommit=True` outside managed transactions, so `SET LOCAL search_path` has
nowhere to attach.

Practical consequences:

- A model without `__schema_name__` always resolves through the same path.
- Per-tenant schemas require either one model class per tenant, or an
  architecture that manages `search_path` explicitly (not implemented here).
- `default_schema` has never affected generated SQL. Use `search_path`.

## DDL statements take a schema of their own

`__schema_name__` selects the read/write namespace. It is not consulted when DDL
is built, so a migration has to name the schema it means — but it can say so
directly now, rather than assembling a qualified name by hand.

| Statement | How to qualify it |
|---|---|
| `CREATE TABLE` / `DROP TABLE` | pass `TableExpression(dialect, "users", schema_name="app")` |
| `CREATE` / `ALTER` / `DROP` VIEW, incl. materialized | `schema_name="app"` |
| `CREATE` / `ALTER` / `DROP` TYPE | `schema_name="app"` |
| `CREATE` / `DROP` INDEX, incl. full-text | `schema_name="app"` |
| `CREATE` / `ALTER` / `DROP` SEQUENCE | `schema_name="app"` |
| `CREATE` / `ALTER` / `DROP` DOMAIN | `schema_name="app"` |
| `CREATE` / `DROP` FUNCTION | `schema_name="app"` |
| `CREATE` / `DROP` TRIGGER | `schema_name="app"` |

`schema_name` defaults to `None`, which means unqualified. Passing `""` is
rejected: an empty string is a mistake, not a way of saying "unqualified".

## Asking the server which schema is current

```python
backend.get_current_schema()   # 'public'
await async_backend.get_current_schema()
```

This reads `current_schema()`, which walks `search_path` and returns the first
schema that actually exists. `search_path` may name schemas that do not, so it
can legitimately resolve to nothing; that comes back as `None` rather than an
error, and is returned as-is.

## Extensions


`CREATE EXTENSION` quotes its target schema, so a mixed-case schema works:

```python
CreateExtensionExpression(dialect, "postgis", schema="My Schema")
# CREATE EXTENSION IF NOT EXISTS postgis SCHEMA "My Schema"
```

## orafce

orafce installs its functions into the `oracle` schema by default, and every
factory in `functions.orafce` schema-qualifies its call accordingly. If the
extension lives elsewhere, pass `schema=`:

```python
from ...functions import orafce
orafce.nvl(dialect, expr, "n/a")                      # ORACLE.NVL(...)
orafce.nvl(dialect, expr, "n/a", schema="ext")        # EXT.NVL(...)
```

The qualified name is emitted unquoted because `format_function_call`
upper-cases `func_name` verbatim. A `schema` containing uppercase letters,
spaces or a leading digit therefore cannot be expressed through this module — it
is case-folded by the server into a different, usually non-existent schema. A
`UserWarning` is emitted in that case; use a plain lowercase schema name, or
install orafce into the default one.

## PostGIS

`GEOMETRY` and `GEOGRAPHY` are emitted without a schema qualifier, as are the
`ST_*` and `ST_GeogFromText` calls. PostGIS must therefore be installed into a
schema on the connection's `search_path`:

```python
config = PostgresConnectionConfig(..., search_path="public,extensions")
```

Installing PostGIS elsewhere without extending `search_path` makes every
geometry DDL and DML statement fail with `type "geometry" does not exist`.
