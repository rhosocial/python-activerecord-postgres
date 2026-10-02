# docs/en_US/postgres_specific_features/schema_namespace.md

# PostgreSQL Schema Namespaces

> This page covers what is specific to this backend: what a `schema_name` names
> here, how a qualified name is rendered, what a table alias does to column
> references, how `search_path` relates to `__schema_name__`, and the
> PostgreSQL extensions whose names are not qualified at all.
>
> The model-level API — declaring `__schema_name__`, when the schema reaches
> the SQL, the DDL boundary, the cross-backend support matrix — is documented
> in the core library guide `docs/modeling/schema_namespace.md`, which lives in
> the `python-activerecord` repository
> ([`docs/en_US/modeling/schema_namespace.md`][core-en]).

[core-en]: https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/modeling/schema_namespace.md

Unless stated otherwise, the SQL in this page was produced by rendering the
corresponding expression objects with `PostgresDialect`, without a live server.
Statements marked as the server's own response quote PostgreSQL's error text.

## What a `schema_name` names here

`PostgresDialect` implements the core `SchemaSupport` protocol, and the whole
schema flag set answers `True`:

```python
dialect.supports_schema()                # True
dialect.supports_create_schema()         # True
dialect.supports_drop_schema()           # True
dialect.supports_schema_if_not_exists()  # True
dialect.supports_schema_if_exists()      # True
dialect.supports_schema_cascade()        # True
dialect.supports_schema_authorization()  # True
```

So a `schema_name` is accepted everywhere the core expects one. What the value
*names* is PostgreSQL's own: **a schema inside the current database**. One
PostgreSQL database routinely holds several, and they are true namespaces —
`"app"."orders"` and `"public"."orders"` are unrelated tables that happen to
share a name.

Two renderings, both double-quoted:

| Expression | SQL |
|---|---|
| `TableExpression(d, "orders", schema_name="app")` | `"app"."orders"` |
| `TableExpression(d, "orders")` | `"orders"` |
| `TableExpression(d, "orders", schema_name="app", alias="o")` | `"app"."orders" AS "o"` |

Each part is quoted on its own, so quoting preserves case:
`schema_name="ar_xcrm"` renders `"ar_xcrm"`, and `schema_name="MySchema"`
renders `"MySchema"`. PostgreSQL folds *unquoted* identifiers to lower case at
parse time; a quoted one is taken literally. Folding to upper case is Oracle's
behaviour, not PostgreSQL's.

A dot inside the value is not a separator either — see
[Common mistakes](#common-mistakes).

## Declaring one on a model

```python
from typing import ClassVar, Optional

class Order(ActiveRecord):
    __table_name__ = "orders"
    __schema_name__ = "shop"                 # -> "shop"."orders"
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
```

`__schema_name__` is optional and defaults to `None`, which means unqualified.
When it is set, every statement the model builds carries the namespace —
`SELECT`, `WHERE`, `ORDER BY`, `INSERT`, `UPDATE` and `DELETE` alike:

```python
Order.query().select(Order.c.id).to_sql()[0]
# SELECT "shop"."orders"."id" FROM "shop"."orders"

Order.query().where(Order.c.id > 1).to_sql()[0]
# SELECT * FROM "shop"."orders" WHERE "shop"."orders"."id" > %s
```

DML built directly from the expression layer carries it as well:

```python
# INSERT INTO "shop"."orders"  VALUES (%s)
# UPDATE "shop"."orders" SET "user_id" = %s WHERE "shop"."orders"."id" = %s
# DELETE FROM "shop"."orders" WHERE "shop"."orders"."id" = %s
```

A model without `__schema_name__` renders unqualified and lets the connection
decide:

```python
PlainOrder.query().select(PlainOrder.c.id).to_sql()[0]
# SELECT "plain_orders"."id" FROM "plain_orders"
```

The namespace is read once, through `schema_name()`, and reaches each column
expression as it is built. Changing `__schema_name__` afterwards therefore does
not rewrite an expression that already exists — rebuild the condition, or build
it after the change.

## DDL statements take a schema of their own

`__schema_name__` selects the read/write namespace. It is **not** consulted when
DDL is built — a migration has to name the schema it means — but every statement
that names a schema-bearing object accepts a schema of its own, so qualification
no longer has to be assembled by hand.

### Statements that take `schema_name`

```python
TableExpression(dialect, "users", schema_name="app").to_sql()[0]   # "app"."users"

DropTableExpression(dialect, TableExpression(dialect, "users", schema_name="app"),
                    if_exists=True).to_sql()[0]
# DROP TABLE IF EXISTS "app"."users"

TruncateExpression(dialect, "users", schema_name="app").to_sql()[0]
# TRUNCATE TABLE "app"."users"

CreateIndexExpression(dialect, "idx_users_email", "users", ["email"],
                      schema_name="app").to_sql()[0]
# CREATE INDEX "app"."idx_users_email" ON "app"."users" ("email")

PostgresAlterIndexExpression(dialect, "idx_users_email",
                             PostgresAlterIndexActionType.RENAME_TO,
                             schema_name="app",
                             new_name="idx_users_email_idx").to_sql()[0]
# ALTER INDEX "app"."idx_users_email" RENAME TO "idx_users_email_idx"
```

`CREATE TABLE` and `DROP TABLE` are the two plain forms that take a qualified
`TableExpression` rather than a `schema_name` of their own — they have no
`schema_name` parameter. That is also why the tuple spelling is not available
everywhere: the derived forms normalize `(schema, table)`, so
`CreateTableAsExpression(dialect, ("app", "t"), query)` renders
`CREATE TABLE "app"."t" AS ...` and `CreateTableLikeExpression(dialect,
("app", "t"), ("app", "src"))` renders `CREATE TABLE "app"."t" (LIKE
"app"."src")`, but `CreateTableExpression` and `DropTableExpression` accept only
`str` or `TableExpression` and raise

```
TypeError: table must be str or TableExpression, got tuple
```

`CreateIndexExpression` has one `schema_name` and it covers both names: the index
and the table it is built on land in the same schema.

### This backend's own statements spell it `schema`

Most of the PostgreSQL-specific expressions spell the parameter `schema` rather
than `schema_name`. The materialized-view statements are the clearest case:

```python
PostgresCreateMaterializedViewExpression(dialect, "Order Summary", query,
                                         schema="mv_reporting")
PostgresRefreshMaterializedViewExpression(dialect, "Order Summary",
                                          schema="mv_reporting")
PostgresAlterMaterializedViewExpression(dialect, "Order Summary", actions,
                                         schema="mv_reporting")
PostgresDropMaterializedViewExpression(dialect, "Order Summary",
                                        schema="mv_reporting")
```

and so are extensions, enums, partitions, `COPY`, `VACUUM`, `ANALYZE`, `REINDEX`,
`REPACK`, statistics, `pg_partman` and `COMMENT ON`. The index statements are the
exception, and they are inconsistent with each other:
`PostgresCreateIndexExpression` accepts **no** schema parameter at all, so the
index it creates follows the connection's `search_path`, while
`PostgresAlterIndexExpression` and `PostgresDropIndexExpression` take
`schema_name`. To create an index in a named schema, use the core
`CreateIndexExpression`.

Because the keyword varies by class, check the signature before passing a schema:
a wrong keyword name raises `TypeError` at construction.

The domain expressions accept the namespace under either name: `schema=` (older,
positional-or-keyword) and `schema_name=` (keyword-only). Passing both with
different values raises, and this check happens **when the expression is built**,
not while rendering:

```
ValueError: schema and schema_name must match when both are provided
```

`schema_name` defaults to `None`, which means unqualified. Passing `""` is
rejected — see [The empty string](#the-empty-string-and-when-it-is-caught).

### `CREATE SCHEMA` / `DROP SCHEMA` are the exception

There, the schema is not a qualifier — it *is* the object:

```python
CreateSchemaExpression(dialect, "app").to_sql()[0]      # CREATE SCHEMA "app"
CreateSchemaExpression(dialect, "app", if_not_exists=True).to_sql()[0]
# CREATE SCHEMA IF NOT EXISTS "app"
CreateSchemaExpression(dialect, "app", authorization="app_user").to_sql()[0]
# CREATE SCHEMA "app" AUTHORIZATION "app_user"
DropSchemaExpression(dialect, "app").to_sql()[0]        # DROP SCHEMA "app"
DropSchemaExpression(dialect, "app", if_exists=True, cascade=True).to_sql()[0]
# DROP SCHEMA IF EXISTS "app" CASCADE
```

## `search_path` and `__schema_name__` are two different jobs

`PostgresConnectionConfig.search_path` is a **server runtime parameter**, not a
libpq connection keyword. The backend folds it into the `options` keyword as
`-c search_path=...` before calling `psycopg.connect()`; handing `search_path` to
libpq as a connection parameter of its own fails the connect outright with
`invalid connection option "search_path"`. The comment in
`backend/backend.py` that folds it records that failure as the reason the field
used to make connecting impossible rather than merely ineffective.

The folded value is appended to whatever `options` already holds, so setting
both works rather than one replacing the other:

```python
PostgresConnectionConfig(..., search_path="app", options={"datestyle": "ISO"})
# psycopg receives options='-c datestyle=ISO -c search_path=app'
```

libpq parses that string by splitting on whitespace, so keep the schema list free
of spaces: write `search_path="app,public"`, not `"app, public"`.

Two more consequences:

- **It is fixed for the life of the connection.** It is set once, at connect
  time, and cannot be changed per query or per transaction. The driver-level
  connection also runs with `autocommit = True`, and the backend never issues a
  `SET` or `SET LOCAL` on its own — switching schemas mid-session means writing
  that SQL yourself.
- **One caveat in `to_connection_string()`.** The config's own URI builder emits
  the parameter as a URI query parameter, and libpq refuses that form:

  ```python
  config.to_connection_string()
  # postgres://postgres@localhost:5432/test?search_path=app,public
  # -> ProgrammingError: invalid URI query parameter: "search_path"
  ```

  Connect through the backend, which folds the parameter into `options` where
  libpq accepts it.

**Let `search_path` carry the common case and keep `__schema_name__` for the
exception:**

```python
config = PostgresConnectionConfig(
    ...,
    search_path="app,public",   # ordinary tables resolve unqualified
)
```

- **Single schema** — set no `__schema_name__` at all. Unqualified names plus
  `search_path` keep DML, DDL and introspection consistent with one another, and
  avoid three-part column references entirely.
- **Several schemas** — set `__schema_name__` only on the models that deviate
  from `search_path`. The smaller the exceptional surface, the fewer chances of
  hitting the mistakes below. Every deviation costs a three-part column
  reference on every statement that touches the model.
- **Per-tenant schemas** — one model class per tenant, or an architecture that
  manages `search_path` explicitly. The latter is not implemented here, because
  `search_path` cannot be moved per query.

`PostgresConnectionConfig.default_schema` is deprecated. It has never affected
generated SQL — a model without `__schema_name__` resolves through `search_path`
regardless — so do not reach for it. Introspection still reads it as its first
choice when picking a schema to look in, falling back to the first `search_path`
entry and then to `public`, so a stale value there can make introspection report
a schema that your SQL never uses.

## Three-part references, and what an alias does to them

An unaliased range may be addressed by its relation name *or* by its qualified
name. An aliased range **replaces** the relation name, so the alias is the only
thing left to address it by — and PostgreSQL rejects a schema-qualified
reference to an aliased range, because the alias has taken the range's place:

| Range | Column reference | Server response |
|---|---|---|
| `FROM "shop"."orders"` | `"shop"."orders"."id"` | accepted |
| `FROM "shop"."orders"` | `"orders"."id"` | accepted |
| `FROM "shop"."orders" AS "o"` | `"o"."id"` | accepted |
| `FROM "shop"."orders" AS "o"` | `"shop"."orders"."id"` | `invalid reference to FROM-clause entry` |
| `FROM "shop"."orders" AS "o"` | `"orders"."id"` | `missing FROM-clause entry` |

The framework follows that rule by producing the three-part form whenever no
alias is in effect, and the alias-only form whenever one is:

```python
Order.query().select(Order.c.id).to_sql()[0]
# SELECT "shop"."orders"."id" FROM "shop"."orders"

Order.query().select(Order.c.with_table_alias("o").id).to_sql()[0]
# SELECT "o"."id" FROM "shop"."orders"
```

The suppression happens when the column expression is **constructed**, not when
it is rendered: `FieldProxy` sets `schema_name` to `None` as soon as a table
alias is in effect. The PostgreSQL dialect's `format_column` override is *not*
what enforces this — it inspects the *column* alias (`expr.alias`), which is
unset on this path; the three-part branch is guarded by `schema_name and not
alias`. A hand-built `Column` therefore bypasses the guard:

```python
Column(dialect, "id", table="o", schema_name="shop").to_sql()[0]
# "shop"."o"."id"      <- PostgreSQL rejects this
```

The framework also drops the schema from a *column* reference that carries a
column alias, because that is what PostgreSQL requires:

```python
Column(dialect, "id", table="orders", schema_name="shop", alias="x").to_sql()[0]
# "orders"."id" AS "x"
```

### Cross-schema joins

Each side qualifies its own range, so one statement can span two namespaces with
no extra configuration:

```python
Order.query().join(User, on=Order.c.user_id == User.c.id).select(
    Order.c.id, User.c.name
).to_sql()[0]
# SELECT "shop"."orders"."id", "crm"."users"."name"
#   FROM "shop"."orders" JOIN "crm"."users"
#   ON "shop"."orders"."user_id" = "crm"."users"."id"
```

Alias one side and only that side's columns change shape — the range keeps its
qualifier:

```python
Order.query().join(
    User, on=Order.c.user_id == User.c.with_table_alias("u").id, alias="u"
).select(Order.c.id, User.c.with_table_alias("u").name).to_sql()[0]
# SELECT "shop"."orders"."id", "u"."name"
#   FROM "shop"."orders" JOIN "crm"."users" AS "u"
#   ON "shop"."orders"."user_id" = "u"."id"
```

The framework refuses a join whose condition still addresses the unaliased
range:

```
ValueError: cannot join crm.users with alias 'u' using a condition that still
refers to crm.users: an aliased range can only be addressed by its alias. Build
the condition from users.c.with_table_alias('u') so the reference and the alias
agree.
```

So build the condition from the aliased accessor. In a self-join that means
aliasing **both** sides, or neither — the left range has no `alias=` of its own,
and a condition that mixes the two spellings is refused:

```python
Order.query().join(
    Order, on=Order.c.with_table_alias("c").id == Order.c.with_table_alias("p").user_id,
    alias="p",
).select(Order.c.with_table_alias("c").id, Order.c.with_table_alias("p").id).to_sql()[0]
# SELECT "c"."id", "p"."id"
#   FROM "shop"."orders" JOIN "shop"."orders" AS "p"
#   ON "c"."id" = "p"."user_id"
```

Aliasing only the column side leaves the range unaliased, and the SQL that comes
out is rejected by the server rather than by the framework:

```python
Order.query().select(Order.c.with_table_alias("o").id).to_sql()[0]
# SELECT "o"."id" FROM "shop"."orders"     <- missing FROM-clause entry
```

### Set operations

`UNION`, `INTERSECT` and `EXCEPT` name no object of their own, so there is
nothing for them to qualify. Each branch keeps its own namespace:

```python
Order.query().select(Order.c.id).union(User.query().select(User.c.id)).to_sql()[0]
# SELECT "shop"."orders"."id" FROM "shop"."orders"
#   UNION
#   SELECT "crm"."users"."id" FROM "crm"."users"
```

### CTEs

A CTE is named for the rest of the query, not for the database, so its own name
is never qualified — qualifying it would look for a table called
`"recent_orders"` *inside* the schema. The query inside it still carries the
model's schema:

```python
WITH "recent_orders" AS (SELECT "shop"."orders"."id" FROM "shop"."orders")
SELECT "recent_orders"."id" FROM "recent_orders"
```

## The empty string, and when it is caught

`""` is a mistake, not a way of saying "unqualified" — that is what `None` means.
It is rejected, but **not when the expression is built**: an expression only
collects parameters at that point — its dialect may not even be settled yet —
so strict validation happens while the statement is rendered, where the
statement is known to be whole. The failure therefore arrives later than you
would expect:

```python
class Bad(ActiveRecord):
    __table_name__ = "empties"
    __schema_name__ = ""

Bad.schema_name()                        # ''           -- no error
Bad.c.id                                 # Column       -- no error
Bad.query()                              # ActiveQuery  -- no error
Bad.query().select(Bad.c.id)             # ActiveQuery  -- no error
Bad.query().select(Bad.c.id).to_sql()    # ValueError   -- here
```

The message names the expression at fault:

```
ValueError: Column.schema_name must be a non-empty string; use None for an
unqualified reference
```

```
ValueError: TableExpression.schema_name must be a non-empty string; use None for
an unqualified reference
```

A blank string is rejected the same way as an empty one — the check strips
whitespace first, so `"   "` is refused too.

A non-string is rejected the same way, with its own message:

```
ValueError: TableExpression.schema_name must be a string or None, not int
```

`TruncateExpression` raises the `TableExpression` wording, because it renders
its table through a `TableExpression` internally — the message names the object
that validated the value, not the statement you wrote.

The reason to reject rather than treat `""` as absent: `format_table` decides
whether to qualify from `bool(expr.schema_name)`, which is false for `""`, and
takes the unqualified branch. A caller who asked for `app.users` would get
`users` with no error, no warning and no affected-row count to notice it by —
and on any connection whose `search_path` happens to contain `app`, the
statement would run against the other table.

## Common mistakes

**A dot in `__table_name__` is not a namespace.** The identifier is quoted as a
single unit:

```python
class User(ActiveRecord):
    __table_name__ = "app.users"

User.query().select(User.c.id).to_sql()[0]
# SELECT "app.users"."id" FROM "app.users"    -- relation "app.users" does not exist
```

Nothing splits it into schema plus name. Use `__schema_name__`, or pass a
qualified `TableExpression` to the statement that needs one.

**A dot in `__schema_name__` is not a namespace either.** Each part is quoted
separately, so a dot inside one part stays inside that part:

```python
TableExpression(dialect, "orders", schema_name="app.public").to_sql()[0]
# "app.public"."orders"     -- a schema literally named "app.public"
```

**A schema in a field name is just a column name.** The schema qualifies the
*range*; it has nothing to do with how a column is spelled:

```python
class Report(ActiveRecord):
    __table_name__ = "reports"
    __schema_name__ = "app"
    app_total: Optional[int] = None

Report.query().select(Report.c.app_total).to_sql()[0]
# SELECT "app"."reports"."app_total" FROM "app"."reports"
```

**Expecting construction to raise.** Nothing rejects a bad `schema_name` until
the statement renders. A model-level mistake therefore survives every step up to
and including query building, and fails — or, in the case the check exists to
prevent, resolves to the wrong table without a word about it.

**Aliasing only one side of a join, or only the column side.** Both are covered
above; the rule is that a reference and its range alias have to agree.

**Reaching for `default_schema`.** Deprecated and inert for generated SQL. Set
`search_path`.

## Asking the server which schema is current

```python
backend.get_current_schema()          # the first schema on search_path that exists
await async_backend.get_current_schema()
```

This asks the server through `current_schema()`, which walks `search_path` and
returns the first schema that actually exists. `search_path` may name schemas
that do not exist, so it can legitimately resolve to nothing; that comes back as
`None` rather than an error, and is returned as-is.

## Extensions

`CREATE EXTENSION` quotes its target schema, so a mixed-case schema works:

```python
PostgresCreateExtensionExpression(dialect, "postgis", schema="My Schema").to_sql()[0]
# CREATE EXTENSION IF NOT EXISTS postgis SCHEMA "My Schema"
```

The parameter here is `schema=`, like the rest of this backend's own statements.

## orafce

orafce installs its functions into the `oracle` schema by default, and every
factory in `functions.orafce` schema-qualifies its call accordingly. If the
extension lives elsewhere, pass `schema=`:

```python
orafce.nvl(dialect, expr, "n/a")                # ORACLE.NVL(%s, %s)
orafce.nvl(dialect, expr, "n/a", schema="ext")  # EXT.NVL(%s, %s)
```

The qualified name is emitted **unquoted**, because `format_function_call`
upper-cases `func_name` verbatim. A `schema` containing uppercase letters, spaces
or a leading digit therefore cannot be expressed through this module — it is
case-folded by the server into a different, usually non-existent schema:

```
UserWarning: orafce schema 'Ext' is not a plain lowercase identifier; function
names are rendered unquoted and upper-cased, so the server will look for a
different schema. Use a lowercase name.
```

Use a plain lowercase schema name, or install orafce into the default one.

## PostGIS

`GEOMETRY` and `GEOGRAPHY` are emitted without a schema qualifier, and so are the
`ST_*` calls. PostGIS must therefore be installed into a schema on the
connection's `search_path`:

```python
config = PostgresConnectionConfig(..., search_path="public,extensions")
```

Installing PostGIS elsewhere without extending `search_path` makes every
geometry DDL and DML statement fail with `type "geometry" does not exist`.