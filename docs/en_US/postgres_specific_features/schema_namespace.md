# docs/en_US/postgres_specific_features/schema_namespace.md

# PostgreSQL Schema Namespaces

> This page covers what is specific to this backend: what a `schema_name` names
> here, how a qualified name is rendered, how each DDL object picks its own
> namespace, what a table alias does to column references, how `search_path`
> relates to `__schema_name__`, and the PostgreSQL extensions whose names are
> not qualified at all.
>
> The model-level API — declaring `__schema_name__`, the DDL factories that
> build a model's statements, the cross-backend support matrix — is documented
> in the core library guide `docs/modeling/schema_namespace.md`, which lives in
> the `python-activerecord` repository
> ([`docs/en_US/modeling/schema_namespace.md`][core-en]).

[core-en]: https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/modeling/schema_namespace.md

Unless stated otherwise, the SQL in this page was produced by rendering the
corresponding expression objects with `PostgresDialect(version=(15, 0, 0))`,
without a live server. Statements marked as the server's own response quote
PostgreSQL's error text and were not exercised here; this repository has no
PostgreSQL instance to run against.

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

`TableExpression` is the single carrier of a qualified name. It is used for a
range in a `FROM` clause and for the objects that DDL names rather than
selects, and each instance carries its own `schema_name`:

```python
TableExpression(dialect, "orders", schema_name="app").to_sql()[0]   # "app"."orders"
TableExpression(dialect, "orders").to_sql()[0]                     # "orders"
```

Two renderings, both double-quoted, with the alias on the range:

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

DML built directly from the expression layer carries it as well. `UPDATE` and
`MERGE` take their target as a qualified `TableExpression`:

```python
# INSERT INTO "app"."users" ("id") SELECT "id" FROM "staging"
# UPDATE "app"."users" SET "name" = "src"."name"
# DELETE FROM "app"."users"
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

## DDL names its objects independently

Every statement that names a table takes a `TableExpression`, and every
statement that names a database object takes a namespace for that object alone.
An index, a trigger and the table it is built on each carry their own.

### Every table name is a `TableExpression`

A statement that names a table refuses a bare string:

```python
CreateTableExpression(dialect, "users", columns)     # TypeError
DropTableExpression(dialect, "users")                # TypeError
TruncateExpression(dialect, "users")                 # TypeError
AlterTableExpression(dialect, "users", actions)      # TypeError
CreateIndexExpression(dialect, "idx", "users", ["id"])   # TypeError
DropIndexExpression(dialect, "idx", "users")             # TypeError
```

```
TypeError: table must be a TableExpression, got str
```

A bare string used to be wrapped into an unnamed `TableExpression`, which
dropped the namespace without saying so:
`CreateIndexExpression(d, "idx", "users", ["id"], schema_name="app")` rendered
`CREATE INDEX "app"."idx" ON "users"` — the index qualified, the table not. Pass
the qualified reference instead:

```python
CreateIndexExpression(
    dialect, "idx_users_email",
    TableExpression(dialect, "users", schema_name="app"),
    ["email"], schema_name="app",
).to_sql()[0]
# CREATE INDEX "app"."idx_users_email" ON "app"."users" ("email")
```

`MergeExpression` applies the same rule to `target_table`, with its own
message:

```
TypeError: target_table must be a TableExpression, got str
```

`INSERT` and `DELETE` are no longer exceptions. `InsertExpression.into` and
`DeleteExpression.tables` reject a bare string for the same reason:

```
TypeError: into must be a TableExpression, got str
TypeError: tables must be a TableExpression, got str
```

A `DELETE` against several tables needs every one of them qualified:

```python
DeleteExpression(dialect, [
    TableExpression(dialect, "orders", schema_name="sales"),
    TableExpression(dialect, "archive", schema_name="cold"),
], where=predicate).to_sql()[0]
```

### Where the namespace comes from, statement by statement

```python
TableExpression(dialect, "users", schema_name="app").to_sql()[0]   # "app"."users"

DropTableExpression(dialect, TableExpression(dialect, "users", schema_name="app"),
                    if_exists=True).to_sql()[0]
# DROP TABLE IF EXISTS "app"."users"

TruncateExpression(dialect, TableExpression(dialect, "users", schema_name="app")).to_sql()[0]
# TRUNCATE TABLE "app"."users"

AlterTableExpression(dialect, TableExpression(dialect, "users", schema_name="app"),
                     [DropColumn(dialect, "legacy")]).to_sql()[0]
# ALTER TABLE "app"."users" DROP COLUMN "legacy"

PostgresAlterIndexExpression(dialect, "idx_users_email",
                             PostgresAlterIndexActionType.RENAME_TO,
                             schema_name="app",
                             new_name="idx_users_email_idx").to_sql()[0]
# ALTER INDEX "app"."idx_users_email" RENAME TO "idx_users_email_idx"
```

`CREATE TABLE` and `DROP TABLE` have no `schema_name` parameter of their own:
the table's namespace arrives inside its `TableExpression`. That is also why
the tuple spelling is not available for them. The derived forms normalize
`(schema, table)`, so `CreateTableAsExpression(dialect, ("app", "t"), query)`
renders `CREATE TABLE "app"."t" AS ...` and `CreateTableLikeExpression(dialect,
("app", "t"), ("app", "src"))` renders `CREATE TABLE "app"."t" (LIKE
"app"."src")`, while `CreateTableExpression` and `DropTableExpression` accept
only a `TableExpression` and raise

```
TypeError: table must be a TableExpression, got tuple
```

### Indexes and tables choose namespaces independently

`schema_name` on an index statement qualifies **the index name**. The table is
qualified by its own `TableExpression`, so the two need not agree:

```python
CreateIndexExpression(
    dialect, "idx_shared",
    TableExpression(dialect, "orders", schema_name="sales"),
    ["user_id"], schema_name="app",
).to_sql()[0]
# CREATE INDEX "app"."idx_shared" ON "sales"."orders" ("user_id")
```

PostgreSQL's `search_path` has nothing to do with either name: both are
qualified or bare exactly as written.

`DROP INDEX` takes the same pair, and its `table` argument is optional. It
renders no `ON` clause, because PostgreSQL's `DROP INDEX` has none:

```python
DropIndexExpression(dialect, "idx_users_email",
                    TableExpression(dialect, "users", schema_name="app"),
                    schema_name="app").to_sql()[0]
# DROP INDEX "app"."idx_users_email"
```

Whether an index name may carry a namespace at all is a property of the
grammar, reported by `supports_index_schema_qualification()`. PostgreSQL
answers `True`, and a dialect that answers `False` raises
`UnsupportedFeatureError` while rendering rather than emitting a statement its
server would reject.

### Triggers choose a namespace each

`CreateTriggerExpression` takes the table and the function as
`TableExpression` values, each with its own namespace, and `schema_name`
qualifies the trigger name:

```python
CreateTriggerExpression(
    dialect, "trg_orders",
    TableExpression(dialect, "orders", schema_name="sales"),
    TriggerTiming.BEFORE, [TriggerEvent.UPDATE],
    function_name=TableExpression(dialect, "set_updated_at", schema_name="tools"),
    schema_name="app",
).to_sql()[0]
# CREATE TRIGGER "app"."trg_orders" BEFORE UPDATE ON "sales"."orders"
#   FOR EACH ROW EXECUTE FUNCTION "tools"."set_updated_at"()
```

All three namespaces in that statement are independent: the trigger is created
in `app`, reads `sales`.`orders`, and calls `tools`.`set_updated_at`.

### This backend's own statements spell it `schema`

Most of the PostgreSQL-specific expressions spell the parameter `schema` rather
than `schema_name`. The materialized-view statements are the clearest case:

```python
PostgresCreateMaterializedViewExpression(dialect, "Order Summary", query,
                                         schema="mv_reporting")
# CREATE MATERIALIZED VIEW "mv_reporting"."Order Summary" AS ... WITH DATA
PostgresRefreshMaterializedViewExpression(dialect, "Order Summary",
                                          schema="mv_reporting")
# REFRESH MATERIALIZED VIEW "mv_reporting"."Order Summary"
PostgresAlterMaterializedViewExpression(dialect, "Order Summary", actions,
                                         schema="mv_reporting")
# ALTER MATERIALIZED VIEW "mv_reporting"."Order Summary" RENAME TO "Order Summary 2"
PostgresDropMaterializedViewExpression(dialect, "Order Summary",
                                       schema="mv_reporting")
# DROP MATERIALIZED VIEW "mv_reporting"."Order Summary"
```

and so are extensions, enums, partitions, `COPY`, `VACUUM`, `ANALYZE`, `REINDEX`,
`REPACK`, statistics, `pg_partman` and `COMMENT ON`. The index statements
spell it `schema_name`, and they all take it — including
`PostgresCreateIndexExpression`, which adds PostgreSQL's own `opclasses`,
`nulls_not_distinct` and `with_options` on top:

```python
PostgresCreateIndexExpression(
    dialect, "idx_users_name",
    TableExpression(dialect, "users", schema_name="app"),
    ["name"], schema_name="app", opclasses={"name": "text_pattern_ops"},
).to_sql()[0]
# CREATE INDEX "app"."idx_users_name" ON "app"."users" ("name" text_pattern_ops)
```

`PostgresDropIndexExpression` adds `concurrent` for `DROP INDEX CONCURRENTLY`,
which PostgreSQL 18 introduced; earlier versions refuse it. Because the keyword
varies by class, check the signature before passing a schema: a wrong keyword
name raises `TypeError` at construction.

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

### When a namespace is judged

Construction only collects parameters: an expression's dialect may not be
settled yet, and its parameters may still be incomplete. A namespace is
therefore judged while the statement is rendered, by the dialect, which is the
point at which the statement is known to be whole. A dialect that implements
`SchemaSupport` and answers `supports_schema()` with `False` refuses explicitly;
one that does not implement the protocol ignores the namespace altogether.

## DDL built from a model

A model's namespace reaches its DDL through one place. `build_table_reference()`
returns the model's table carrying `__schema_name__`, and every other factory is
reached through it, so a model that declares the namespace once places all of
its objects there and no two statements can drift apart.

```python
class Order(ActiveRecord):
    __table_name__ = "orders"
    __schema_name__ = "shop"

Order.build_table_reference(dialect).to_sql()[0]       # "shop"."orders"
Order.build_table_reference(dialect, alias="o").to_sql()[0]
# "shop"."orders" AS "o"

Order.build_create_table_statement(dialect, columns).to_sql()[0]
# CREATE TABLE "shop"."orders" (...)
Order.build_drop_table_statement(dialect, if_exists=True).to_sql()[0]
# DROP TABLE IF EXISTS "shop"."orders"
Order.build_truncate_statement(dialect, restart_identity=True).to_sql()[0]
# TRUNCATE TABLE "shop"."orders" RESTART IDENTITY
Order.build_alter_table_statement(
    dialect, [DropColumn(dialect, "legacy")]).to_sql()[0]
# ALTER TABLE "shop"."orders" DROP COLUMN "legacy"
```

The index factories take the index's namespace separately. It defaults to the
model's own, which is what a caller almost always wants; pass
`index_schema_name` to place the index elsewhere:

```python
Order.build_create_index_statement(
    dialect, "idx_orders_email", ["email"]).to_sql()[0]
# CREATE INDEX "shop"."idx_orders_email" ON "shop"."orders" ("email")

Order.build_create_index_statement(
    dialect, "idx_orders_email", ["email"], index_schema_name="reporting").to_sql()[0]
# CREATE INDEX "reporting"."idx_orders_email" ON "shop"."orders" ("email")

Order.build_drop_index_statement(
    dialect, "idx_orders_email", if_exists=True).to_sql()[0]
# DROP INDEX IF EXISTS "shop"."idx_orders_email"
```

The full signatures are:

```python
Model.build_table_reference(dialect, alias=None)
Model.build_create_table_statement(dialect, columns, ...)
Model.build_drop_table_statement(dialect, if_exists=False)
Model.build_truncate_statement(dialect, restart_identity=False, cascade=False)
Model.build_alter_table_statement(dialect, actions)
Model.build_create_index_statement(dialect, index_name, columns, *,
                                   index_schema_name=None, **options)
Model.build_drop_index_statement(dialect, index_name, *,
                                 index_schema_name=None, if_exists=False, **options)
```

A hand-assembled expression does not get `__schema_name__` for free. It is
reached from a dialect, not from a model, so a statement built that way has to
be handed the namespaces it needs:

```python
# Reaches "shop"."orders" only because the reference was built that way.
DropTableExpression(
    dialect, TableExpression(dialect, "orders", schema_name="shop")).to_sql()[0]
# DROP TABLE "shop"."orders"
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

**Handing a DDL statement a bare table name.** It raises `TypeError` at
construction, and the message says which argument is at fault. The fix is a
qualified `TableExpression`, not a string.

**Building DDL by hand and expecting `__schema_name__` to reach it.** Only the
model factories read the declaration. An expression assembled at a call site
carries whatever namespaces it was given.

**Expecting construction to raise for a bad `schema_name`.** Nothing rejects a
bad value until the statement renders. A model-level mistake therefore survives
every step up to and including query building, and fails — or, in the case the
check exists to prevent, resolves to the wrong table without a word about it.

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
