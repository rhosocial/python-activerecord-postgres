# PostgreSQL Materialized Views

> Materialized views require PostgreSQL 9.3+; `CONCURRENTLY` refresh and
> `IF NOT EXISTS` require 9.4+.
>
> Official docs:
> [CREATE MATERIALIZED VIEW](https://www.postgresql.org/docs/current/sql-creatematerializedview.html) ·
> [REFRESH MATERIALIZED VIEW](https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html) ·
> [ALTER MATERIALIZED VIEW](https://www.postgresql.org/docs/current/sql-altermaterializedview.html)

## Expression matrix

| Expression | Purpose | PostgreSQL-specific options |
|---|---|---|
| `PostgresCreateMaterializedViewExpression` | `CREATE MATERIALIZED VIEW` | `schema`, `if_not_exists`, `storage_options`, `tablespace`, `column_aliases`, `with_data` |
| `PostgresRefreshMaterializedViewExpression` | `REFRESH MATERIALIZED VIEW` | `schema`, `concurrently`, `with_data` |
| `PostgresAlterMaterializedViewExpression` | `ALTER MATERIALIZED VIEW` | `schema`, action list |
| `PostgresDropMaterializedViewExpression` | `DROP MATERIALIZED VIEW` | `schema`, `if_exists`, `cascade` |

The generic core expressions (`CreateMaterializedViewExpression`,
`RefreshMaterializedViewExpression`, `DropMaterializedViewExpression`) remain
usable; they simply do not carry `schema` / `if_not_exists`.

## Creating a materialized view

```python
from rhosocial.activerecord.backend.expression import Column, FunctionCall, QueryExpression, TableExpression
from rhosocial.activerecord.backend.impl.postgres import PostgresStorageParameter
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresCreateMaterializedViewExpression,
)

summary = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, "product_id"),
        FunctionCall(dialect, "COUNT", Column(dialect, "id")),
        FunctionCall(dialect, "SUM", Column(dialect, "amount")),
    ],
    from_=TableExpression(dialect, "sales"),
    group_by_having=GroupByHavingClause(dialect, group_by=[Column(dialect, "product_id")]),
)

create = PostgresCreateMaterializedViewExpression(
    dialect=dialect,
    view_name="sales_summary",
    query=summary,
    schema="reporting",
    if_not_exists=True,
    column_aliases=["product_id", "sale_count", "total_amount"],
    storage_options={
        PostgresStorageParameter.FILLFACTOR: 70,
        PostgresStorageParameter.AUTOVACUUM_ENABLED: "false",
    },
)
sql, params = create.to_sql()
```

Emitted SQL (note the PostgreSQL grammar order —
column aliases, then `WITH (...)`, then `TABLESPACE`):

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS "reporting"."sales_summary"
    ("product_id", "sale_count", "total_amount")
    WITH (fillfactor = 70, autovacuum_enabled = false) AS
SELECT ... WITH DATA
```

### `WITH NO DATA`

```python
create = PostgresCreateMaterializedViewExpression(
    dialect=dialect, view_name="sales_daily", query=daily, with_data=False
)
```

The view is registered but unscannable until it is refreshed
(`REFRESH MATERIALIZED VIEW ... WITH DATA`).

## Storage parameters

A materialized view is stored as a regular heap relation — the server maps
`RELKIND_MATVIEW` onto `RELOPT_KIND_HEAP` in `fillRelOptions()`
(`src/backend/access/common/reloptions.c`). The accepted set is therefore the
standard heap relation options, enumerated by
[`PostgresStorageParameter`](../../api/storage_parameters.md):

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresStorageParameter

PostgresStorageParameter.FILLFACTOR.value            # 'fillfactor'
PostgresStorageParameter.FILLFACTOR.value_type       # PostgresStorageParameterValueType.INT
PostgresStorageParameter.VACUUM_INDEX_CLEANUP.enum_values  # ('auto', 'enabled', 'disabled')
PostgresStorageParameter.PARALLEL_WORKERS.min_version      # (11, 0, 0)
```

Keys are validated against that enum — a name outside the enum raises
`ValueError`. Two families are intentionally outside the enum and need the
explicit `allow_unlisted_storage_parameters=True` opt-in:

* namespaced options (`toast.autovacuum_enabled`, …);
* options registered by a table access method (`USING method`, e.g. a columnar
  AM adding `compresslevel`), since `add_reloption_kind()` /
  `add_*_reloption()` are exported server APIs.

```python
create = PostgresCreateMaterializedViewExpression(
    dialect=dialect,
    view_name="columnar_summary",
    query=summary,
    storage_options={"compresslevel": 4},
    allow_unlisted_storage_parameters=True,
)
```

## Refreshing

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresRefreshMaterializedViewExpression,
)

# standard refresh
PostgresRefreshMaterializedViewExpression(dialect=dialect, name="sales_summary")

# concurrent refresh — requires a UNIQUE index, rejected below PG 9.4
PostgresRefreshMaterializedViewExpression(
    dialect=dialect, name="sales_summary", schema="reporting", concurrently=True
)

# populate a WITH NO DATA view
PostgresRefreshMaterializedViewExpression(
    dialect=dialect, name="sales_daily", with_data=True
)
```

`concurrently=True` raises `UnsupportedFeatureError` on PostgreSQL < 9.4 — on
both the generic and the PostgreSQL expression.

## Altering a materialized view

PostgreSQL supports exactly five actions; `SET TABLESPACE` is **not** among
them (a materialized view cannot be relocated to another tablespace after
creation).

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresAlterMaterializedViewExpression,
    PostgresChangeMaterializedViewOwnerAction,
    PostgresRenameMaterializedViewAction,
    PostgresResetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewSchemaAction,
)

alter = PostgresAlterMaterializedViewExpression(
    dialect=dialect,
    view_name="sales_summary",
    schema="reporting",
    actions=[
        PostgresRenameMaterializedViewAction(dialect, "sales_summary_v2"),
        PostgresSetMaterializedViewSchemaAction(dialect, "archive"),
        PostgresSetMaterializedViewPropertiesAction(dialect, {"fillfactor": 90}),
        PostgresResetMaterializedViewPropertiesAction(dialect, ["autovacuum_enabled"]),
        PostgresChangeMaterializedViewOwnerAction(dialect, "CURRENT_USER"),
    ],
)
sql, params = alter.to_sql()
```

Multiple actions render as separate statements joined with `";\n"`:

```sql
ALTER MATERIALIZED VIEW "reporting"."sales_summary" RENAME TO "sales_summary_v2";
ALTER MATERIALIZED VIEW "reporting"."sales_summary" SET SCHEMA "archive";
ALTER MATERIALIZED VIEW "reporting"."sales_summary" SET (fillfactor = 90);
ALTER MATERIALIZED VIEW "reporting"."sales_summary" RESET (autovacuum_enabled);
ALTER MATERIALIZED VIEW "reporting"."sales_summary" OWNER TO CURRENT_USER
```

`CURRENT_ROLE` / `CURRENT_USER` / `SESSION_USER` are emitted verbatim; any other
owner is quoted as an identifier.

## Dropping

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresDropMaterializedViewExpression,
)

PostgresDropMaterializedViewExpression(
    dialect=dialect, view_name="sales_summary", schema="reporting",
    if_exists=True, cascade=True,
)
# DROP MATERIALIZED VIEW IF EXISTS "reporting"."sales_summary" CASCADE
```

## Introspection

Materialized views are **not** part of `list_views()` (which stays `relkind='v'`).
Use the dedicated methods, available on both the sync and async introspector:

```python
views = backend.introspector.list_materialized_views()
for view in views:
    print(view.name, view.schema, view.extra)

info = backend.introspector.get_materialized_view_info("sales_summary")
exists = backend.introspector.materialized_view_exists("sales_summary")
```

Each `ViewInfo` carries:

| Field | Meaning |
|---|---|
| `definition` | defining query, from `pg_matviews` |
| `is_updatable` / `is_insertable` | always `False` |
| `extra['is_materialized']` | always `True` (distinguishes MV rows) |
| `extra['is_populated']` | `False` for a `WITH NO DATA` view never refreshed |
| `extra['has_unique_index']` | prerequisite for `REFRESH ... CONCURRENTLY` |

`list_tables(table_type="MATERIALIZED VIEW")` also filters `relkind = 'm'`.

## Capability probes

```python
dialect.supports_materialized_view()                     # PG >= 9.3
dialect.supports_refresh_materialized_view()             # PG >= 9.3
dialect.supports_materialized_view_concurrent_refresh()  # PG >= 9.4
dialect.supports_materialized_view_if_not_exists()       # PG >= 9.4
dialect.supports_materialized_view_tablespace()          # PG >= 9.3
dialect.supports_materialized_view_storage_options()     # PG >= 9.3
dialect.supports_alter_materialized_view()               # PG >= 9.3
```

All of them are declared by `PostgresMaterializedViewSupport`. Every
materialized-view formatter lives in `PostgresMaterializedViewMixin`, which sits
**before** the core `ViewMixin` in `PostgresDialect`'s MRO — a regression test
asserts that ordering, because a mixin placed after the core one silently
disables its formatter.

## See also

* [DDL operations](./README.md)
* [PostgreSQL-specific features](../postgres_specific_features/protocol_support.md)
* Runnable example:
  `src/rhosocial/activerecord/backend/impl/postgres/examples/ddl/materialized_view.py`
