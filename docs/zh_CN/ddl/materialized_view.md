# PostgreSQL 物化视图

> 物化视图需要 PostgreSQL 9.3+；`CONCURRENTLY` 刷新与 `IF NOT EXISTS` 需要 9.4+。
>
> 官方文档：
> [CREATE MATERIALIZED VIEW](https://www.postgresql.org/docs/current/sql-creatematerializedview.html) ·
> [REFRESH MATERIALIZED VIEW](https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html) ·
> [ALTER MATERIALIZED VIEW](https://www.postgresql.org/docs/current/sql-altermaterializedview.html)

## 表达式一览

| 表达式 | 用途 | PostgreSQL 专有选项 |
|---|---|---|
| `PostgresCreateMaterializedViewExpression` | `CREATE MATERIALIZED VIEW` | `schema`、`if_not_exists`、`storage_options`、`tablespace`、`column_aliases`、`with_data` |
| `PostgresRefreshMaterializedViewExpression` | `REFRESH MATERIALIZED VIEW` | `schema`、`concurrently`、`with_data` |
| `PostgresAlterMaterializedViewExpression` | `ALTER MATERIALIZED VIEW` | `schema`、动作列表 |
| `PostgresDropMaterializedViewExpression` | `DROP MATERIALIZED VIEW` | `schema`、`if_exists`、`cascade` |

核心库通用表达式（`CreateMaterializedViewExpression`、
`RefreshMaterializedViewExpression`、`DropMaterializedViewExpression`）仍可使用，
只是不携带 `schema` / `if_not_exists`。

## 创建物化视图

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

生成的 SQL（注意 PostgreSQL 语法顺序：列别名 → `WITH (...)` → `TABLESPACE`）：

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

视图会被注册，但在刷新前不可查询（需
`REFRESH MATERIALIZED VIEW ... WITH DATA`）。

## 存储参数

物化视图以普通堆表形式存储——服务端在 `fillRelOptions()` 中把
`RELKIND_MATVIEW` 映射到 `RELOPT_KIND_HEAP`
（`src/backend/access/common/reloptions.c`）。因此可用参数集合就是标准堆表
关系选项，已由 [`PostgresStorageParameter`](../../api/storage_parameters.md) 枚举：

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresStorageParameter

PostgresStorageParameter.FILLFACTOR.value                  # 'fillfactor'
PostgresStorageParameter.FILLFACTOR.value_type             # PostgresStorageParameterValueType.INT
PostgresStorageParameter.VACUUM_INDEX_CLEANUP.enum_values # ('auto', 'enabled', 'disabled')
PostgresStorageParameter.PARALLEL_WORKERS.min_version     # (11, 0, 0)
```

键名按该枚举校验——枚举外的名字会抛 `ValueError`。以下两类参数有意不纳入
枚举，需要显式传 `allow_unlisted_storage_parameters=True`：

* 带命名空间的参数（`toast.autovacuum_enabled` 等）；
* 表访问方法（`USING method`）注册的参数（例如列式 AM 的 `compresslevel`），
  因为 `add_reloption_kind()` / `add_*_reloption()` 是服务端导出的扩展点。

```python
create = PostgresCreateMaterializedViewExpression(
    dialect=dialect,
    view_name="columnar_summary",
    query=summary,
    storage_options={"compresslevel": 4},
    allow_unlisted_storage_parameters=True,
)
```

## 刷新

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresRefreshMaterializedViewExpression,
)

# 普通刷新
PostgresRefreshMaterializedViewExpression(dialect=dialect, name="sales_summary")

# 并发刷新——要求存在 UNIQUE 索引；PG < 9.4 会被拒绝
PostgresRefreshMaterializedViewExpression(
    dialect=dialect, name="sales_summary", schema="reporting", concurrently=True
)

# 填充 WITH NO DATA 的视图
PostgresRefreshMaterializedViewExpression(
    dialect=dialect, name="sales_daily", with_data=True
)
```

`concurrently=True` 在 PostgreSQL < 9.4 上抛 `UnsupportedFeatureError`——
通用表达式与 PostgreSQL 专有表达式两条路径都会拦截。

## 修改物化视图

PostgreSQL 恰好支持 5 种动作，**不含** `SET TABLESPACE`
（物化视图创建后无法迁移到其他表空间）。

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

多动作会渲染为多条语句，以 `";\n"` 连接：

```sql
ALTER MATERIALIZED VIEW "reporting"."sales_summary" RENAME TO "sales_summary_v2";
ALTER MATERIALIZED VIEW "reporting"."sales_summary" SET SCHEMA "archive";
ALTER MATERIALIZED VIEW "reporting"."sales_summary" SET (fillfactor = 90);
ALTER MATERIALIZED VIEW "reporting"."sales_summary" RESET (autovacuum_enabled);
ALTER MATERIALIZED VIEW "reporting"."sales_summary" OWNER TO CURRENT_USER
```

`CURRENT_ROLE` / `CURRENT_USER` / `SESSION_USER` 原样输出；其余 owner
按标识符加引号。

## 删除

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

## 内省

物化视图**不属于** `list_views()`（后者固定为 `relkind='v'`）。
请使用专用方法，同步与异步内省器都提供：

```python
views = backend.introspector.list_materialized_views()
for view in views:
    print(view.name, view.schema, view.extra)

info = backend.introspector.get_materialized_view_info("sales_summary")
exists = backend.introspector.materialized_view_exists("sales_summary")
```

每个 `ViewInfo` 携带：

| 字段 | 含义 |
|---|---|
| `definition` | 定义查询，来自 `pg_matviews` |
| `is_updatable` / `is_insertable` | 恒为 `False` |
| `extra['is_materialized']` | 恒为 `True`（用于区分物化视图行） |
| `extra['is_populated']` | `WITH NO DATA` 且从未刷新时为 `False` |
| `extra['has_unique_index']` | `REFRESH ... CONCURRENTLY` 的前置条件 |

`list_tables(table_type="MATERIALIZED VIEW")` 也会按 `relkind = 'm'` 过滤。

## 能力探测

```python
dialect.supports_materialized_view()                     # PG >= 9.3
dialect.supports_refresh_materialized_view()             # PG >= 9.3
dialect.supports_materialized_view_concurrent_refresh()  # PG >= 9.4
dialect.supports_materialized_view_if_not_exists()       # PG >= 9.4
dialect.supports_materialized_view_tablespace()          # PG >= 9.3
dialect.supports_materialized_view_storage_options()     # PG >= 9.3
dialect.supports_alter_materialized_view()               # PG >= 9.3
```

这些探针全部由 `PostgresMaterializedViewSupport` 声明。所有物化视图格式化器
都位于 `PostgresMaterializedViewMixin`，它在 `PostgresDialect` 的 MRO 中
**排在**核心 `ViewMixin` **之前**——回归测试会断言该顺序，因为一旦排到核心
mixin 之后，其格式化器会静默失效。

## 相关文档

* [DDL 操作](./README.md)
* [PostgreSQL 专有特性](../postgres_specific_features/protocol_support.md)
* 可运行示例：
  `src/rhosocial/activerecord/backend/impl/postgres/examples/ddl/materialized_view.py`
