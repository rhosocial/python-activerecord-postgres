# PostgreSQL Schema 命名空间

> PostgreSQL 在 schema 处理上的特有行为。模型层 API（如何声明
> `__schema_name__`、schema 何时出现在 SQL 中、DDL 的边界）请参见核心库文档
> *Schema 命名空间*。

## 别名优先于 schema

写作 `"shop"."orders"` 的范围，可以用两段式（`"orders"."id"`）**或**三段式
（`"shop"."orders"."id"`）引用。而写作 `"shop"."orders" AS "o"` 的范围，
**只能**用 `"o"."id"` 引用：

| 范围 | 列引用 | 结果 |
|---|---|---|
| `FROM "shop"."orders"` | `"shop"."orders"."id"` | 合法 |
| `FROM "shop"."orders"` | `"orders"."id"` | 合法 |
| `FROM "shop"."orders" AS "o"` | `"o"."id"` | 合法 |
| `FROM "shop"."orders" AS "o"` | `"shop"."orders"."id"` | `invalid reference to FROM-clause entry` |
| `FROM "shop"."orders" AS "o"` | `"orders"."id"` | `missing FROM-clause entry` |

框架在表别名生效时即丢弃 schema 限定符，因此 `Model.c.with_table_alias("o")`
会生成 `"o"."col"`，需与 `join(..., alias="o")` 配对使用。

注意：执行该约束的并不是 `PostgresColumnMixin.format_column` —— 它检查的是
*列*别名，而该路径上列别名并未设置。真正的保证来自 `FieldProxy`。手工构造
`Column` 会绕过它，可能产出 PostgreSQL 拒绝的 SQL。

## `search_path` 是建连期设置

`PostgresConnectionConfig.search_path` 会作为 libpq 参数传给
`psycopg.connect()`。因此它在连接生命周期内固定，**无法**按查询或按事务切换。
后端在受管事务之外还以 `autocommit=True` 运行，`SET LOCAL search_path`
没有可依附的事务。

实际影响：

- 未设置 `__schema_name__` 的模型始终通过同一条路径解析。
- 按租户划分 schema 需要每个租户一个模型类，或另行设计显式管理
  `search_path` 的架构（本库未实现）。
- `default_schema` 从未影响生成的 SQL。请改用 `search_path`。

## 扩展

`CREATE EXTENSION` 会为其目标 schema 加引号，因此混合大小写的 schema 可用：

```python
CreateExtensionExpression(dialect, "postgis", schema="My Schema")
# CREATE EXTENSION IF NOT EXISTS postgis SCHEMA "My Schema"
```

## orafce

orafce 默认把函数安装到 `oracle` schema，`functions.orafce` 中的每个工厂函数
都会据此对调用加 schema 限定。若扩展装在别处，传入 `schema=`：

```python
from ...functions import orafce
orafce.nvl(dialect, expr, "n/a")                      # ORACLE.NVL(...)
orafce.nvl(dialect, expr, "n/a", schema="ext")        # EXT.NVL(...)
```

限定名以**不加引号**的形式输出，因为 `format_function_call` 会原样对
`func_name` 做大写化。因此含大写字母、空格或首位数字的 `schema` 无法通过本模块
表达 —— 它会被服务端折叠成另一个（通常不存在的）schema。此时会发出
`UserWarning`；请改用纯小写的 schema 名，或把 orafce 装到默认 schema。

## PostGIS

`GEOMETRY` 与 `GEOGRAPHY` 不带 schema 限定符输出，`ST_*` 与
`ST_GeogFromText` 等调用同样如此。因此 PostGIS 必须安装到连接 `search_path`
上的某个 schema：

```python
config = PostgresConnectionConfig(..., search_path="public,extensions")
```

若把 PostGIS 装在别处且未扩展 `search_path`，所有 geometry 的 DDL 与 DML
语句都会报 `type "geometry" does not exist`。
