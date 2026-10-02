# docs/zh_CN/postgres_specific_features/schema_namespace.md

# PostgreSQL Schema 命名空间

> 本文只讲 PostgreSQL 自己的那部分：`schema_name` 在本后端指向什么、限定名渲染
> 出来是什么样、表别名会改变什么、`search_path` 与 `__schema_name__` 如何分工，以及
> 哪些扩展的名称根本不加限定。
>
> 模型层的通用部分——怎么在模型上声明 `__schema_name__`、schema 何时进入 SQL、
> DDL 的边界、各后端支持矩阵——由核心库（`python-activerecord` 仓库）的
> `docs/modeling/schema_namespace.md` 讲，见
> [`docs/zh_CN/modeling/schema_namespace.md`][core-zh]。

[core-zh]: https://github.com/Rhosocial/python-activerecord/tree/main/docs/zh_CN/modeling/schema_namespace.md

除另有说明外，本文中的 SQL 都由对应的表达式对象配合 `PostgresDialect` 渲染得出，
不经过真实服务端；标注为「服务端响应」的内容引用的是 PostgreSQL 自身的报错文本。

## `schema_name` 在这里指向什么

`PostgresDialect` 实现了核心库的 `SchemaSupport` 协议，schema 这一组能力标志全为
`True`：

```python
dialect.supports_schema()                # True
dialect.supports_create_schema()         # True
dialect.supports_drop_schema()           # True
dialect.supports_schema_if_not_exists()  # True
dialect.supports_schema_if_exists()      # True
dialect.supports_schema_cascade()        # True
dialect.supports_schema_authorization()  # True
```

所以凡是核心库期待出现 `schema_name` 的地方，这里都能接受。至于这个值**指向
什么**，按 PostgreSQL 自己的定义来：当前 database 里的一个 schema。一个 database
里通常有好几个，它们是货真价实的命名空间——`"app"."orders"` 与 `"public"."orders"`
只是同名，毫无关系。

渲染出来是这样，都用双引号：

| 表达式 | SQL |
|---|---|
| `TableExpression(d, "orders", schema_name="app")` | `"app"."orders"` |
| `TableExpression(d, "orders")` | `"orders"` |
| `TableExpression(d, "orders", schema_name="app", alias="o")` | `"app"."orders" AS "o"` |

每一段各自加引号，所以大小写原样保留：`schema_name="ar_xcrm"` 渲染成 `"ar_xcrm"`，
`schema_name="MySchema"` 渲染成 `"MySchema"`。PostgreSQL 只在解析期把**未加引号**的
标识符折成小写，加了引号就按字面处理。折成大写是 Oracle 的行为，不是 PostgreSQL 的。

值里写点号同样不是分隔符，见[常见错误](#常见错误)。

## 在模型上声明

```python
from typing import ClassVar, Optional

class Order(ActiveRecord):
    __table_name__ = "orders"
    __schema_name__ = "shop"                 # -> "shop"."orders"
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
```

`__schema_name__` 是可选的，默认 `None`，也就是不加限定。一旦设上，模型构造出来的
每条语句都带着这个命名空间——`SELECT`、`WHERE`、`ORDER BY`、`INSERT`、`UPDATE`、
`DELETE` 一视同仁：

```python
Order.query().select(Order.c.id).to_sql()[0]
# SELECT "shop"."orders"."id" FROM "shop"."orders"

Order.query().where(Order.c.id > 1).to_sql()[0]
# SELECT * FROM "shop"."orders" WHERE "shop"."orders"."id" > %s
```

直接用表达式层构造的 DML 同样带限定：

```python
# INSERT INTO "shop"."orders"  VALUES (%s)
# UPDATE "shop"."orders" SET "user_id" = %s WHERE "shop"."orders"."id" = %s
# DELETE FROM "shop"."orders" WHERE "shop"."orders"."id" = %s
```

不写 `__schema_name__` 的模型不加限定，交给连接去决定：

```python
PlainOrder.query().select(PlainOrder.c.id).to_sql()[0]
# SELECT "plain_orders"."id" FROM "plain_orders"
```

命名空间只经由 `schema_name()` 读取一次，并在列表达式构造的那一刻传下去。因此构造
之后再改 `__schema_name__`，不会回头改写已经建好的表达式——请重建条件，或者改完之后
再建。

## DDL 自带 schema 参数

`__schema_name__` 只管读和写，构造 DDL 时不读它——迁移必须自己说清楚要哪个 schema。
好在凡涉及命名空间对象的语句都收自己的 schema 参数，不用再手拼限定名。

### 收 `schema_name` 的语句

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

`CREATE TABLE` 与 `DROP TABLE` 是仅有的两种不带自己 `schema_name` 的普通形式——它们要
的是一个限定过的 `TableExpression`。`(schema, table)` 元组之所以并非到处可用，也是
这个缘故：派生形式会做归一化，`CreateTableAsExpression(dialect, ("app", "t"), query)`
渲染成 `CREATE TABLE "app"."t" AS ...`，`CreateTableLikeExpression(dialect, ("app",
"t"), ("app", "src"))` 渲染成 `CREATE TABLE "app"."t" (LIKE "app"."src")`；而
`CreateTableExpression` 与 `DropTableExpression` 只收 `str` 或 `TableExpression`，
传元组会报：

```
TypeError: table must be str or TableExpression, got tuple
```

`CreateIndexExpression` 只有一个 `schema_name`，索引与它所依附的表共用它，两者落在同一个
schema。

### 本后端自己的语句写作 `schema`

PostgreSQL 特有的表达式大多把这个参数写作 `schema` 而不是 `schema_name`，物化视图那
几条最为明显：

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

扩展、枚举、分区、`COPY`、`VACUUM`、`ANALYZE`、`REINDEX`、`REPACK`、统计信息、
`pg_partman` 与 `COMMENT ON` 也都如此。索引那几条是例外，而且彼此还不一致：
`PostgresCreateIndexExpression` **完全不收** schema 参数，建出来的索引跟着连接的
`search_path` 走；`PostgresAlterIndexExpression` 与 `PostgresDropIndexExpression`
则收 `schema_name`。要在指定 schema 里建索引，请用核心库的
`CreateIndexExpression`。

关键字名随类而异，所以传之前先看签名：写错会在构造阶段报 `TypeError`。

domain 表达式对命名空间有两种叫法：`schema=`（较早，可按位置传）与 `schema_name=`
（仅关键字）。两个都传且值不同会报错，而且这道检查发生在**构造表达式时**，不是渲染时：

```
ValueError: schema and schema_name must match when both are provided
```

`schema_name` 默认是 `None`，也就是不加限定。传 `""` 会被拒绝，见[空串](#空串不等于不加限定而且报错来得比预期晚)。

### `CREATE SCHEMA` / `DROP SCHEMA` 是例外

这两条里的 schema 不是限定符，它**就是**对象本身：

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

## `search_path` 与 `__schema_name__` 各管一摊

`PostgresConnectionConfig.search_path` 是**服务端运行时参数**，不是 libpq 的连接
关键字。后端会先把它折进 `options`，以 `-c search_path=...` 的形式交给
`psycopg.connect()`；要是把它当作独立连接参数直接递给 libpq，连接会直接失败并报
`invalid connection option "search_path"`。`backend/backend.py` 里做这次折叠的注释
记录了正是这个原因：该字段过去让连接根本建不起来，而不只是没生效。

折进去的值会追加在 `options` 已有内容的后面，因此两者同时设置可以共存，而不是后者
覆盖前者：

```python
PostgresConnectionConfig(..., search_path="app", options={"datestyle": "ISO"})
# psycopg 收到 options='-c datestyle=ISO -c search_path=app'
```

libpq 解析这个字符串时按空白切分，所以 schema 列表里不要留空格：写
`search_path="app,public"`，不要写 `"app, public"`。

由此还有两条结论：

- **它在连接生命周期内固定。** 只在建连那一刻设一次，不能按查询切换，也不能按事务
  切换。驱动层的连接还开着 `autocommit = True`，而后端自己从不发 `SET` 或
  `SET LOCAL`——想在会话中途换 schema，那句 SQL 得你自己写。
- **`to_connection_string()` 有一处需注意。** 配置自带的 URI 生成器会把它塞进
  URI 的查询参数，而 libpq 不认这种写法：

  ```python
  config.to_connection_string()
  # postgres://postgres@localhost:5432/test?search_path=app,public
  # -> ProgrammingError: invalid URI query parameter: "search_path"
  ```

  走后端建连即可，它会折进 libpq 接受的 `options`。

**常规场景交给 `search_path`，`__schema_name__` 只留给例外：**

```python
config = PostgresConnectionConfig(
    ...,
    search_path="app,public",   # 普通表不加限定地解析
)
```

- **只有一个 schema** —— 干脆别设 `__schema_name__`。不加限定的名字配上
  `search_path`，DML、DDL 和内省三者才能彼此一致，也彻底不会有三段式列引用。
- **有多个 schema** —— 只在偏离 `search_path` 的那几个模型上设 `__schema_name__`。
  例外面越小，触发下面那些错误的可能越低；而且每多一处偏离，这个模型牵涉的每条
  语句就多一处三段式列引用。
- **按租户切 schema** —— 要么每个租户一个模型类，要么另做一套显式管理 `search_path`
  的方案；后者本库没有实现，因为 `search_path` 没法按查询移动。

`PostgresConnectionConfig.default_schema` 已废弃。它从来没有影响过生成的 SQL——不设
`__schema_name__` 的模型照样走 `search_path`——所以别再用它。但内省仍然会优先读它来
找 schema：先用 `default_schema`，再退到 `search_path` 的第一项，最后退到 `public`。
所以这里留一个过期值，内省可能报出一个你的 SQL 根本不用的 schema。

## 三段引用：别名一出现就塌成两段

没有别名的范围，既能用关系名指，也能用全名指。取了别名之后，关系名被**替换**掉了，
只剩别名可用；而且 PostgreSQL 拒绝对已取别名的范围做 schema 限定，因为那个位置已经
由别名占住：

| 范围 | 列引用 | 服务端响应 |
|---|---|---|
| `FROM "shop"."orders"` | `"shop"."orders"."id"` | 接受 |
| `FROM "shop"."orders"` | `"orders"."id"` | 接受 |
| `FROM "shop"."orders" AS "o"` | `"o"."id"` | 接受 |
| `FROM "shop"."orders" AS "o"` | `"shop"."orders"."id"` | `invalid reference to FROM-clause entry` |
| `FROM "shop"."orders" AS "o"` | `"orders"."id"` | `missing FROM-clause entry` |

框架遵守这条规则的方式是：没有别名就产出三段式，有别名就只产出别名：

```python
Order.query().select(Order.c.id).to_sql()[0]
# SELECT "shop"."orders"."id" FROM "shop"."orders"

Order.query().select(Order.c.with_table_alias("o").id).to_sql()[0]
# SELECT "o"."id" FROM "shop"."orders"
```

别名一出现就把 schema 抹掉，这件事发生在**构造**列表达式的时候，不是渲染的时候——
`FieldProxy` 一旦发现表别名生效，就把 `schema_name` 置成 `None`。PostgreSQL 方言里
那个 `format_column` 覆写并不是这条约束的执行者：它看的是*列*别名（`expr.alias`），
而这条路径上并没有设列别名；三段式那一支的守卫条件是 `schema_name and not alias`。
所以手工构造的 `Column` 会绕开这道防线：

```python
Column(dialect, "id", table="o", schema_name="shop").to_sql()[0]
# "shop"."o"."id"      <- PostgreSQL 不接受
```

反过来，带*列*别名的列引用，框架会把 schema 去掉，因为 PostgreSQL 要求如此：

```python
Column(dialect, "id", table="orders", schema_name="shop", alias="x").to_sql()[0]
# "orders"."id" AS "x"
```

### 跨 schema 的 join

每一边各自限定自己的范围，一条语句跨两个命名空间不用任何额外配置：

```python
Order.query().join(User, on=Order.c.user_id == User.c.id).select(
    Order.c.id, User.c.name
).to_sql()[0]
# SELECT "shop"."orders"."id", "crm"."users"."name"
#   FROM "shop"."orders" JOIN "crm"."users"
#   ON "shop"."orders"."user_id" = "crm"."users"."id"
```

只给一边取别名，也只有那一边的列引用变形，范围上的限定符照旧留着：

```python
Order.query().join(
    User, on=Order.c.user_id == User.c.with_table_alias("u").id, alias="u"
).select(Order.c.id, User.c.with_table_alias("u").name).to_sql()[0]
# SELECT "shop"."orders"."id", "u"."name"
#   FROM "shop"."orders" JOIN "crm"."users" AS "u"
#   ON "shop"."orders"."user_id" = "u"."id"
```

如果 join 条件还在引用没取别名的那个范围，框架会直接拦下来：

```
ValueError: cannot join crm.users with alias 'u' using a condition that still
refers to crm.users: an aliased range can only be addressed by its alias. Build
the condition from users.c.with_table_alias('u') so the reference and the alias
agree.
```

所以条件要从取了别名的那个访问器搭出来。自连接里这意味着**两边都取别名，或者两边都
不取**：左侧范围没有自己的 `alias=`，条件里把两种写法混在一起会被拒：

```python
Order.query().join(
    Order, on=Order.c.with_table_alias("c").id == Order.c.with_table_alias("p").user_id,
    alias="p",
).select(Order.c.with_table_alias("c").id, Order.c.with_table_alias("p").id).to_sql()[0]
# SELECT "c"."id", "p"."id"
#   FROM "shop"."orders" JOIN "shop"."orders" AS "p"
#   ON "c"."id" = "p"."user_id"
```

只给列这一侧取别名、范围那边不取，生成的 SQL 会被服务端拒绝，而不是被框架拦下：

```python
Order.query().select(Order.c.with_table_alias("o").id).to_sql()[0]
# SELECT "o"."id" FROM "shop"."orders"     <- missing FROM-clause entry
```

### 集合操作

`UNION`、`INTERSECT`、`EXCEPT` 自己不指名任何对象，因此无从限定。两个分支各自保留
自己的命名空间：

```python
Order.query().select(Order.c.id).union(User.query().select(User.c.id)).to_sql()[0]
# SELECT "shop"."orders"."id" FROM "shop"."orders"
#   UNION
#   SELECT "crm"."users"."id" FROM "crm"."users"
```

### CTE

CTE 的名字是给余下那条查询用的，不是给数据库用的，所以它自己永远不加限定——加了就
变成在某个 schema 里找一张叫 `"recent_orders"` 的表。它内部那条 SELECT 仍然带着模型
的 schema：

```python
WITH "recent_orders" AS (SELECT "shop"."orders"."id" FROM "shop"."orders")
SELECT "recent_orders"."id" FROM "recent_orders"
```

## 空串：不等于"不加限定"，而且报错来得比预期晚

`""` 是笔误，不是"不加限定"的另一种说法——那件事归 `None` 管。它会被拒绝，但**不是
在构造的时候**：表达式在构造阶段只负责收集参数，那时连方言都未必已经定下来，参数也
未必齐全，所以严格校验要等到渲染，因为只有那时整条语句才算拼完整。于是这个错比预期
晚一步才浮出来：

```python
class Bad(ActiveRecord):
    __table_name__ = "empties"
    __schema_name__ = ""

Bad.schema_name()                        # ''           —— 不报错
Bad.c.id                                 # Column       —— 不报错
Bad.query()                              # ActiveQuery  —— 不报错
Bad.query().select(Bad.c.id)             # ActiveQuery  —— 不报错
Bad.query().select(Bad.c.id).to_sql()    # ValueError   —— 到这里才抛出异常
```

报错信息会指明是哪个表达式：

```
ValueError: Column.schema_name must be a non-empty string; use None for an
unqualified reference
```

```
ValueError: TableExpression.schema_name must be a non-empty string; use None for
an unqualified reference
```

纯空白串与空串同样被拒——这道检查会先去掉空白再判断，所以 `"   "` 也会被拒。

非字符串同样被拒，只是换一条信息：

```
ValueError: TableExpression.schema_name must be a string or None, not int
```

`TruncateExpression` 抛的是 `TableExpression` 那条信息，因为它内部是靠一个
`TableExpression` 来渲染表名的——信息里指的是执行校验的那个对象，不是你写的语句。

之所以要拒而不是把 `""` 当作没有：`format_table` 判断是否加限定看的是
`bool(expr.schema_name)`，对 `""` 为假，于是走不加限定的分支。于是一个本来要
`app.users` 的调用会拿到 `users`，没有报错、没有警告、也没有行数变化可查——而只要
连接的 `search_path` 恰好含 `app`，这条语句就写到了另一张表上。

## 常见错误

**`__table_name__` 里写点号不算命名空间。** 标识符是当作一个整体加引号的：

```python
class User(ActiveRecord):
    __table_name__ = "app.users"

User.query().select(User.c.id).to_sql()[0]
# SELECT "app.users"."id" FROM "app.users"    -- relation "app.users" does not exist
```

没有任何东西会替你把它拆成 schema 和表名。请用 `__schema_name__`，或者给需要的那条
语句传一个限定过的 `TableExpression`。

**`__schema_name__` 里写点号同样不算命名空间。** 每一段是分开加引号的，所以点号留在
它所在的那一段里：

```python
TableExpression(dialect, "orders", schema_name="app.public").to_sql()[0]
# "app.public"."orders"     —— 一个真名叫 "app.public" 的 schema
```

**字段名里写 schema，那只是列名。** schema 限定的是**范围**，跟列怎么拼没有关系：

```python
class Report(ActiveRecord):
    __table_name__ = "reports"
    __schema_name__ = "app"
    app_total: Optional[int] = None

Report.query().select(Report.c.app_total).to_sql()[0]
# SELECT "app"."reports"."app_total" FROM "app"."reports"
```

**指望构造阶段就报错。** 在语句渲染出来之前，没有任何环节会拒绝一个不合法的
`schema_name`。模型层的错误因此能一路活过包括构建查询在内的所有步骤，直到渲染才
抛出异常——或者干脆像这道校验当初要防的那种情形一样，未声张地写到了 schema 里的另
一张表上。

**join 只给一边取别名，或只给列那一侧取别名。** 上面都已交代；一条规则即可概括：引用
与范围的别名必须一致。

**拿 `default_schema` 当 `search_path` 用。** 它已废弃，对生成的 SQL 不起作用。要改
请设 `search_path`。

## 问服务端当前 schema

```python
backend.get_current_schema()          # search_path 上第一个真正存在的 schema
await async_backend.get_current_schema()
```

问的是服务端的 `current_schema()`：沿 `search_path` 往后找，返回第一个确实存在的
schema。`search_path` 里可以列着并不存在的 schema，所以有可能一个都找不到——这时返回
`None` 而不是报错，原样给你。

## 扩展

`CREATE EXTENSION` 会为其目标 schema 加引号，因此混合大小写的 schema 可用：

```python
PostgresCreateExtensionExpression(dialect, "postgis", schema="My Schema").to_sql()[0]
# CREATE EXTENSION IF NOT EXISTS postgis SCHEMA "My Schema"
```

这里的参数写作 `schema=`，与本后端其余专有语句一致。

## orafce

orafce 默认把函数安装到 `oracle` schema，`functions.orafce` 中的每个工厂函数都会
据此对调用加 schema 限定。若扩展装在别处，传入 `schema=`：

```python
orafce.nvl(dialect, expr, "n/a")                # ORACLE.NVL(%s, %s)
orafce.nvl(dialect, expr, "n/a", schema="ext")  # EXT.NVL(%s, %s)
```

限定名以**不加引号**的形式输出，因为 `format_function_call` 会原样对 `func_name` 做
大写化。因此含大写字母、空格或首位数字的 `schema` 无法通过本模块表达——它会被服务端
折叠成另一个（通常不存在的）schema：

```
UserWarning: orafce schema 'Ext' is not a plain lowercase identifier; function
names are rendered unquoted and upper-cased, so the server will look for a
different schema. Use a lowercase name.
```

请改用纯小写的 schema 名，或把 orafce 装到默认 schema。

## PostGIS

`GEOMETRY` 与 `GEOGRAPHY` 不带 schema 限定符输出，`ST_*` 等调用同样如此。因此
PostGIS 必须安装到连接 `search_path` 上的某个 schema：

```python
config = PostgresConnectionConfig(..., search_path="public,extensions")
```

若把 PostGIS 装在别处且未扩展 `search_path`，所有 geometry 的 DDL 与 DML 语句都会报
`type "geometry" does not exist`。