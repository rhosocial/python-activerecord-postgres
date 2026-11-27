# pg_hint_plan

`pg_hint_plan` 允许用特殊 SQL 注释中的提示短语影响 PostgreSQL 的规划器，例如 `/*+ SeqScan(t) */`。当成本估算与规划器看不到的数据特性（列相关性、数据倾斜）不一致时很有用。

💡 *AI 提示词：* "什么时候适合用 pg_hint_plan 固定执行计划，而不是改统计信息或索引？"

## 可用性

`postgres-exts` 镜像在 PG12–18 上提供 `pg_hint_plan` 1.8.1。每个数据库需先启用：

```sql
CREATE EXTENSION pg_hint_plan;
```

**注意：** 仅 `CREATE EXTENSION` 不会安装 planner hook。除非服务端把 `pg_hint_plan` 放进 `shared_preload_libraries`，否则每个会话要先加载一次：

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import load_pg_hint_plan

backend.execute(*load_pg_hint_plan(dialect).to_sql())
```

## 构造提示

pg_hint_plan 的提示是**附着在单条语句上的注释**，不是独立语句。builder 返回的是普通字典负载（`HintPayload`），既不是 SQL 字符串也不是表达式节点；只有 dialect formatter 负责渲染注释。

提示是 PostgreSQL 专有语法，因此不放在 core 的语句上。后端**派生自己的语句类**，每个只增加一个关键字参数 `hints`（框架表达式/格式化规则的情形 B：`E_c = derive(E_core, +params_c)`，原参数保持不动）。派生类继承基类的 `format_method`，因此不新增表达式/格式化对；PostgreSQL formatter 同时处理通用表达式与派生表达式——通用语句的字节与以前完全一致，只有带非空提示的派生实例才会加前缀。

八条语句在 PostgreSQL 类上承载提示：

| PostgreSQL 类 | 语句 | 模块 |
|---|---|---|
| `PostgresQueryStatement` | `SELECT` | `expression/hinted_statements.py` |
| `PostgresInsertStatement` | `INSERT` | `expression/hinted_statements.py` |
| `PostgresUpdateStatement` | `UPDATE` | `expression/hinted_statements.py` |
| `PostgresDeleteStatement` | `DELETE` | `expression/hinted_statements.py` |
| `PostgresExplainStatement` | `EXPLAIN` | `expression/hinted_statements.py` |
| `PostgresCreateTableAsStatement` | `CREATE TABLE ... AS` | `expression/hinted_statements.py` |
| `PostgresCreateMaterializedViewStatement` | `CREATE MATERIALIZED VIEW ... AS` | `expression/ddl/mv.py` |
| `PostgresRefreshMaterializedViewStatement` | `REFRESH MATERIALIZED VIEW` | `expression/ddl/mv.py` |

最后两个在提示支持之前就已经是 PostgreSQL 派生类，本次就地扩展；其余六个是 core 语句在本后端的对应类。

通过语句的 `hints=` 关键字传入负载：

```python
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import (
    hint_leading, hint_hash_join, hint_seq_scan,
)
from rhosocial.activerecord.backend.impl.postgres.expression.hint import HintGroup

query = PostgresQueryStatement(
    dialect,
    select=[...],
    from_=...,
    hints=(
        hint_leading(dialect, HintGroup("a", "b"), "c"),   # Leading((a b) c)
        hint_hash_join(dialect, "a", "b"),
        hint_seq_scan(dialect, "a"),
    ),
)
sql, params = query.to_sql()
# /*+ Leading((a b) c) HashJoin(a b) SeqScan(a) */ SELECT ...
backend.execute(sql, params, options=ExecutionOptions(stmt_type=StatementType.DQL))
```

`PostgresExplainStatement` 会把注释写在 `EXPLAIN` 关键字之前，这正是 pg_hint_plan 要求的位置：

```python
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresExplainStatement,
)

explained = PostgresExplainStatement(
    dialect, query, hints=(hint_seq_scan(dialect, "a"),)
)
sql, params = explained.to_sql()
# /*+ SeqScan(a) */ EXPLAIN SELECT ...
```

无论外层是通用还是 PostgreSQL 的 `EXPLAIN`，只要被包裹的语句自身已带提示，它都会拒绝渲染（两个注释位置会有歧义）。

`hints=` 关键字同样被三条在运行时才规划语句的 DDL 语句接受：`PostgresCreateTableAsStatement`（`CREATE TABLE ... AS`）、`PostgresCreateMaterializedViewStatement`（`CREATE MATERIALIZED VIEW ... AS`）与 `PostgresRefreshMaterializedViewStatement`（`REFRESH MATERIALIZED VIEW`）。在 PG 18 + pg_hint_plan 1.8.1 上实测，三者都会因头部注释报告 `used hint` 并改用不同计划：

```python
from rhosocial.activerecord.backend.impl.postgres.expression.hinted_statements import (
    PostgresCreateTableAsStatement,
)

ctas = PostgresCreateTableAsStatement(
    dialect, Table(dialect, "ct"), query, hints=(hint_seq_scan(dialect, "t"),)
)
sql, params = ctas.to_sql()
# /*+ SeqScan(t) */ CREATE TABLE "ct" AS SELECT ...
```

对这三条语句，提示要交给**语句本身**，而不是它所包裹的查询：pg_hint_plan 拒绝一个语句中出现两个注释块，formatter 也会按名拒绝带提示的被包裹查询（`TypeError`，"a pg_hint_plan hint belongs to the outermost statement"）。`CREATE VIEW` 没有任何带提示的类：它在创建时不被规划，pg_hint_plan 会忽略该注释（实测：完全没有日志行）；视图体里的带提示查询会被拒绝，而不是悄悄失效。应改为提示读取该视图的查询。

提示负载是键齐全的普通字典（不适用的字段为 `None`/`()`，可用 `make_hint_payload()` 构造），因此能随 artifact 序列化往返；它不是表达式节点，也不会单独渲染。承载它的 PostgreSQL 语句才是拥有该参数的节点；方言不支持该注释语法时，对带提示的语句抛出 `UnsupportedFeatureError`，而不是悄悄丢弃提示：

```python
dialect.supports_pg_hint_plan()   # 仅在 pg_hint_plan 可用时为 True
```

提示注释位于语句之前，因此执行带提示的查询时要显式传 `stmt_type`：后端无法从 SQL 首词推断语句类型。

### 提示词表（26 种）

| 提示 | Builder | 版本门槛 |
|---|---|---|
| `SeqScan` | `hint_seq_scan` | — |
| `TidScan` | `hint_tid_scan` | — |
| `IndexScan` | `hint_index_scan` | — |
| `IndexOnlyScan` | `hint_index_only_scan` | — |
| `BitmapScan` | `hint_bitmap_scan` | — |
| `IndexScanRegexp` | `hint_index_scan_regexp` | — |
| `IndexOnlyScanRegexp` | `hint_index_only_scan_regexp` | — |
| `BitmapScanRegexp` | `hint_bitmap_scan_regexp` | — |
| `NoSeqScan` | `hint_no_seq_scan` | — |
| `NoTidScan` | `hint_no_tid_scan` | — |
| `NoIndexScan` | `hint_no_index_scan` | — |
| `NoIndexOnlyScan` | `hint_no_index_only_scan` | — |
| `NoBitmapScan` | `hint_no_bitmap_scan` | — |
| `DisableIndex` | `hint_disable_index` | pg_hint_plan ≥ 2.0 |
| `NestLoop` | `hint_nest_loop` | — |
| `HashJoin` | `hint_hash_join` | — |
| `MergeJoin` | `hint_merge_join` | — |
| `NoNestLoop` | `hint_no_nest_loop` | — |
| `NoHashJoin` | `hint_no_hash_join` | — |
| `NoMergeJoin` | `hint_no_merge_join` | — |
| `Memoize` | `hint_memoize` | PostgreSQL ≥ 17 |
| `NoMemoize` | `hint_no_memoize` | PostgreSQL ≥ 17 |
| `Leading` | `hint_leading` | — |
| `Rows` | `hint_rows` | — |
| `Parallel` | `hint_parallel` | — |
| `Set` | `hint_set` | — |

`*Regexp` builder 在目标之后接受 POSIX 正则，存入负载的 `indexes` 字段；`DisableIndex` 在同一字段放索引名。有版本门槛的提示仍可正常构造，仅在渲染时以 `UnsupportedFeatureError` 拒绝，异常中会点明当前服务端或扩展版本。

## 提示目标是什么

提示照**查询自己的写法**命名关系：pg_hint_plan 用提示文本与查询中的拼写做匹配，因此 builder 接受关系引用——`Table`（含 Node/Edge/Materialized/Foreign 表）、`NamedRelationRef`，或用于伪名称的普通字符串——并把它们解析成唯一一种拼写。实测规则：

- **别名优先。** `FROM t1 a` 匹配 `IndexScan(a)`；`IndexScan(t1)` 会被静默忽略。
- **永不输出 schema。** `IndexScan(s2.t1)` 根本不会被解析；而裸名 `IndexScan(t1)` 仍能匹配写作 `FROM s2.t1` 的引用。
- **同名表有歧义。** `FROM t1 a JOIN s2.t1 b` 用名字匹配不到任何一张；只能用别名区分。
- **引号是精确匹配。** 带引号的名字（`"Mixed"`、`"Odd Name"`）精确匹配；小写化后的拼写匹配不上。解析器仅在对象的 `name_need_quote` 为真时加引号，绝不自行小写化。
- **视图不能被提示。** 对视图的 `IndexScan(v1)` 会被报告为未使用的提示；应改为提示该视图展开后的查询所涉及的基础表。

```python
hint_index_scan(dialect, NamedRelationRef(dialect, Table(dialect, "orders"), alias="o"))
# /*+ IndexScan(o) */

hint_seq_scan(dialect, Table(dialect, "orders", schema_name="sales"))
# /*+ SeqScan("orders") */   -- 提示里永不写 schema
```

普通字符串原样透传：这是伪名称（`*VALUES*`、`ANY_subquery`）或调用方已知别名的逃生口。解析发生在 builder 执行时；从 artifact 解码回来的负载已持有解析后的字符串，会原样渲染、不会再次解析。

标识符与 GUC 值都通过全匹配校验；注释终止符和语句分隔符会被拒绝。

## 能力探测

```python
dialect.supports_pg_hint_plan()          # 扩展已装且版本 >= 1.3
dialect.get_extension_version("pg_hint_plan")
```

上述版本门槛分别用 `dialect.version`（服务端，`Memoize`/`NoMemoize`）和 `get_extension_version("pg_hint_plan")`（扩展，`DisableIndex`）探测。

## 另见

- **[unaccent](./unaccent.md)**：重音不敏感的文本归一化
- **[pg_trgm](./pg_trgm.md)**：三元组相似性检索
