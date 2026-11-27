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

pg_hint_plan 的提示是**附着在单条语句上的注释**，不是独立语句。builder 返回的是普通提示负载（`PostgresHint`），不拼接 SQL 字符串；只有 dialect formatter 负责渲染注释，注释附着在承载提示的语句上。

```python
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresHintedQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import (
    hint_leading, hint_hash_join, hint_seq_scan,
)
from rhosocial.activerecord.backend.impl.postgres.expression.hint import HintGroup

query = PostgresHintedQueryStatement(
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

`PostgresHintedInsertStatement`、`PostgresHintedUpdateStatement`、`PostgresHintedDeleteStatement` 与 `PostgresHintedExplainStatement` 同样接受 `hints=` 关键字。其中 EXPLAIN 版本会把注释写在 `EXPLAIN` 关键字之前，这正是 pg_hint_plan 要求的位置：

```python
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresHintedExplainStatement,
)

explained = PostgresHintedExplainStatement(
    dialect, query, hints=(hint_seq_scan(dialect, "a"),)
)
sql, params = explained.to_sql()
# /*+ SeqScan(a) */ EXPLAIN SELECT ...
```

提示负载是键齐全的普通字典（`PostgresHint.as_dict()`），因此能随 artifact 序列化往返；它不是表达式节点，也不会单独渲染。

提示注释位于语句之前，因此执行带提示的查询时要显式传 `stmt_type`：后端无法从 SQL 首词推断语句类型。

`PostgresHintedQueryStatement` 及其四个同类是 DSL 入口。可用 builder：`hint_seq_scan`、`hint_tid_scan`、`hint_index_scan`、`hint_index_only_scan`、`hint_bitmap_scan`、`hint_no_seq_scan`、`hint_no_index_scan`、`hint_hash_join`、`hint_merge_join`、`hint_nest_loop`、`hint_leading`、`hint_rows`、`hint_parallel`、`hint_set`。

标识符与 GUC 值都通过全匹配校验；注释终止符和语句分隔符会被拒绝。

## 能力探测

```python
dialect.supports_pg_hint_plan()          # 扩展已装且版本 >= 1.3
dialect.get_extension_version("pg_hint_plan")
```

## 另见

- **[unaccent](./unaccent.md)**：重音不敏感的文本归一化
- **[pg_trgm](./pg_trgm.md)**：三元组相似性检索
