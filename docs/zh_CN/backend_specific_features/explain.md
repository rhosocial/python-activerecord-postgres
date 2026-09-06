# EXPLAIN 支持

## 概述

PostgreSQL 后端提供对 `EXPLAIN` 语句的完整支持，可用于分析查询性能并查看执行计划。该功能通过统一的 API 与 ActiveRecord 查询构建器集成，同时暴露 PostgreSQL 特有的行为。

主要能力：
- 基础 `EXPLAIN`，用于查看执行计划
- `EXPLAIN ANALYZE`，返回实际运行时统计数据（所有现代版本均支持）
- 多种输出格式：`TEXT`、`XML`、`JSON`、`YAML`
- PostgreSQL 括号式选项语法（`EXPLAIN (ANALYZE, FORMAT JSON)`）
- 结构化结果解析，通过 `PostgresExplainResult` 访问
- 索引使用分析辅助方法（`Seq Scan`、`Index Scan`、`Index Only Scan`）
- 同步与异步双模式支持

## 基本用法

### 通过查询构建器使用 EXPLAIN

```python
from rhosocial.activerecord.backend.expression.statements import ExplainFormat

# 简单 EXPLAIN — 返回 PostgresExplainResult
result = User.query().explain().all()

# 查看生成的 SQL 前缀
print(result.sql)  # EXPLAIN SELECT ...

# 打印执行耗时（秒）
print(result.duration)
```

### 读取结果行

```python
result = User.query().explain().all()

for row in result.rows:
    print(row.line)
```

每行是一个 `PostgresExplainPlanLine`，只有一个字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `line` | `str` | 一行计划文本（如 `"Seq Scan on users  (cost=0.00..8.27 rows=1 width=...)"`） |

PostgreSQL 将每个计划节点以缩进文本行的形式写入 `"QUERY PLAN"` 列。所有行合在一起构成完整的执行计划树。

## 输出格式

PostgreSQL 支持四种输出格式，所有现代服务器版本均可使用：

### TEXT 格式（默认）

```python
# 默认情况下不生成 FORMAT 选项
result = User.query().explain().all()
# 生成: EXPLAIN SELECT ...
```

### JSON 格式

```python
result = User.query().explain(format=ExplainFormat.JSON).all()
# 生成: EXPLAIN (FORMAT JSON) SELECT ...
```

### XML 格式

```python
result = User.query().explain(format=ExplainFormat.XML).all()
# 生成: EXPLAIN (FORMAT XML) SELECT ...
```

### YAML 格式

```python
result = User.query().explain(format=ExplainFormat.YAML).all()
# 生成: EXPLAIN (FORMAT YAML) SELECT ...
```

## EXPLAIN ANALYZE

`EXPLAIN ANALYZE` 会实际执行查询，并在代价估算的基础上同时返回真实的运行时统计数据，可用于对比预估行数与实际行数及耗时。

```python
# 基础 ANALYZE
result = User.query().explain(analyze=True).all()
# 生成: EXPLAIN (ANALYZE) SELECT ...

# 带 JSON 格式的 ANALYZE
result = User.query().explain(analyze=True, format=ExplainFormat.JSON).all()
# 生成: EXPLAIN (ANALYZE, FORMAT JSON) SELECT ...

# 带 YAML 格式的 ANALYZE
result = User.query().explain(analyze=True, format=ExplainFormat.YAML).all()
# 生成: EXPLAIN (ANALYZE, FORMAT YAML) SELECT ...
```

**重要说明：**
- `EXPLAIN ANALYZE` **会真正执行查询**。在生产环境中对写操作使用时请谨慎评估副作用。
- 所有现代 PostgreSQL 版本均支持，在支持的版本范围内无最低版本限制。

## 生成的 SQL 语法

PostgreSQL 使用括号式选项语法，与 MySQL 的平铺式语法不同：

```
EXPLAIN [ ( option [, ...] ) ] 语句
```

多个选项在括号内以逗号分隔：

```python
# 无选项
# → EXPLAIN SELECT ...

# 仅 analyze=True
# → EXPLAIN (ANALYZE) SELECT ...

# 仅指定 format
# → EXPLAIN (FORMAT JSON) SELECT ...

# 同时指定两者
# → EXPLAIN (ANALYZE, FORMAT JSON) SELECT ...
```

`ANALYZE` 选项始终在选项列表中排在 `FORMAT` 之前。

## 索引使用分析

`PostgresExplainResult` 提供基于文本匹配的索引使用快速评估，通过对所有计划行进行关键词搜索：

```python
result = User.query().where(User.c.email == "alice@example.com").explain().all()

usage = result.analyze_index_usage()
print(usage)  # "full_scan" | "index_with_lookup" | "covering_index" | "unknown"

# 便捷属性
if result.is_full_scan:
    print("警告：检测到顺序扫描")

if result.is_covering_index:
    print("最优：正在使用 Index Only Scan")

if result.is_index_used:
    print("索引已被使用")
```

### 索引使用判断规则

分析将所有计划行拼接后进行大小写不敏感的文本匹配：

| 匹配关键词（不区分大小写） | `analyze_index_usage()` 返回值 |
|---|---|
| `"INDEX ONLY SCAN"`（优先检查） | `"covering_index"` |
| `"INDEX SCAN"`、`"BITMAP INDEX SCAN"` 或 `"BITMAP HEAP SCAN"` | `"index_with_lookup"` |
| `"SEQ SCAN"` | `"full_scan"` |
| 以上均不匹配 | `"unknown"` |

**注意：** `"INDEX ONLY SCAN"` 优先于 `"INDEX SCAN"` 检查，以避免误判。

## 异步 API

异步后端提供相同接口，在终止方法处使用 `await`：

```python
async def analyze_queries():
    # 简单异步 EXPLAIN
    result = await User.query().explain().all_async()

    # 带 JSON 格式的 ANALYZE
    result = await User.query().explain(
        analyze=True,
        format=ExplainFormat.JSON
    ).all_async()

    for row in result.rows:
        print(row.line)
```

## 运行时检查格式支持

使用 `dialect` 方法在调用前检查选项可用性：

```python
dialect = backend.dialect

# 检查 ANALYZE 支持（PostgreSQL 始终为 True）
if dialect.supports_explain_analyze():
    result = User.query().explain(analyze=True).all()

# 检查特定格式（四种格式始终支持）
if dialect.supports_explain_format("JSON"):
    result = User.query().explain(format=ExplainFormat.JSON).all()
```

## 与复杂查询组合使用

EXPLAIN 可与完整的查询构建器链式调用配合：

```python
# JOIN 查询
result = (
    User.query()
    .join(Order, User.c.id == Order.c.user_id)
    .where(User.c.status == "active")
    .explain(format=ExplainFormat.JSON)
    .all()
)

# GROUP BY / 聚合
result = (
    Order.query()
    .group_by(Order.c.user_id)
    .explain(analyze=True)
    .count(Order.c.id)
)

# 子查询
result = (
    User.query()
    .where(User.c.id.in_(Order.query().select(Order.c.user_id)))
    .explain()
    .all()
)
```

## 与 MySQL EXPLAIN 的对比

两个后端共享相同的查询构建器 API，但在 SQL 语法和结果结构上存在差异：

| 特性 | PostgreSQL | MySQL |
|---|---|---|
| SQL 语法 | `EXPLAIN (ANALYZE, FORMAT JSON)` | `EXPLAIN ANALYZE FORMAT=JSON` |
| 选项分隔符 | 括号内逗号分隔 | 空格分隔，无括号 |
| 支持的格式 | TEXT、XML、JSON、YAML | TEXT、JSON（5.6.5+）、TREE（8.0.16+） |
| ANALYZE 支持 | 所有现代版本 | 8.0.18+ |
| 结果行类型 | `PostgresExplainPlanLine`（1 个字段） | `MySQLExplainRow`（12 个字段） |
| 覆盖索引标志 | 计划文本中含 `"Index Only Scan"` | `extra` 列含 `"Using index"` |

## 版本兼容性

| 功能 | 最低 PostgreSQL 版本 |
|---|---|
| 基础 `EXPLAIN` | 所有支持版本 |
| `EXPLAIN FORMAT=TEXT/XML/JSON/YAML` | 所有支持版本 |
| `EXPLAIN ANALYZE` | 所有支持版本 |
| `EXPLAIN (ANALYZE, FORMAT JSON)` | 所有支持版本 |

PostgreSQL 后端面向 PostgreSQL 9.6 及更高版本；本文档描述的所有 EXPLAIN 功能在 9.6+ 均可使用。

## API 参考

### PostgreSQL 使用的 `ExplainOptions` 字段

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `analyze` | `bool` | `False` | 生成 `ANALYZE` 选项 |
| `format` | `Optional[ExplainFormat]` | `None` | 输出格式（`TEXT`/`XML`/`JSON`/`YAML`） |

其他 `ExplainOptions` 字段（如 `verbose`、`buffers`、`timing`、`settings`）会被接受，但当前 PostgreSQL 方言实现尚未将其写入生成的 SQL。

### `PostgresExplainResult` 方法

| 方法 / 属性 | 说明 |
|---|---|
| `rows` | `List[PostgresExplainPlanLine]`，解析后的计划行 |
| `sql` | 被 EXPLAIN 的完整 SQL 字符串 |
| `duration` | 查询执行耗时（秒） |
| `raw_rows` | psycopg 返回的原始行数据（包含 `"QUERY PLAN"` 键的字典列表） |
| `analyze_index_usage()` | 返回 `"full_scan"`、`"index_with_lookup"`、`"covering_index"` 或 `"unknown"` |
| `is_full_scan` | `analyze_index_usage() == "full_scan"` 时为 `True` |
| `is_index_used` | 有任意索引被使用时为 `True` |
| `is_covering_index` | 使用了 Index Only Scan 时为 `True` |

### `PostgresDialect` 方法

| 方法 | 返回值 | 说明 |
|---|---|---|
| `supports_explain_analyze()` | `bool` | PostgreSQL 始终为 `True` |
| `supports_explain_format(fmt)` | `bool` | `TEXT`、`XML`、`JSON`、`YAML` 均为 `True` |
| `format_explain_statement(expr)` | `str` | 构建 `EXPLAIN [(ANALYZE, FORMAT X)]` 前缀 |

## 最佳实践

- **程序化分析推荐使用 JSON 格式。** `FORMAT JSON` 提供最为详细的结构化计划树，比纯文本更易解析。
- **`EXPLAIN ANALYZE` 仅用于开发阶段。** 该语句会真正执行查询，仅在需要实际行数和耗时数据时使用。
- **针对不同受众选择不同格式。** 人工查阅使用 `TEXT`，工具集成使用 `JSON`/`YAML`，XML 报表管道使用 `XML`。
- **结合索引分析。** 在集成测试中使用 `result.is_full_scan` 作为快速冒烟测试，及时发现意外的顺序扫描。

💡 *AI 提示词：说明如何在 rhosocial-activerecord 中使用 PostgreSQL 的 EXPLAIN ANALYZE FORMAT JSON 诊断慢查询。*
