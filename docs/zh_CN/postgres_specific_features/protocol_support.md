# PostgreSQL 协议支持矩阵

本文档提供了 PostgreSQL 后端支持的 SQL 方言协议的完整矩阵，以及版本特定的功能支持。

## 概述

PostgreSQL 方言实现了来自 `rhosocial.activerecord.backend.dialect.protocols` 模块的众多协议，为数据库特定功能提供细粒度的功能检测和优雅的错误处理。

## 协议支持矩阵

### 查询相关协议

| 协议 | 支持状态 | 版本要求 | 备注 |
|----------|---------------|---------------------|-------|
| **WindowFunctionSupport** | ✅ 完全支持 | ≥ 8.4 | 支持窗口函数和 frame 子句 |
| **CTESupport** | ✅ 完全支持 | ≥ 8.4 | 支持基本和递归 CTE；MATERIALIZED 提示 ≥ 12 |
| **SetOperationSupport** | ✅ 完全支持 | 所有版本 | 完全支持 UNION、UNION ALL、INTERSECT、EXCEPT |
| **WildcardSupport** | ✅ 完全支持 | 所有版本 | 支持 SELECT * 语法 |
| **JoinSupport** | ✅ 完全支持 | 所有版本 | 支持所有连接类型（INNER、LEFT、RIGHT、FULL、CROSS、NATURAL） |
| **FilterClauseSupport** | ✅ 完全支持 | ≥ 9.4 | 聚合函数的 FILTER 子句 |
| **ReturningSupport** | ✅ 完全支持 | ≥ 8.2 | INSERT/UPDATE/DELETE 的 RETURNING 子句 |
| **UpsertSupport** | ✅ 完全支持 | ≥ 9.5 | ON CONFLICT DO UPDATE/NOTHING 语法 |
| **LateralJoinSupport** | ✅ 完全支持 | ≥ 9.3 | LATERAL 连接 |
| **ArraySupport** | ✅ 完全支持 | 所有版本 | 原生数组类型和操作 |
| **JSONSupport** | ✅ 完全支持 | ≥ 9.2 | JSON 类型；JSONB ≥ 9.4；JSON_TABLE ≥ 12 |
| **ExplainSupport** | ✅ 完全支持 | 所有版本 | EXPLAIN 和 EXPLAIN ANALYZE 支持多种格式 |
| **GraphSupport** | ❌ 不支持 | N/A | 无原生图查询 MATCH 子句 |
| **LockingSupport** | ✅ 部分支持 | ≥ 9.5 | FOR UPDATE；SKIP LOCKED ≥ 9.5 |
| **MergeSupport** | ✅ 完全支持 | ≥ 15.0 | MERGE 语句 |
| **OrderedSetAggregationSupport** | ✅ 完全支持 | ≥ 9.4 | 有序集聚合函数 |
| **TemporalTableSupport** | ❌ 不支持 | N/A | 无内置时态表 |
| **QualifyClauseSupport** | ❌ 不支持 | N/A | 无 QUALIFY 子句 |

### DDL 相关协议

| 协议 | 支持状态 | 版本要求 | 备注 |
|----------|---------------|---------------------|-------|
| **TableSupport** | ✅ 完全支持 | 所有版本 | CREATE TABLE、DROP TABLE、ALTER TABLE 支持所有功能 |
| **ViewSupport** | ✅ 完全支持 | 所有版本 | CREATE VIEW、DROP VIEW 支持所有选项 |
| **TruncateSupport** | ✅ 完全支持 | 所有版本 | TRUNCATE TABLE；RESTART IDENTITY ≥ 8.4；CASCADE |
| **SchemaSupport** | ✅ 完全支持 | 所有版本 | CREATE SCHEMA、DROP SCHEMA 支持所有选项 |
| **IndexSupport** | ✅ 完全支持 | 所有版本 | CREATE INDEX、DROP INDEX 支持所有功能 |
| **SequenceSupport** | ✅ 完全支持 | 所有版本 | CREATE SEQUENCE、DROP SEQUENCE |

## 按协议详细说明功能支持

### SetOperationSupport（集合操作支持）

PostgreSQL 为 SQL 集合操作提供全面支持：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_union()` | ✅ | 所有版本 | UNION 操作 |
| `supports_union_all()` | ✅ | 所有版本 | UNION ALL 操作 |
| `supports_intersect()` | ✅ | 所有版本 | INTERSECT 操作 |
| `supports_except()` | ✅ | 所有版本 | EXCEPT 操作 |
| `supports_set_operation_order_by()` | ✅ | 所有版本 | 集合操作中的 ORDER BY 子句 |
| `supports_set_operation_limit_offset()` | ✅ | 所有版本 | 集合操作中的 LIMIT/OFFSET |
| `supports_set_operation_for_update()` | ✅ | 所有版本 | 集合操作中的 FOR UPDATE 子句 |

**示例：**
```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

dialect = PostgresDialect()
assert dialect.supports_union() # True
assert dialect.supports_intersect() # True
assert dialect.supports_except() # True
```

### TruncateSupport（截断表支持）

PostgreSQL 支持带有多个数据库特定功能的 TRUNCATE TABLE：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_truncate()` | ✅ | 所有版本 | TRUNCATE 语句 |
| `supports_truncate_table_keyword()` | ✅ | 所有版本 | TABLE 关键字（可选但支持） |
| `supports_truncate_restart_identity()` | ✅ | ≥ 8.4 | RESTART IDENTITY 重置序列 |
| `supports_truncate_cascade()` | ✅ | 所有版本 | CASCADE 截断依赖表 |

**版本特定行为：**

- **PostgreSQL < 8.4**：仅基本 TRUNCATE TABLE
- **PostgreSQL ≥ 8.4**：添加 RESTART IDENTITY 支持

**示例：**
```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.expression.statements import TruncateExpression

# 基本截断
dialect = PostgresDialect()
expr = TruncateExpression(dialect, table_name="users")
sql, params = dialect.format_truncate_statement(expr)
# 结果：TRUNCATE TABLE "users"

# 使用 RESTART IDENTITY（PostgreSQL 8.4+）
expr = TruncateExpression(dialect, table_name="users", restart_identity=True)
sql, params = dialect.format_truncate_statement(expr)
# 结果：TRUNCATE TABLE "users" RESTART IDENTITY"

# 使用 CASCADE
expr = TruncateExpression(dialect, table_name="orders", cascade=True)
sql, params = dialect.format_truncate_statement(expr)
# 结果：TRUNCATE TABLE "orders" CASCADE"
```

### CTESupport（公共表表达式支持）

带有版本依赖功能的公共表表达式：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_basic_cte()` | ✅ | ≥ 8.4 | 基本 CTE（WITH 子句） |
| `supports_recursive_cte()` | ✅ | ≥ 8.4 | 递归 CTE（WITH RECURSIVE） |
| `supports_materialized_cte()` | ✅ | ≥ 12.0 | MATERIALIZED/NOT MATERIALIZED 提示 |

### WindowFunctionSupport（窗口函数支持）

自 PostgreSQL 8.4 起完全支持窗口函数：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_window_functions()` | ✅ | ≥ 8.4 | 窗口函数（OVER 子句） |
| `supports_window_frame_clause()` | ✅ | ≥ 8.4 | ROWS/RANGE frame 子句 |

### JSONSupport（JSON 支持）

JSON 功能随版本逐步支持：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_json_type()` | ✅ | ≥ 9.2 | JSON 数据类型 |
| `supports_jsonb()` | ✅ | ≥ 9.4 | JSONB 数据类型（二进制格式） |
| `supports_json_table()` | ✅ | ≥ 12.0 | JSON_TABLE 函数 |

### AdvancedGroupingSupport（高级分组支持）

高级分组结构：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_rollup()` | ✅ | ≥ 9.5 | ROLLUP 分组 |
| `supports_cube()` | ✅ | ≥ 9.5 | CUBE 分组 |
| `supports_grouping_sets()` | ✅ | ≥ 9.5 | GROUPING SETS |

### UpsertSupport（Upsert 支持）

INSERT ... ON CONFLICT 支持：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_upsert()` | ✅ | ≥ 9.5 | 通过 ON CONFLICT 实现 UPSERT |
| `get_upsert_syntax_type()` | ✅ | ≥ 9.5 | 返回 "ON CONFLICT" |

### LateralJoinSupport（LATERAL 连接支持）

LATERAL 连接支持：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_lateral_join()` | ✅ | ≥ 9.3 | LATERAL 连接 |

### LockingSupport（锁定支持）

行级锁定功能：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_for_update_skip_locked()` | ✅ | ≥ 9.5 | FOR UPDATE SKIP LOCKED |

### MergeSupport（MERGE 支持）

MERGE 语句支持：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_merge_statement()` | ✅ | ≥ 15.0 | MERGE 语句 |

### OrderedSetAggregationSupport（有序集聚合支持）

有序集聚合函数：

| 功能 | 支持 | 版本 | 描述 |
|---------|---------|---------|-------------|
| `supports_ordered_set_aggregation()` | ✅ | ≥ 9.4 | PERCENTILE_CONT、PERCENTILE_DISC 等 |

## PostgreSQL 特定协议

除了标准协议外，PostgreSQL 还提供数据库特定协议：

| 协议 | 描述 |
|----------|-------------|
| **PostgresExtensionSupport** | 扩展检测和管理（PostGIS、pgvector 等） |
| **PostgresMaterializedViewSupport** | 物化视图支持 CONCURRENTLY 刷新 |
| **PostgresTableSupport** | 表特定功能（INHERITS、分区） |
| **PostgresVectorSupport** | pgvector 扩展，用于向量相似性搜索 |
| **PostgresSpatialSupport** | PostGIS 扩展，用于空间数据 |
| **PostgresTrigramSupport** | pg_trgm 扩展，用于三元组相似性 |
| **PostgresHstoreSupport** | hstore 扩展，用于键值存储 |

## 版本兼容性矩阵

### PostgreSQL 8.x 系列

| 版本 | 添加的关键功能 |
|---------|-------------------|
| 8.2 | RETURNING 子句 |
| 8.3 | 窗口函数框架 |
| 8.4 | 完整窗口函数、递归 CTE、TRUNCATE RESTART IDENTITY |

### PostgreSQL 9.x 系列

| 版本 | 添加的关键功能 |
|---------|-------------------|
| 9.2 | JSON 数据类型 |
| 9.3 | LATERAL 连接、物化视图 |
| 9.4 | JSONB、FILTER 子句、有序集聚合 |
| 9.5 | UPSERT（ON CONFLICT）、FOR UPDATE SKIP LOCKED、ROLLUP/CUBE/GROUPING SETS |

### PostgreSQL 10.x - 14.x 系列

| 版本 | 添加的关键功能 |
|---------|-------------------|
| 12 | MATERIALIZED CTE 提示、JSON_TABLE |
| 13 | （增量改进） |
| 14 | （性能改进） |

### PostgreSQL 15.x 及更高版本

| 版本 | 添加的关键功能 |
|---------|-------------------|
| 15 | MERGE 语句 |

## 使用示例

### 检查协议支持

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.dialect.protocols import (
    SetOperationSupport, TruncateSupport, CTESupport
)

# 为 PostgreSQL 13 创建方言
dialect = PostgresDialect(version=(13, 0, 0))

# 检查协议实现
assert isinstance(dialect, SetOperationSupport)
assert isinstance(dialect, TruncateSupport)
assert isinstance(dialect, CTESupport)

# 检查特定功能
assert dialect.supports_truncate_restart_identity() # True (≥ 8.4)
assert dialect.supports_materialized_cte() # True (≥ 12)
assert dialect.supports_merge_statement() # False (requires ≥ 15)
```

### 版本特定功能检测

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

# 旧 PostgreSQL 版本
old_dialect = PostgresDialect(version=(8, 3, 0))
assert not old_dialect.supports_truncate_restart_identity() # False (requires ≥ 8.4)

# 新 PostgreSQL 版本
new_dialect = PostgresDialect(version=(15, 0, 0))
assert new_dialect.supports_merge_statement() # True (≥ 15)
assert new_dialect.supports_truncate_restart_identity() # True (≥ 8.4)
```

## 另见

- [PostgreSQL 官方文档](https://www.postgresql.org/docs/current/)
- [PostgreSQL 功能矩阵](https://www.postgresql.org/about/featurematrix/)
- [rhosocial.activerecord 后端开发指南](../../../python-activerecord/docs/backend_development.md)
