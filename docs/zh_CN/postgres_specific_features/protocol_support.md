# PostgreSQL 协议支持矩阵

本文档提供了 PostgreSQL 后端支持的 SQL 方言协议的完整矩阵，以及特定版本的功能支持。

## 概述

PostgreSQL 方言实现了 `rhosocial.activerecord.backend.dialect.protocols` 模块中的众多协议，为数据库特定功能提供细粒度的特性检测和优雅的错误处理。

## 协议支持矩阵

### 查询相关协议

| 协议 | 支持状态 | 版本要求 | 备注 |
|------|---------|---------|------|
| **WindowFunctionSupport** | ✅ 完整 | ≥ 8.4 | 支持窗口函数和帧子句 |
| **CTESupport** | ✅ 完整 | ≥ 8.4 | 支持基本和递归 CTE；MATERIALIZED 提示 ≥ 12 |
| **SetOperationSupport** | ✅ 完整 | 所有版本 | 完整支持 UNION、UNION ALL、INTERSECT、EXCEPT |
| **WildcardSupport** | ✅ 完整 | 所有版本 | 支持 SELECT * 语法 |
| **JoinSupport** | ✅ 完整 | 所有版本 | 支持所有连接类型（INNER、LEFT、RIGHT、FULL、CROSS、NATURAL） |
| **FilterClauseSupport** | ✅ 完整 | ≥ 9.4 | 聚合函数的 FILTER 子句 |
| **ReturningSupport** | ✅ 完整 | ≥ 8.2 | INSERT/UPDATE/DELETE 的 RETURNING 子句 |
| **UpsertSupport** | ✅ 完整 | ≥ 9.5 | ON CONFLICT DO UPDATE/NOTHING 语法 |
| **LateralJoinSupport** | ✅ 完整 | ≥ 9.3 | LATERAL 连接 |
| **ArraySupport** | ✅ 完整 | 所有版本 | 原生数组类型和操作 |
| **JSONSupport** | ✅ 完整 | ≥ 9.2 | JSON 类型；JSONB ≥ 9.4；JSON_TABLE ≥ 12 |
| **ExplainSupport** | ✅ 完整 | 所有版本 | EXPLAIN 和 EXPLAIN ANALYZE 支持多种格式 |
| **GraphSupport** | ❌ 无 | 不适用 | 无原生图查询 MATCH 子句 |
| **LockingSupport** | ✅ 部分 | ≥ 9.5 | FOR UPDATE；SKIP LOCKED ≥ 9.5 |
| **MergeSupport** | ✅ 完整 | ≥ 15.0 | MERGE 语句 |
| **OrderedSetAggregationSupport** | ✅ 完整 | ≥ 9.4 | 有序集聚合函数 |
| **TemporalTableSupport** | ❌ 无 | 不适用 | 无内置时态表 |
| **QualifyClauseSupport** | ❌ 无 | 不适用 | 无 QUALIFY 子句 |

### DDL 相关协议

| 协议 | 支持状态 | 版本要求 | 备注 |
|------|---------|---------|------|
| **TableSupport** | ✅ 完整 | 所有版本 | CREATE TABLE、DROP TABLE、ALTER TABLE 所有功能 |
| **ViewSupport** | ✅ 完整 | 所有版本 | CREATE VIEW、DROP VIEW 所有选项 |
| **TruncateSupport** | ✅ 完整 | 所有版本 | TRUNCATE TABLE；RESTART IDENTITY ≥ 8.4；CASCADE |
| **SchemaSupport** | ✅ 完整 | 所有版本 | CREATE SCHEMA、DROP SCHEMA 所有选项 |
| **IndexSupport** | ✅ 完整 | 所有版本 | CREATE INDEX、DROP INDEX 所有功能 |
| **SequenceSupport** | ✅ 完整 | 所有版本 | CREATE SEQUENCE、DROP SEQUENCE |

## PostgreSQL 专属协议

除了标准协议外，PostgreSQL 还提供了数据库特定的原生特性协议和扩展支持协议。

### 扩展协议

| 协议 | 扩展名 | 描述 | 文档 |
|------|--------|------|------|
| **PostgresExtensionSupport** | - | 扩展检测和管理 | [PostgreSQL 扩展](https://www.postgresql.org/docs/current/extend.html) |
| **PostgresPgvectorSupport** | pgvector | 向量相似搜索 | [pgvector GitHub](https://github.com/pgvector/pgvector) |
| **PostgresPostGISSupport** | postgis | 空间数据库功能 | [PostGIS 文档](https://postgis.net/docs/) |
| **PostgresPgTrgmSupport** | pg_trgm | 三元组相似搜索 | [pg_trgm 文档](https://www.postgresql.org/docs/current/pgtrgm.html) |
| **PostgresHstoreSupport** | hstore | 键值对存储 | [hstore 文档](https://www.postgresql.org/docs/current/hstore.html) |
| **PostgresLtreeSupport** | ltree | 层级数据的标签树 | [ltree 文档](https://www.postgresql.org/docs/current/ltree.html) |
| **PostgresIntarraySupport** | intarray | 整数数组操作符 | [intarray 文档](https://www.postgresql.org/docs/current/intarray.html) |
| **PostgresEarthdistanceSupport** | earthdistance | 大圆距离计算 | [earthdistance 文档](https://www.postgresql.org/docs/current/earthdistance.html) |
| **PostgresTablefuncSupport** | tablefunc | 交叉表和连接树 | [tablefunc 文档](https://www.postgresql.org/docs/current/tablefunc.html) |
| **PostgresPgStatStatementsSupport** | pg_stat_statements | 查询执行统计 | [pg_stat_statements 文档](https://www.postgresql.org/docs/current/pgstatstatements.html) |

### 原生特性协议

| 协议 | 描述 | 最低版本 | 文档 |
|------|------|----------|------|
| **PostgresPartitionSupport** | 高级分区特性 | PG 11+ | [分区文档](https://www.postgresql.org/docs/current/ddl-partitioning.html) |
| **PostgresIndexSupport** | 索引增强 | PG 10+ | [索引文档](https://www.postgresql.org/docs/current/indexes.html) |
| **PostgresVacuumSupport** | VACUUM 改进 | PG 13+ | [VACUUM 文档](https://www.postgresql.org/docs/current/sql-vacuum.html) |
| **PostgresQueryOptimizationSupport** | 查询优化特性 | PG 11+ | [查询文档](https://www.postgresql.org/docs/current/runtime-config-query.html) |
| **PostgresDataTypeSupport** | 数据类型增强 | PG 11+ | [数据类型文档](https://www.postgresql.org/docs/current/datatype.html) |
| **PostgresSQLSyntaxSupport** | SQL 语法增强 | PG 12+ | [SQL 语法文档](https://www.postgresql.org/docs/current/sql-syntax.html) |
| **PostgresLogicalReplicationSupport** | 逻辑复制特性 | PG 10+ | [复制文档](https://www.postgresql.org/docs/current/logical-replication.html) |
| **PostgresMaterializedViewSupport** | 物化视图 | PG 9.3+ | [物化视图文档](https://www.postgresql.org/docs/current/rules-materializedviews.html) |
| **PostgresTableSupport** | 表特定特性 | 所有版本 | [表文档](https://www.postgresql.org/docs/current/ddl.html) |

## 详细原生特性支持

### PostgresPartitionSupport

**特性来源**：原生支持（无需扩展）

PostgreSQL 分区特性超越 SQL 标准：

| 特性 | 支持 | 版本 | 描述 |
|------|------|------|------|
| `supports_hash_partitioning()` | ✅ | ≥ 11.0 | HASH 分区方法 |
| `supports_default_partition()` | ✅ | ≥ 11.0 | 用于不匹配行的 DEFAULT 分区 |
| `supports_partition_key_update()` | ✅ | ≥ 11.0 | 键更新时自动行移动 |
| `supports_concurrent_detach()` | ✅ | ≥ 14.0 | 非阻塞分区分离 |
| `supports_partition_bounds_expression()` | ✅ | ≥ 12.0 | 分区边界中的表达式 |
| `supports_partitionwise_join()` | ✅ | ≥ 11.0 | 分区级连接优化 |
| `supports_partitionwise_aggregate()` | ✅ | ≥ 11.0 | 分区级聚合优化 |

**官方文档**：https://www.postgresql.org/docs/current/ddl-partitioning.html

### PostgresIndexSupport

**特性来源**：原生支持（无需扩展）

PostgreSQL 索引特性超越标准 SQL：

| 特性 | 支持 | 版本 | 描述 |
|------|------|------|------|
| `supports_safe_hash_index()` | ✅ | ≥ 10.0 | Hash 索引有 WAL 日志（崩溃安全） |
| `supports_parallel_create_index()` | ✅ | ≥ 11.0 | 并行 B-tree 索引构建 |
| `supports_gist_include()` | ✅ | ≥ 12.0 | GiST 索引的 INCLUDE 子句 |
| `supports_reindex_concurrently()` | ✅ | ≥ 12.0 | 非阻塞索引重建 |
| `supports_btree_deduplication()` | ✅ | ≥ 13.0 | B-tree 重复压缩 |
| `supports_brin_multivalue()` | ✅ | ≥ 14.0 | BRIN 多值 min/max |
| `supports_brin_bloom()` | ✅ | ≥ 14.0 | BRIN 布隆过滤器索引 |
| `supports_spgist_include()` | ✅ | ≥ 14.0 | SP-GiST 索引的 INCLUDE 子句 |

**官方文档**：https://www.postgresql.org/docs/current/indexes.html

### PostgresVacuumSupport

**特性来源**：原生支持（无需扩展）

| 特性 | 支持 | 版本 | 描述 |
|------|------|------|------|
| `supports_parallel_vacuum()` | ✅ | ≥ 13.0 | 索引并行 VACUUM |
| `supports_index_cleanup_auto()` | ✅ | ≥ 14.0 | INDEX_CLEANUP AUTO 选项 |
| `supports_vacuum_process_toast()` | ✅ | ≥ 14.0 | PROCESS_TOAST 控制 |

**官方文档**：https://www.postgresql.org/docs/current/sql-vacuum.html

### PostgresQueryOptimizationSupport

**特性来源**：原生支持（无需扩展）

| 特性 | 支持 | 版本 | 描述 |
|------|------|------|------|
| `supports_jit()` | ✅ | ≥ 11.0 | JIT 编译（需要 LLVM） |
| `supports_incremental_sort()` | ✅ | ≥ 13.0 | 增量排序优化 |
| `supports_memoize()` | ✅ | ≥ 14.0 | Memoize 执行节点 |
| `supports_async_foreign_scan()` | ✅ | ≥ 14.0 | 异步外部表扫描 |

**官方文档**：https://www.postgresql.org/docs/current/runtime-config-query.html

### PostgresDataTypeSupport

**特性来源**：原生支持（无需扩展）

| 特性 | 支持 | 版本 | 描述 |
|------|------|------|------|
| `supports_multirange_type()` | ✅ | ≥ 14.0 | Multirange 数据类型 |
| `supports_domain_arrays()` | ✅ | ≥ 11.0 | 域类型数组 |
| `supports_composite_domains()` | ✅ | ≥ 11.0 | 复合类型域 |
| `supports_jsonb_subscript()` | ✅ | ≥ 14.0 | JSONB 下标表示法 |
| `supports_numeric_infinity()` | ✅ | ≥ 14.0 | NUMERIC 中的 Infinity 值 |
| `supports_nondeterministic_collation()` | ✅ | ≥ 12.0 | 非确定性 ICU 排序规则 |
| `supports_xid8_type()` | ✅ | ≥ 13.0 | 64 位事务 ID 类型 |

**官方文档**：https://www.postgresql.org/docs/current/datatype.html

### PostgresSQLSyntaxSupport

**特性来源**：原生支持（无需扩展）

| 特性 | 支持 | 版本 | 描述 |
|------|------|------|------|
| `supports_generated_columns()` | ✅ | ≥ 12.0 | GENERATED ALWAYS AS 列 |
| `supports_cte_search_cycle()` | ✅ | ≥ 14.0 | CTE SEARCH/CYCLE 子句 |
| `supports_fetch_with_ties()` | ✅ | ≥ 13.0 | FETCH FIRST WITH TIES |

**官方文档**：https://www.postgresql.org/docs/current/sql-syntax.html

### PostgresLogicalReplicationSupport

**特性来源**：原生支持（无需扩展）

| 特性 | 支持 | 版本 | 描述 |
|------|------|------|------|
| `supports_commit_timestamp()` | ✅ | ≥ 10.0 | 事务提交时间戳跟踪 |
| `supports_streaming_transactions()` | ✅ | ≥ 14.0 | 流式进行中事务 |
| `supports_two_phase_decoding()` | ✅ | ≥ 14.0 | 两阶段提交解码 |
| `supports_binary_replication()` | ✅ | ≥ 14.0 | 二进制传输模式 |

**官方文档**：https://www.postgresql.org/docs/current/logical-replication.html

## 详细扩展支持

### 扩展版本检测

框架支持扩展的版本感知特性检测。每个扩展都定义了其特性的最低版本要求。

#### PostgresPgvectorSupport

**扩展**：pgvector  
**仓库**：https://github.com/pgvector/pgvector

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_pgvector_type()` | 0.1.0 | vector 数据类型 |
| `supports_pgvector_similarity_search()` | 0.1.0 | <-> 距离操作符 |
| `supports_pgvector_ivfflat_index()` | 0.1.0 | IVFFlat 索引 |
| `supports_pgvector_hnsw_index()` | 0.5.0 | HNSW 索引（需要 0.5.0+） |

**安装**：`CREATE EXTENSION vector;`

#### PostgresPostGISSupport

**扩展**：PostGIS  
**网站**：https://postgis.net/

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_postgis_geometry_type()` | 2.0 | geometry 类型（平面） |
| `supports_postgis_geography_type()` | 2.0 | geography 类型（球面） |
| `supports_postgis_spatial_index()` | 2.0 | GiST 空间索引 |
| `supports_postgis_spatial_functions()` | 2.0 | ST_* 函数 |

**安装**：`CREATE EXTENSION postgis;`

#### PostgresPgTrgmSupport

**扩展**：pg_trgm

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_pg_trgm_similarity()` | 1.0 | similarity() 函数 |
| `supports_pg_trgm_index()` | 1.0 | GiST/GIN 三元组索引 |

**安装**：`CREATE EXTENSION pg_trgm;`

#### PostgresHstoreSupport

**扩展**：hstore

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_hstore_type()` | 1.0 | hstore 数据类型 |
| `supports_hstore_operators()` | 1.0 | ->、->>、@>、? 操作符 |

**安装**：`CREATE EXTENSION hstore;`

#### PostgresLtreeSupport

**扩展**：ltree

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_ltree_type()` | 1.0 | ltree 数据类型 |
| `supports_ltree_operators()` | 1.0 | <@、@>、~ 操作符 |
| `supports_ltree_index()` | 1.0 | GiST/B-tree 索引 |

**安装**：`CREATE EXTENSION ltree;`

#### PostgresIntarraySupport

**扩展**：intarray

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_intarray_operators()` | 1.0 | &&、@>、<@ 操作符 |
| `supports_intarray_functions()` | 1.0 | uniq()、sort()、idx() |
| `supports_intarray_index()` | 1.0 | GiST 索引支持 |

**安装**：`CREATE EXTENSION intarray;`

#### PostgresEarthdistanceSupport

**扩展**：earthdistance（依赖 cube）

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_earthdistance_type()` | 1.0 | earth 数据类型 |
| `supports_earthdistance_operators()` | 1.0 | <@> 距离操作符 |

**安装**：
```sql
CREATE EXTENSION cube;
CREATE EXTENSION earthdistance;
```

#### PostgresTablefuncSupport

**扩展**：tablefunc

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_tablefunc_crosstab()` | 1.0 | crosstab() 交叉表 |
| `supports_tablefunc_connectby()` | 1.0 | connectby() 树查询 |
| `supports_tablefunc_normal_rand()` | 1.0 | normal_rand() 函数 |

**安装**：`CREATE EXTENSION tablefunc;`

#### PostgresPgStatStatementsSupport

**扩展**：pg_stat_statements  
**要求**：在 postgresql.conf 中设置 `shared_preload_libraries = 'pg_stat_statements'`

| 特性 | 最低版本 | 描述 |
|------|----------|------|
| `supports_pg_stat_statements_view()` | 1.0 | 查询统计视图 |
| `supports_pg_stat_statements_reset()` | 1.0 | 重置统计函数 |

**安装**：
1. 在 postgresql.conf 中添加：`shared_preload_libraries = 'pg_stat_statements'`
2. 重启 PostgreSQL
3. 执行：`CREATE EXTENSION pg_stat_statements;`

## 扩展版本感知特性检测

### 使用 check_extension_feature()

框架提供了便捷的方法进行版本感知的特性检测：

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

# 连接并内省
backend = PostgresBackend(...)
backend.connect()
backend.introspect_and_adapt()

# 检查扩展特性（带版本感知）
if backend.dialect.check_extension_feature('vector', 'hnsw_index'):
    # HNSW 索引受支持（pgvector >= 0.5.0）
    pass

# 替代方式：使用特定支持方法
if backend.dialect.supports_pgvector_hnsw_index():
    # 结果相同，更明确
    pass
```

### 扩展检测流程

1. **连接**：建立到 PostgreSQL 的连接
2. **内省**：调用 `introspect_and_adapt()`
3. **缓存**：扩展信息缓存在 `dialect._extensions` 中
4. **检测**：使用 `check_extension_feature()` 或特定方法

**示例**：
```python
# 自动检测
backend.introspect_and_adapt()

# 检查已安装扩展
for ext_name, info in backend.dialect._extensions.items():
    if info.installed:
        print(f"{ext_name}: {info.version} (模式: {info.schema})")

# 编程检查
if backend.dialect.is_extension_installed('postgis'):
    version = backend.dialect.get_extension_version('postgis')
    print(f"PostGIS 版本: {version}")
```

## 版本兼容性矩阵

### PostgreSQL 8.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
| 8.2 | RETURNING 子句 |
| 8.3 | 窗口函数框架 |
| 8.4 | 完整窗口函数、递归 CTE、TRUNCATE RESTART IDENTITY |

### PostgreSQL 9.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
| 9.2 | JSON 数据类型 |
| 9.3 | LATERAL 连接、物化视图 |
| 9.4 | JSONB、FILTER 子句、有序集聚合 |
| 9.5 | UPSERT（ON CONFLICT）、FOR UPDATE SKIP LOCKED、ROLLUP/CUBE/GROUPING SETS |
| 9.6 | 短语全文搜索 |

### PostgreSQL 10.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
| 10 | 逻辑复制、声明式分区、标识列、SCRAM-SHA-256、Hash 索引 WAL 日志、提交时间戳跟踪 |

### PostgreSQL 11.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
| 11 | HASH/DEFAULT 分区、存储过程（OUT 参数）、JIT 编译、分区级连接/聚合、并行 CREATE INDEX、域数组、复合域 |

### PostgreSQL 12.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
| 12 | 生成列、MATERIALIZED CTE 提示、JSON_TABLE、REINDEX CONCURRENTLY、GiST INCLUDE、非确定性 ICU 排序、分区边界表达式 |

### PostgreSQL 13.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
| 13 | B-tree 去重、并行 VACUUM、增量排序、FETCH WITH TIES、xid8 类型 |

### PostgreSQL 14.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
| 14 | Multirange 类型、JSONB 下标、CTE SEARCH/CYCLE、BRIN 多值/布隆、并发 DETACH PARTITION、流式事务、Memoize、SP-GiST INCLUDE、Numeric Infinity |

### PostgreSQL 15.x 系列

| 版本 | 新增关键特性 |
|------|-------------|
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

# 检查特定特性
assert dialect.supports_truncate_restart_identity()  # True（≥ 8.4）
assert dialect.supports_materialized_cte()  # True（≥ 12）
assert dialect.supports_merge_statement()  # False（需要 ≥ 15）
```

### 版本特定特性检测

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

# 旧版本 PostgreSQL
old_dialect = PostgresDialect(version=(8, 3, 0))
assert not old_dialect.supports_truncate_restart_identity()  # False（需要 ≥ 8.4）

# 新版本 PostgreSQL
new_dialect = PostgresDialect(version=(15, 0, 0))
assert new_dialect.supports_merge_statement()  # True（≥ 15）
assert new_dialect.supports_truncate_restart_identity()  # True（≥ 8.4）
```

## 另见

- [PostgreSQL 官方文档](https://www.postgresql.org/docs/current/)
- [PostgreSQL 特性矩阵](https://www.postgresql.org/about/featurematrix/)
- [rhosocial.activerecord 后端开发指南](../../../python-activerecord/docs/backend_development.md)
