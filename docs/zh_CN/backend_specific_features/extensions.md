# PostgreSQL 扩展

## 概述

PostgreSQL 支持通过扩展来增强其功能。后端提供自动检测和集成许多流行扩展的功能。

## 支持的扩展

| 扩展 | 分类 | 说明 | 最低版本 |
|------|------|------|---------|
| PostGIS | 空间 | 空间数据库功能 | 2.0 |
| pgvector | AI/ML | 向量相似度搜索 | 0.5.0 |
| pg_trgm | 文本 | 三元组相似度搜索 | 1.0 |
| hstore | 键值 | 键值对存储 | 1.0 |
| uuid-ossp | UUID | UUID 生成函数 | 1.0 |
| pgcrypto | 加密 | 加密函数 | 1.0 |
| ltree | 层次 | 标签树层次数据 | 1.0 |
| intarray | 数组 | 整数数组操作 | 1.0 |
| citext | 文本 | 大小写不敏感文本 | 1.0 |
| btree_gin | 索引 | B-tree GIN 索引支持 | 1.0 |
| btree_gist | 索引 | B-tree GiST 索引支持 | 1.0 |
| pg_partman | 分区 | 分区管理 | 4.0 |
| pg_cron | 调度 | 任务调度 | 1.0 |
| pg_stat_statements | 监控 | 查询统计 | 1.0 |
| pgaudit | 审计 | 审计日志 | 1.0 |
| pglogical | 复制 | 逻辑复制 | 2.0 |
| pg_surgery | 修复 | 表修复操作 | 1.0 |
| pg_walinspect | 监控 | WAL 检查 | 1.0 |
| tablefunc | 函数 | 交叉表查询 | 1.0 |
| cube | 几何 | 多维立方体 | 1.0 |
| earthdistance | 几何 | 地球距离计算 | 1.0 |
| fuzzystrmatch | 文本 | 模糊字符串匹配 | 1.0 |
| orafce | 兼容性 | Oracle 兼容函数 | 3.0 |
| hypopg | 规划 | 假设索引 | 1.0 |
| bloom | 索引 | 布隆过滤器索引 | 1.0 |
| address_standardizer | 地址 | 地址解析 | 1.0 |
| postgis_raster | 空间 | 栅格数据支持 | 2.0 |
| pgrouting | 路由 | 地理空间路由 | 2.0 |

## 扩展检测

后端在连接时自动检测已安装的扩展：

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(...)
backend.connect()

# 扩展会自动检测
# 检查特定扩展是否已安装
if backend.dialect.is_extension_installed('postgis'):
    print("PostGIS 可用")

# 获取扩展版本
version = backend.dialect.get_extension_version('pgvector')
print(f"pgvector 版本: {version}")

# 列出所有已安装扩展
extensions = backend.dialect.detect_extensions()
for name, info in extensions.items():
    print(f"{name}: {info.version}")
```

## 扩展详情

### PostGIS（空间）

PostGIS 提供完整的空间数据库功能：

```python
# 需要 PostGIS 扩展
# 安装: CREATE EXTENSION postgis;

from rhosocial.activerecord.backend.impl.postgres.protocols.extensions.postgis import PostgresPostGISSupport

# 检查功能
if backend.dialect.supports_postgis_geometry_type():
    print("支持几何类型")

if backend.dialect.supports_postgis_spatial_index():
    print("支持空间索引")
```

**功能特性：**
- 几何和地理数据类型
- 空间索引（GiST）
- 空间分析函数
- 坐标系转换

### pgvector（AI/ML）

pgvector 提供向量相似度搜索，适用于 AI 应用：

```python
# 需要 pgvector 扩展
# 安装: CREATE EXTENSION vector;

from rhosocial.activerecord.backend.impl.postgres.protocols.extensions.pgvector import PostgresPgvectorSupport

# 检查功能
if backend.dialect.supports_pgvector():
    print("支持 pgvector")
```

**功能特性：**
- 向量数据类型
- L2 距离、内积、余弦距离
- IVFFlat 和 HNSW 索引
- 近似最近邻搜索

### pg_trgm（文本搜索）

pg_trgm 提供三元组相似度搜索：

```python
# 需要 pg_trgm 扩展
# 安装: CREATE EXTENSION pg_trgm;

# 检查功能
if backend.dialect.supports_pg_trgm():
    print("支持 pg_trgm")
```

**功能特性：**
- 三元组相似度匹配
- 模糊文本搜索
- GIN/GiST 索引支持

### hstore（键值）

hstore 提供键值对存储：

```python
# 需要 hstore 扩展
# 安装: CREATE EXTENSION hstore;

# 检查功能
if backend.dialect.supports_hstore():
    print("支持 hstore")
```

**功能特性：**
- 键值对存储
- 包含操作符
- 数组转换

### ltree（层次）

ltree 提供标签树层次数据：

```python
# 需要 ltree 扩展
# 安装: CREATE EXTENSION ltree;

# 检查功能
if backend.dialect.supports_ltree():
    print("支持 ltree")
```

**功能特性：**
- 层次标签路径
- 树遍历操作符
- LCA（最近公共祖先）查询

## 扩展管理

### DDL 表达式

后端提供用于管理扩展的 DDL 表达式：

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.extension import (
    PostgresCreateExtensionExpression,
    PostgresDropExtensionExpression,
)

# 创建扩展
create_expr = PostgresCreateExtensionExpression(
    dialect,
    extension_name="postgis",
    if_not_exists=True,
)

# 删除扩展
drop_expr = PostgresDropExtensionExpression(
    dialect,
    extension_name="postgis",
    if_exists=True,
)
```

### CLI 命令

可通过 CLI 管理扩展：

```bash
# 列出已安装扩展
rhosocial-activerecord-postgres introspect extensions \
    --host localhost --database mydb

# 检查特定扩展
rhosocial-activerecord-postgres introspect extensions \
    --host localhost --database mydb | grep postgis
```

## 版本要求

| 扩展 | PostgreSQL 版本 | 说明 |
|------|----------------|------|
| PostGIS | 9.2+ | 推荐 3.0+ |
| pgvector | 12+ | 推荐 0.5.0+ |
| pg_trgm | 8.3+ | |
| hstore | 8.3+ | |
| ltree | 8.3+ | |
| uuid-ossp | 8.3+ | |
| pgcrypto | 8.3+ | |
| pg_partman | 9.4+ | 推荐 4.0+ |
| pg_cron | 9.5+ | |
| pg_stat_statements | 8.4+ | |
| pgaudit | 9.5+ | |
| pglogical | 9.4+ | |

## 另请参阅

- [PostgreSQL 字段类型](./field_types.md) — ARRAY、JSONB、UUID 类型
- [高级索引](./indexing.md) — GIN、GiST、BRIN 索引
- [PostgreSQL 方言](./dialect.md) — PostgreSQL 特有 SQL 语法

💡 *AI 提示：* "如何在 Python 中使用 PostGIS 进行空间查询？"
