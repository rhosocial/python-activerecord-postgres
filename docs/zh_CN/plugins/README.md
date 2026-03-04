# 插件支持

PostgreSQL 支持通过扩展机制安装额外的功能模块。本节介绍如何检测和使用这些扩展。

## 概述

PostgreSQL 扩展系统允许安装额外的功能模块，如：
- **PostGIS**: 空间数据库功能
- **pgvector**: 向量相似性搜索（AI 应用）
- **pg_trgm**: 三元组相似性搜索
- **hstore**: 键值对存储

## 内省与适配

后端通过 `introspect_and_adapt()` 方法自动检测服务器能力和已安装扩展。

### 基本用法

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(
    host='localhost',
    port=5432,
    database='mydb',
    username='user',
    password='password'
)
backend.connect()

# 执行内省与适配
backend.introspect_and_adapt()
```

### 检测时机

`introspect_and_adapt()` 在以下情况执行：
- 首次调用时
- 连接重连且服务器版本变化时

### 检测内容

1. **服务器版本**: 确定版本相关的功能支持
2. **已安装扩展**: 查询 `pg_extension` 系统表
3. **运行时适配**: 根据检测结果调整功能支持

## 检测扩展

### 检查扩展是否已安装

```python
# 检查扩展是否已安装
if backend.dialect.is_extension_installed('postgis'):
    print("PostGIS 已安装")

# 获取扩展详细信息
ext_info = backend.dialect.get_extension_info('postgis')
if ext_info:
    print(f"版本: {ext_info.version}")
    print(f"所在模式: {ext_info.schema}")
```

### 获取扩展版本

```python
version = backend.dialect.get_extension_version('postgis')
if version:
    print(f"PostGIS 版本: {version}")
```

## 版本支持

| PostgreSQL 版本 | 插件支持状态 |
|----------------|-------------|
| 14.x ~ 18.x | ✅ 完全支持 |
| 9.6 ~ 13.x | ⚠️ 尽力而为 |
| 9.5 及以下 | ❌ 未测试 |

**说明**: 插件功能主要针对 PostgreSQL 14+ 进行开发和测试。更低版本可能工作，但不保证所有功能可用。

## 已知扩展

Dialect 维护以下已知扩展的定义：

| 扩展名 | 最低版本 | 类别 | 描述 |
|--------|----------|------|------|
| postgis | 2.0 | spatial | PostGIS 空间数据库扩展 |
| vector | 0.1 | vector | pgvector 向量相似性搜索 |
| pg_trgm | 1.0 | text | 三元组相似性搜索 |
| hstore | 1.0 | data | 键值对存储 |
| uuid-ossp | 1.0 | utility | UUID 生成函数 |
| pgcrypto | 1.0 | security | 加密函数 |

## 安装扩展

使用扩展前需要先在数据库中安装：

```sql
-- 安装 PostGIS
CREATE EXTENSION postgis;

-- 安装 pgvector
CREATE EXTENSION vector;

-- 安装 hstore
CREATE EXTENSION hstore;
```

安装后需要重新执行内省：

```python
backend.execute("CREATE EXTENSION vector")
backend.introspect_and_adapt()
```

## 相关主题

- **[PostGIS](./postgis.md)**: 空间数据库功能
- **[pgvector](./pgvector.md)**: 向量相似性搜索
- **[pg_trgm](./pg_trgm.md)**: 三元组相似性
- **[hstore](./hstore.md)**: 键值存储

💡 *AI 提示词：* "PostgreSQL 的扩展机制相比 MySQL 的插件系统有什么优势？"
