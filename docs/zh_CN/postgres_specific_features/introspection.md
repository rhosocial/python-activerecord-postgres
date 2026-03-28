# 数据库内省

PostgreSQL 后端提供完整的数据库内省功能，使用 `pg_catalog` 系统表查询数据库结构元数据。

## 概述

PostgreSQL 内省系统位于 `backend.introspector` 属性中，提供：

- **数据库信息**：名称、版本、编码、数据库大小
- **表信息**：表列表、表详情（包括物化视图、分区表、外部表）
- **列信息**：列名、数据类型、是否可空、默认值
- **索引信息**：索引名、列、唯一性、索引类型（BTREE、GIN、GiST 等）
- **外键信息**：引用表、列映射、更新/删除行为
- **视图信息**：视图定义 SQL
- **触发器信息**：触发事件、执行时机

## 基本用法

### 访问内省器

```python
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend

backend = PostgreSQLBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
backend.connect()

# 通过 introspector 属性访问
introspector = backend.introspector
```

### 获取数据库信息

```python
# 获取数据库基本信息
db_info = backend.introspector.get_database_info()
print(f"数据库名称: {db_info.name}")
print(f"版本: {db_info.version}")
print(f"编码: {db_info.encoding}")
print(f"数据库大小: {db_info.size_bytes} bytes")
```

### 列出表

```python
# 列出所有用户表
tables = backend.introspector.list_tables()
for table in tables:
    print(f"表: {table.name}, 类型: {table.table_type.value}")
    if table.comment:
        print(f"  注释: {table.comment}")

# 包含系统表
all_tables = backend.introspector.list_tables(include_system=True)

# 过滤特定类型
base_tables = backend.introspector.list_tables(table_type="BASE TABLE")
views = backend.introspector.list_tables(table_type="VIEW")
materialized_views = backend.introspector.list_tables(table_type="MATERIALIZED VIEW")

# 检查表是否存在
if backend.introspector.table_exists("users"):
    print("users 表存在")

# 获取表详情
table_info = backend.introspector.get_table_info("users")
if table_info:
    print(f"表名: {table_info.name}")
    print(f"估计行数: {table_info.row_count}")
    print(f"表大小: {table_info.size_bytes} bytes")
```

### 查询列信息

```python
# 列出表的所有列
columns = backend.introspector.list_columns("users")
for col in columns:
    nullable = "NOT NULL" if col.nullable.value == "NOT_NULL" else "NULLABLE"
    pk = " [PK]" if col.is_primary_key else ""
    print(f"{col.name}: {col.data_type} {nullable}{pk}")
    if col.comment:
        print(f"  注释: {col.comment}")

# 获取主键信息
pk = backend.introspector.get_primary_key("users")
if pk:
    print(f"主键: {[c.name for c in pk.columns]}")

# 获取单列信息
col_info = backend.introspector.get_column_info("users", "email")
```

### 查询索引

```python
# 列出表的所有索引
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    idx_type = idx.index_type.value if idx.index_type else "BTREE"
    unique = "UNIQUE " if idx.is_unique else ""
    primary = "PRIMARY " if idx.is_primary else ""
    print(f"{primary}{unique}索引: {idx.name} ({idx_type})")
    for col in idx.columns:
        desc = "DESC" if col.is_descending else "ASC"
        nulls = ""
        if col.is_nulls_first is not None:
            nulls = " NULLS FIRST" if col.is_nulls_first else " NULLS LAST"
        print(f"  - {col.name} ({desc}{nulls})")
```

### 查询外键

```python
# 列出表的外键
foreign_keys = backend.introspector.list_foreign_keys("posts")
for fk in foreign_keys:
    print(f"外键: {fk.name}")
    print(f"  列: {fk.columns} -> {fk.referenced_table}.{fk.referenced_columns}")
    print(f"  ON DELETE: {fk.on_delete.value}")
    print(f"  ON UPDATE: {fk.on_update.value}")
```

### 查询视图

```python
# 列出所有视图
views = backend.introspector.list_views()
for view in views:
    print(f"视图: {view.name}")

# 获取视图详情
view_info = backend.introspector.get_view_info("user_posts_summary")
if view_info:
    print(f"定义: {view_info.definition}")
    print(f"可更新: {view_info.is_updatable}")
```

### 查询触发器

```python
# 列出所有触发器
triggers = backend.introspector.list_triggers()
for trigger in triggers:
    print(f"触发器: {trigger.name} on {trigger.table_name}")
    print(f"  事件: {trigger.event.value}")
    print(f"  时机: {trigger.timing.value}")

# 列出特定表的触发器
table_triggers = backend.introspector.list_triggers("users")
```

## PostgreSQL 特定行为

### 表类型

PostgreSQL 支持多种表类型：

| 类型 | 说明 |
|------|------|
| `BASE TABLE` | 普通表 |
| `VIEW` | 视图 |
| `MATERIALIZED VIEW` | 物化视图 |
| `FOREIGN TABLE` | 外部表 |
| `PARTITIONED TABLE` | 分区表 |

```python
tables = backend.introspector.list_tables()
for table in tables:
    if table.table_type.value == "MATERIALIZED VIEW":
        print(f"物化视图: {table.name}")
    elif table.table_type.value == "PARTITIONED TABLE":
        print(f"分区表: {table.name}")
```

### Schema 支持

PostgreSQL 使用 schema 组织数据库对象：

```python
# 默认 schema 为 'public'
tables = backend.introspector.list_tables()  # 查询 public schema

# 指定其他 schema
tables = backend.introspector.list_tables(schema="analytics")

# 获取表详情时指定 schema
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### 索引类型

PostgreSQL 支持多种高级索引类型：

| 索引类型 | 说明 |
|---------|------|
| `BTREE` | 默认索引类型，适合等值和范围查询 |
| `HASH` | 仅等值查询 |
| `GIN` | 全文搜索、JSONB、数组 |
| `GIST` | 地理空间、全文搜索 |
| `SPGIST` | 空间分区 GiST |
| `BRIN` | 大表块范围索引 |

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("articles")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        print(f"GIN 索引: {idx.name} (可能用于 JSONB 或全文搜索)")
    elif idx.index_type == IndexType.GIST:
        print(f"GiST 索引: {idx.name} (可能用于地理数据)")
```

### 触发器事件

PostgreSQL 触发器支持多种事件：

| 事件 | 说明 |
|------|------|
| `INSERT` | 插入操作 |
| `UPDATE` | 更新操作 |
| `DELETE` | 删除操作 |
| `TRUNCATE` | 清空表 |

```python
triggers = backend.introspector.list_triggers("users")
for trigger in triggers:
    print(f"触发器 {trigger.name}:")
    print(f"  事件: {trigger.event.value}")
    print(f"  时机: {trigger.timing.value}")  # BEFORE, AFTER, INSTEAD OF
```

### NULLS 排序

PostgreSQL 索引支持 NULLS FIRST/LAST：

```python
indexes = backend.introspector.list_indexes("users")
for idx in indexes:
    for col in idx.columns:
        if col.is_nulls_first is not None:
            nulls_order = "NULLS FIRST" if col.is_nulls_first else "NULLS LAST"
            print(f"{idx.name}.{col.name}: {nulls_order}")
```

## 异步 API

异步后端提供相同的内省方法，方法名与同步版本相同：

```python
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgreSQLBackend

backend = AsyncPostgreSQLBackend(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password"
)
await backend.connect()

# 异步内省方法（方法名与同步版本相同）
db_info = await backend.introspector.get_database_info()
tables = await backend.introspector.list_tables()
columns = await backend.introspector.list_columns("users")
indexes = await backend.introspector.list_indexes("users")
```

## 缓存管理

内省结果默认会被缓存以提高性能：

```python
# 清除所有内省缓存
backend.introspector.clear_cache()

# 使特定作用域的缓存失效
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

# 使所有表相关缓存失效
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE)

# 使特定表的缓存失效
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users"
)

# 使特定 schema 的缓存失效
backend.introspector.invalidate_cache(
    scope=IntrospectionScope.TABLE,
    name="users",
    table_name="users"  # 用于限定 schema
)
```

## 版本兼容性

不同 PostgreSQL 版本的内省行为差异：

| 功能 | PostgreSQL 12+ | PostgreSQL 11 及以下 |
|------|----------------|---------------------|
| 物化视图信息 | 完整支持 | 基础支持 |
| 分区表信息 | 完整支持 | 有限支持 |
| 生成列 | 支持 | 不支持 |
| 存储过程触发器 | 支持 | 不支持 |

## 最佳实践

### 1. 使用缓存

内省操作涉及复杂的 `pg_catalog` 查询，建议利用缓存：

```python
# 首次查询会缓存结果
tables = backend.introspector.list_tables()

# 后续查询直接从缓存返回
tables_again = backend.introspector.list_tables()

# 只有在表结构变更后才需要清除缓存
backend.introspector.invalidate_cache(scope=IntrospectionScope.TABLE, name="users")
```

### 2. 指定 Schema

在多 schema 环境中，明确指定 schema：

```python
# 明确指定 schema 避免歧义
table_info = backend.introspector.get_table_info("users", schema="auth")
```

### 3. 利用索引类型信息

根据索引类型优化查询：

```python
from rhosocial.activerecord.backend.introspection.types import IndexType

indexes = backend.introspector.list_indexes("documents")
for idx in indexes:
    if idx.index_type == IndexType.GIN:
        # GIN 索引适合 JSONB 或全文搜索查询
        print(f"GIN 索引可用: {idx.name}")
```

### 4. 异步环境中的并发查询

```python
async def get_schema_info(backend):
    # 并发获取多个表的列信息
    import asyncio
    tables = await backend.introspector.list_tables()
    tasks = [
        backend.introspector.list_columns(table.name)
        for table in tables
    ]
    all_columns = await asyncio.gather(*tasks)
    return dict(zip([t.name for t in tables], all_columns))
```

## API 参考

### 核心方法

| 方法 | 说明 | 参数 |
|------|------|------|
| `get_database_info()` | 获取数据库信息 | 无 |
| `list_tables()` | 列出表 | `include_system`, `table_type`, `schema` |
| `get_table_info(name)` | 获取表详情 | `name`, `schema` |
| `table_exists(name)` | 检查表存在 | `name`, `schema` |
| `list_columns(table_name)` | 列出列 | `table_name`, `schema` |
| `get_column_info(table_name, column_name)` | 获取列详情 | `table_name`, `column_name`, `schema` |
| `get_primary_key(table_name)` | 获取主键 | `table_name`, `schema` |
| `list_indexes(table_name)` | 列出索引 | `table_name`, `schema` |
| `get_index_info(table_name, index_name)` | 获取索引详情 | `table_name`, `index_name`, `schema` |
| `index_exists(table_name, index_name)` | 检查索引存在 | `table_name`, `index_name`, `schema` |
| `list_foreign_keys(table_name)` | 列出外键 | `table_name`, `schema` |
| `list_views()` | 列出视图 | `schema` |
| `get_view_info(name)` | 获取视图详情 | `name`, `schema` |
| `view_exists(name)` | 检查视图存在 | `name`, `schema` |
| `list_triggers(table_name)` | 列出触发器 | `table_name`, `schema` |
| `get_trigger_info(table_name, trigger_name)` | 获取触发器详情 | `table_name`, `trigger_name`, `schema` |
| `clear_cache()` | 清除缓存 | 无 |
| `invalidate_cache(scope, ...)` | 使缓存失效 | `scope`, `name`, `table_name` |

## 命令行内省命令

PostgreSQL 后端提供命令行内省命令，无需编写代码即可查询数据库元数据。

### 基本用法

```bash
# 列出所有表
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --host localhost --port 5432 --database mydb --user postgres --password

# 列出所有视图
python -m rhosocial.activerecord.backend.impl.postgres introspect views \
  --database mydb --user postgres

# 获取数据库信息
python -m rhosocial.activerecord.backend.impl.postgres introspect database \
  --database mydb --user postgres

# 包含系统表
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --include-system
```

### 连接参数

支持通过命令行参数或环境变量配置连接：

| 参数 | 环境变量 | 说明 |
|------|----------|------|
| `--host` | `POSTGRES_HOST` | 数据库主机（默认 localhost） |
| `--port` | `POSTGRES_PORT` | 数据库端口（默认 5432） |
| `--database` | `POSTGRES_DATABASE` | 数据库名称（必需） |
| `--user` | `POSTGRES_USER` | 用户名（默认 postgres） |
| `--password` | `POSTGRES_PASSWORD` | 密码 |

### 查询表详情

```bash
# 获取表的完整信息（列、索引、外键）
python -m rhosocial.activerecord.backend.impl.postgres introspect table users \
  --database mydb --user postgres

# 仅查询列信息
python -m rhosocial.activerecord.backend.impl.postgres introspect columns users \
  --database mydb --user postgres

# 仅查询索引信息
python -m rhosocial.activerecord.backend.impl.postgres introspect indexes users \
  --database mydb --user postgres

# 仅查询外键信息
python -m rhosocial.activerecord.backend.impl.postgres introspect foreign-keys posts \
  --database mydb --user postgres

# 查询触发器
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers \
  --database mydb --user postgres

# 查询特定表的触发器
python -m rhosocial.activerecord.backend.impl.postgres introspect triggers users \
  --database mydb --user postgres
```

### Schema 支持

PostgreSQL 支持 schema 参数：

```bash
# 指定 schema 查询表
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --schema auth

# 查询特定 schema 中的表详情
python -m rhosocial.activerecord.backend.impl.postgres introspect table users \
  --database mydb --user postgres --schema analytics
```

### 内省类型

| 类型 | 说明 | 是否需要表名 |
|------|------|-------------|
| `tables` | 列出所有表 | 否 |
| `views` | 列出所有视图 | 否 |
| `database` | 数据库信息 | 否 |
| `table` | 表完整详情（列、索引、外键） | 是 |
| `columns` | 列信息 | 是 |
| `indexes` | 索引信息 | 是 |
| `foreign-keys` | 外键信息 | 是 |
| `triggers` | 触发器信息 | 可选 |

### 输出格式

```bash
# 表格格式（默认，需要 rich 库）
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres

# JSON 格式
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output json

# CSV 格式
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output csv

# TSV 格式
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --output tsv
```

### 使用异步后端

```bash
# 使用 --use-async 参数启用异步模式
python -m rhosocial.activerecord.backend.impl.postgres introspect tables \
  --database mydb --user postgres --use-async
```

### 环境变量配置

可以在环境中设置连接参数，简化命令行调用：

```bash
# 设置环境变量
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=mydb
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret

# 直接使用命令
python -m rhosocial.activerecord.backend.impl.postgres introspect tables
```
