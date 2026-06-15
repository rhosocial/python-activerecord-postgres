# PostgreSQL DDL 操作

PostgreSQL 后端支持与核心库相同的类型安全 DDL 表达式，并具有 PostgreSQL 特定扩展。

## 支持的操作

| 操作 | PostgreSQL 支持 | 备注 |
|----------|-------------------|-------|
| `CreateTableExpression` | ✅ 完整 | PRIMARY KEY, NOT NULL, UNIQUE 等 |
| `DropTableExpression` | ✅ 完整 | IF EXISTS, CASCADE, RESTRICT |
| `AlterTableExpression` | ✅ 完整 | ADD/DROP COLUMN, ALTER COLUMN |
| `CreateIndexExpression` | ✅ 完整 | 索引类型 (BTREE, HASH, GIN, GiST, BRIN) |
| `DropIndexExpression` | ✅ 完整 | |
| `CreateViewExpression` | ✅ 完整 | 物化视图支持 |
| `DropViewExpression` | ✅ 完整 | |
| `CreatePartitionExpression` | ✅ 完整 | RANGE, LIST, HASH 分区（PG 10+） |
| `AttachPartitionExpression` | ✅ 完整 | 含 CONCURRENTLY（PG 14+） |
| `DetachPartitionExpression` | ✅ 完整 | 含 CONCURRENTLY（PG 14+） |
| `AlterIndexExpression` | ✅ 完整 | RENAME TO, SET TABLESPACE, SET/RESET 存储参数 |
| `ReindexExpression` | ✅ 完整 | 索引/表/模式/数据库级重建，含 CONCURRENTLY |

## PostgreSQL 特性

### 分区支持

PostgreSQL 10+ 支持声明式分区，详见 [分区文档](../postgres_specific_features/partition.md)。

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionExpression, PostgresAttachPartitionExpression,
)

# 创建子分区
partition = PostgresCreatePartitionExpression(
    dialect, parent_table="orders", partition_name="orders_2024_q1",
    partition_type="RANGE", bounds="FROM ('2024-01-01') TO ('2024-04-01')",
)
```

### pg_partman 扩展

pg_partman 扩展支持自动分区管理，详见 [分区文档](../postgres_specific_features/partition.md)。

### 索引操作增强

PG 后端支持高级索引操作：

```python
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.index import (
    PostgresAlterIndexExpression, PostgresAlterIndexActionType,
    PostgresReindexExpression,
)

# ALTER INDEX ... RENAME TO
alter = PostgresAlterIndexExpression(
    dialect, index_name="idx_old",
    action=PostgresAlterIndexActionType.RENAME_TO("idx_new"),
)
# sql: 'ALTER INDEX "idx_old" RENAME TO "idx_new"'

# REINDEX
reindex = PostgresReindexExpression(
    dialect, target_type="INDEX", target_name="idx_corrupted",
    concurrently=True,
)
# sql: 'REINDEX INDEX CONCURRENTLY "idx_corrupted"'
```

### 索引类型

PostgreSQL 支持多种索引类型：

```python
create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_users_name",
    table_name="users",
    columns=["name"],
    index_type="GIN"  # GIN, GiST, BRIN, BTREE, HASH
)
```

### 局部索引

PostgreSQL 支持带 WHERE 子句的局部索引：

```python
from rhosocial.activerecord.backend.expression import Column, Literal

create_idx = CreateIndexExpression(
    dialect,
    index_name="idx_active_users",
    table_name="users",
    columns=["email"],
    where=Column(dialect, "status") == Literal(dialect, "active")
)
```

### Schema 支持

PostgreSQL 支持 schema：

```python
create_table = CreateTableExpression(
    dialect,
    table_name="schema_name.users",
    columns=columns
)
```

## 运行示例

```bash
cd python-activerecord-postgres
source .venv3.8/bin/activate
PYTHONPATH=src python docs/examples/chapter_04_ddl/ddl.py
```

示例测试：
1. 创建带约束的表
2. 使用 IF NOT EXISTS 创建表
3. ALTER TABLE - 添加列
4. ALTER TABLE - 删除列
5. 使用 IF EXISTS 删除表
6. 内省验证架构变化

> **注意**：PostgreSQL 具有比 SQLite 更强大的 DDL 支持。完整的 PostgreSQL DDL 功能请参考 [PostgreSQL 16 文档](https://www.postgresql.org/docs/16/sql-createtable.html)。