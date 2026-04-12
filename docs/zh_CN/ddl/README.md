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

## PostgreSQL 特性

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