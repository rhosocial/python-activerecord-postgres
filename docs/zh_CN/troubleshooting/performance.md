# 性能问题

## 概述

本节介绍 PostgreSQL 性能问题及优化方法。

## 慢查询分析

### 启用慢查询日志

```sql
-- 查看当前配置
SHOW log_min_duration_statement;

-- 启用超过 1000ms 的查询日志
ALTER SYSTEM SET log_min_duration_statement = 1000;
SELECT pg_reload_conf();
```

### 使用 EXPLAIN ANALYZE

```python
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend, PostgreSQLConnectionConfig

backend = PostgreSQLBackend(
    connection_config=PostgreSQLConnectionConfig(
        host='localhost',
        database='myapp',
        username='user',
        password='password',
    )
)
backend.connect()

with backend.get_connection().cursor() as cursor:
    cursor.execute("EXPLAIN ANALYZE SELECT * FROM users WHERE name = '张三'")
    for row in cursor:
        print(row)

backend.disconnect()
```

## 常见性能问题

### 1. 缺少索引

```sql
-- 检查索引是否存在
SELECT indexname FROM pg_indexes WHERE tablename = 'users';

-- 添加索引
CREATE INDEX idx_users_name ON users(name);

-- 添加部分索引（PostgreSQL 特有）
CREATE INDEX idx_users_active ON users(name) WHERE active = true;
```

### 2. SELECT *

```python
# 避免 SELECT *，只查询需要的列
users = User.query().select(User.c.id, User.c.name).all()
```

### 3. N+1 查询问题

```python
# 使用预加载避免 N+1
users = User.query().with_('posts').all()
```

### 4. VACUUM 和 ANALYZE

```sql
-- 运行 VACUUM 回收空间
VACUUM users;

-- 运行 ANALYZE 更新统计信息
ANALYZE users;

-- 组合使用
VACUUM ANALYZE users;
```

## 连接池

对于高并发应用，使用连接池：

```python
config = PostgreSQLConnectionConfig(
    host='localhost',
    database='myapp',
    username='user',
    password='password',
    min_connections=5,
    max_connections=20,
)
```

或使用 PgBouncer 进行外部连接池：

```bash
# 安装 PgBouncer
sudo apt install pgbouncer

# 配置 pgbouncer.ini
[databases]
myapp = host=localhost port=5432 dbname=myapp

[pgbouncer]
pool_mode = transaction
max_client_conn = 100
default_pool_size = 20
```

💡 *AI 提示词：* "如何优化 PostgreSQL 查询性能？"
