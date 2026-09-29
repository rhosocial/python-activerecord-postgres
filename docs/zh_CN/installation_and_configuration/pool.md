# 连接管理

## 随用随连模式

PostgreSQL 后端默认使用"随用随连"模式：

```python
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend

backend = PostgresBackend(connection_config=config)
# 此时连接尚未建立

user = User(name="John")  # 在此处建立连接
user.save()
```

## 手动连接管理

### 同步

```python
backend = PostgresBackend(connection_config=config)

# 显式连接
backend.connect()

try:
    # 数据库操作
    users = User.query().all()
finally:
    # 显式断开
    backend.disconnect()
```

### 异步

```python
backend = AsyncPostgresBackend(connection_config=config)

# 显式异步连接
await backend.connect()

try:
    users = await User.query().all()
finally:
    await backend.disconnect()
```

## 连接池（可选）

对于高吞吐量应用，可以使用外部连接池：

```bash
pip install rhosocial-activerecord-postgres[pooling]
```

```python
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend

config = PostgresConnectionConfig(
    # ... 基础配置 ...
    options={
        "pool_min_size": 1,
        "pool_max_size": 10,
        "pool_timeout": 30.0
    }
)
```

💡 *AI 提示词：* "什么时候应该使用连接池，什么时候应该使用随用随连？"
