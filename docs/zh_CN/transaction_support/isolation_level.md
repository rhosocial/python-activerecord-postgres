# 事务隔离级别

## 可用级别

| 级别 | 描述 | 防止的现象 |
|-----|------|----------|
| READ COMMITTED | 默认级别 | 脏读 |
| REPEATABLE READ | 事务内一致读取 | 脏读、不可重复读 |
| SERIALIZABLE | 最严格的隔离 | 所有现象 |

## 设置隔离级别

```python
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.transaction import IsolationLevel

backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

# 设置隔离级别
tm.set_isolation_level(IsolationLevel.SERIALIZABLE)

with tm:
    # 具有 SERIALIZABLE 隔离级别的事务
    pass
```

## READ COMMITTED（默认）

- 每个查询只看到已提交的数据
- 后续查询可能看到不同的数据
- 适用于大多数应用

## REPEATABLE READ

- 事务中的所有查询看到一致的快照
- 防止不可重复读
- 可能因序列化错误而失败

## SERIALIZABLE

- 最严格的隔离
- 防止幻读
- 可能有更多序列化失败
- 需要配合重试逻辑

```python
from rhosocial.activerecord.backend.errors import OperationalError

max_retries = 3
for attempt in range(max_retries):
    try:
        with tm:
            tm.set_isolation_level(IsolationLevel.SERIALIZABLE)
            # 关键操作
            break
    except OperationalError as e:
        if "serialization" in str(e).lower():
            continue
        raise
```

💡 *AI 提示词：* "什么时候应该使用 SERIALIZABLE 隔离级别？"
