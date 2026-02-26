# 事务支持

PostgreSQL 提供强大的事务支持，包括保存点、隔离级别和可延迟约束等高级功能。

## 主题

- **[事务隔离级别](./isolation_level.md)**: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- **[Savepoint 支持](./savepoint.md)**: 嵌套事务
- **[DEFERRABLE 模式](./deferrable.md)**: 延迟约束检查
- **[死锁处理](./deadlock.md)**: 处理并发冲突

## 快速开始

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

# 基础事务
with tm:
    user1 = User(name="张三")
    user1.save()
    user2 = User(name="李四")
    user2.save()
# 上下文退出时自动提交
```

💡 *AI 提示词：* "不同事务隔离级别之间的权衡是什么？"
