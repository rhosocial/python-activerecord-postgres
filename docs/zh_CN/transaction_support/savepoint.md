# Savepoint 支持

Savepoint 允许在事务内部分回滚。

## 创建 Savepoint

```python
backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

tm.begin()

# 第一个操作
user1 = User(name="张三")
user1.save()

# 创建 savepoint
sp = tm.savepoint()

try:
    # 有风险的操作
    user2 = User(name="李四")
    user2.save()
    # 如果失败...
except Exception:
    # 回滚到 savepoint
    tm.rollback_savepoint(sp)

# 继续事务
user3 = User(name="王五")
user3.save()

tm.commit()
```

## 异步 Savepoint

```python
tm = backend.transaction_manager

await tm.begin()

user1 = User(name="张三")
await user1.save()

sp = await tm.savepoint()

try:
    user2 = User(name="李四")
    await user2.save()
except Exception:
    await tm.rollback_savepoint(sp)

await tm.commit()
```

## 使用场景

1. **条件操作**：尝试操作，如果条件不满足则回滚
2. **错误恢复**：部分回滚而不丢失所有工作
3. **嵌套操作**：处理子事务

💡 *AI 提示词：* "Savepoint 与嵌套事务有什么区别？"
