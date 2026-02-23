# DEFERRABLE 模式

PostgreSQL 支持 SERIALIZABLE 事务的可延迟约束。

## 什么是 DEFERRABLE？

可延迟约束在事务提交时检查，而不是在语句执行时检查。

## 使用方法

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.transaction import IsolationLevel

backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

# 设置 DEFERRABLE 模式
tm.set_isolation_level(IsolationLevel.SERIALIZABLE)
tm.set_deferrable(True)

with tm:
    # 约束违规在提交前不会抛出
    # 适用于循环引用
    pass
```

## 约束定义

```sql
-- 在模式中定义可延迟约束
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    reference_id INTEGER REFERENCES orders(id) DEFERRABLE
);
```

## 使用场景

1. **循环引用**：插入相互引用的记录
2. **批量操作**：延迟约束检查到最后
3. **数据迁移**：重新组织数据而不产生中间违规

💡 *AI 提示词：* "什么时候应该使用 DEFERRABLE 约束？"
