# 死锁处理

PostgreSQL 自动检测并解决死锁。

## 什么是死锁？

当两个事务互相等待对方的锁时，就会发生死锁。

## PostgreSQL 的行为

PostgreSQL 检测到死锁后会中止其中一个事务并报错。

## 处理死锁

```python
from rhosocial.activerecord.backend.errors import DeadlockError
import time

def with_retry(operation, max_retries=3, delay=0.1):
    for attempt in range(max_retries):
        try:
            return operation()
        except DeadlockError:
            if attempt == max_retries - 1:
                raise
            time.sleep(delay * (attempt + 1))

# 使用示例
def transfer_funds():
    tm = backend.transaction_manager
    with tm:
        # 转账逻辑
        pass

result = with_retry(transfer_funds)
```

## 最佳实践

1. **一致的锁顺序**：始终以相同顺序访问表
2. **短事务**：尽量缩短事务持续时间
3. **重试逻辑**：为死锁错误实现重试
4. **监控模式**：识别频繁发生死锁的操作

💡 *AI 提示词：* "如何诊断频繁死锁的原因？"
