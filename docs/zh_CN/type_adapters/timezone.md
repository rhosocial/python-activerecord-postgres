# 时区处理

## TIMESTAMP vs TIMESTAMPTZ

| 类型 | 描述 |
|-----|------|
| `TIMESTAMP` | 无时区 - 存储字面时间 |
| `TIMESTAMPTZ` | 带时区 - 转换为 UTC 存储 |

## Python 处理

```python
from datetime import datetime, timezone

class Event(ActiveRecord):
    __table_name__ = "events"
    name: str
    created_at: datetime
    scheduled_at: datetime  # TIMESTAMPTZ

# 带时区的 datetime
event = Event(
    name="会议",
    created_at=datetime.now(timezone.utc)
)
```

## 最佳实践

1. **使用 TIMESTAMPTZ** 存储未来事件
2. **在数据库中存储 UTC** 时间
3. **在展示层转换为本地时间**
4. **使用带时区的** datetime 对象

```python
from datetime import datetime, timezone

# 推荐：带时区
event = Event(
    scheduled_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
)

# 避免：对未来事件使用无时区
event = Event(
    scheduled_at=datetime(2024, 1, 1, 12, 0)  # 有歧义！
)
```

💡 *AI 提示词：* "为什么建议以 UTC 存储时间戳？"
