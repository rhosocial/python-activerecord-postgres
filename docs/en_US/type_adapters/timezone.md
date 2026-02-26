# Timezone Handling

## TIMESTAMP vs TIMESTAMPTZ

| Type | Description |
|------|-------------|
| `TIMESTAMP` | Without timezone - stores literal time |
| `TIMESTAMPTZ` | With timezone - converts to UTC storage |

## Python Handling

```python
from datetime import datetime, timezone

class Event(ActiveRecord):
    __table_name__ = "events"
    name: str
    created_at: datetime
    scheduled_at: datetime  # TIMESTAMPTZ

# Timezone-aware datetime
event = Event(
    name="Meeting",
    created_at=datetime.now(timezone.utc)
)
```

## Best Practices

1. **Use TIMESTAMPTZ** for future events
2. **Store in UTC** in the database
3. **Convert to local** in presentation layer
4. **Use timezone-aware** datetime objects

```python
from datetime import datetime, timezone

# Good: Timezone-aware
event = Event(
    scheduled_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
)

# Avoid: Timezone-naive for future events
event = Event(
    scheduled_at=datetime(2024, 1, 1, 12, 0)  # Ambiguous!
)
```

💡 *AI Prompt:* "Why is storing timestamps in UTC recommended?"
