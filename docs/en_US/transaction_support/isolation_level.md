# Transaction Isolation Levels

## Available Levels

| Level | Description | Phenomena Prevented |
|-------|-------------|---------------------|
| READ COMMITTED | Default level | Dirty reads |
| REPEATABLE READ | Consistent reads within transaction | Dirty reads, Non-repeatable reads |
| SERIALIZABLE | Strictest isolation | All phenomena |

## Setting Isolation Level

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.transaction import IsolationLevel

backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

# Set isolation level
tm.set_isolation_level(IsolationLevel.SERIALIZABLE)

with tm:
    # Transaction with SERIALIZABLE isolation
    pass
```

## READ COMMITTED (Default)

- Each query sees only committed data
- May see different data in subsequent queries
- Good for most applications

## REPEATABLE READ

- All queries in transaction see consistent snapshot
- Prevents non-repeatable reads
- May fail with serialization errors

## SERIALIZABLE

- Strictest isolation
- Prevents phantom reads
- May have more serialization failures
- Use with retry logic

```python
from rhosocial.activerecord.backend.errors import OperationalError

max_retries = 3
for attempt in range(max_retries):
    try:
        with tm:
            tm.set_isolation_level(IsolationLevel.SERIALIZABLE)
            # Critical operations
            break
    except OperationalError as e:
        if "serialization" in str(e).lower():
            continue
        raise
```

💡 *AI Prompt:* "When should I use SERIALIZABLE isolation level?"
