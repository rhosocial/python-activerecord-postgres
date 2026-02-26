# Deadlock Handling

PostgreSQL detects and resolves deadlocks automatically.

## What are Deadlocks?

Deadlocks occur when two transactions wait for each other's locks.

## PostgreSQL Behavior

PostgreSQL detects deadlocks and aborts one transaction with an error.

## Handling Deadlocks

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

# Usage
def transfer_funds():
    tm = backend.transaction_manager
    with tm:
        # Transfer logic
        pass

result = with_retry(transfer_funds)
```

## Best Practices

1. **Consistent lock order**: Always access tables in the same order
2. **Short transactions**: Minimize transaction duration
3. **Retry logic**: Implement retry for deadlock errors
4. **Monitor patterns**: Identify frequently deadlocked operations

💡 *AI Prompt:* "How can I diagnose the cause of frequent deadlocks?"
