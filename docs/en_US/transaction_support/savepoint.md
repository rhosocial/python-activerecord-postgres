# Savepoint Support

Savepoints allow partial rollback within a transaction.

## Creating Savepoints

```python
backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

tm.begin()

# First operation
user1 = User(name="Alice")
user1.save()

# Create savepoint
sp = tm.savepoint()

try:
    # Risky operation
    user2 = User(name="Bob")
    user2.save()
    # If this fails...
except Exception:
    # Rollback to savepoint
    tm.rollback_savepoint(sp)

# Continue with transaction
user3 = User(name="Charlie")
user3.save()

tm.commit()
```

## Async Savepoints

```python
tm = backend.transaction_manager

await tm.begin()

user1 = User(name="Alice")
await user1.save()

sp = await tm.savepoint()

try:
    user2 = User(name="Bob")
    await user2.save()
except Exception:
    await tm.rollback_savepoint(sp)

await tm.commit()
```

## Use Cases

1. **Conditional operations**: Try operations, rollback if conditions not met
2. **Error recovery**: Partial rollback without losing all work
3. **Nested operations**: Handle sub-transactions

💡 *AI Prompt:* "How do savepoints differ from nested transactions?"
