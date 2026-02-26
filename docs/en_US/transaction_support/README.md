# Transaction Support

PostgreSQL provides robust transaction support with advanced features like savepoints, isolation levels, and deferrable constraints.

## Topics

- **[Transaction Isolation Levels](./isolation_level.md)**: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- **[Savepoint Support](./savepoint.md)**: Nested transactions
- **[DEFERRABLE Mode](./deferrable.md)**: Deferred constraint checking
- **[Deadlock Handling](./deadlock.md)**: Handling concurrent conflicts

## Quick Start

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

# Basic transaction
with tm:
    user1 = User(name="Alice")
    user1.save()
    user2 = User(name="Bob")
    user2.save()
# Auto-commit on context exit
```

💡 *AI Prompt:* "What are the trade-offs between different transaction isolation levels?"
