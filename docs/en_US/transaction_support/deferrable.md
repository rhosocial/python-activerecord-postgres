# DEFERRABLE Mode

PostgreSQL supports deferrable constraints for SERIALIZABLE transactions.

## What is DEFERRABLE?

Deferrable constraints are checked at transaction commit time, not at statement time.

## Usage

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.transaction import IsolationLevel

backend = PostgresBackend(connection_config=config)
tm = backend.transaction_manager

# Set DEFERRABLE mode
tm.set_isolation_level(IsolationLevel.SERIALIZABLE)
tm.set_deferrable(True)

with tm:
    # Constraint violations won't be raised until commit
    # Useful for circular references
    pass
```

## Constraint Definition

```sql
-- Deferrable constraint in schema
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    reference_id INTEGER REFERENCES orders(id) DEFERRABLE
);
```

## Use Cases

1. **Circular references**: Insert records that reference each other
2. **Bulk operations**: Defer constraint checking until end
3. **Data migration**: Reorganize data without intermediate violations

💡 *AI Prompt:* "When should I use DEFERRABLE constraints?"
