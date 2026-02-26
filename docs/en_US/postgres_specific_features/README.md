# PostgreSQL Specific Features

PostgreSQL offers many advanced features beyond standard SQL. This section covers features unique to or optimized for PostgreSQL.

## Topics

- **[PostgreSQL-Specific Field Types](./field_types.md)**: ARRAY, JSONB, UUID, Range types
- **[PostgreSQL Dialect Expressions](./dialect.md)**: PostgreSQL-specific SQL syntax
- **[Advanced Indexing](./indexing.md)**: GIN, GiST, BRIN indexes
- **[RETURNING Clause](./returning.md)**: Get data back from DML operations

## Feature Highlights

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list        # PostgreSQL ARRAY
    metadata: dict    # PostgreSQL JSONB
    id: UUID         # PostgreSQL UUID
```

💡 *AI Prompt:* "What makes PostgreSQL's JSONB better than regular JSON storage?"
