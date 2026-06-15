# PostgreSQL Specific Features

PostgreSQL offers many advanced features beyond standard SQL. This section covers features unique to or optimized for PostgreSQL.

## Topics

- **[PostgreSQL-Specific Field Types](./field_types.md)**: ARRAY, JSONB, UUID, Range types
- **[PostgreSQL Dialect Expressions](./dialect.md)**: PostgreSQL-specific SQL syntax
- **[Advanced Indexing](./indexing.md)**: GIN, GiST, BRIN indexes
- **[RETURNING Clause](./returning.md)**: Get data back from DML operations
- **[Protocol Support Matrix](./protocol_support.md)**: Comprehensive protocol support and version compatibility
- **[Database Introspection](./introspection.md)**: Query metadata using pg_catalog
- **[EXPLAIN Support](./explain.md)**: Query execution plan analysis and performance diagnostics
- **[Table Partitioning](partition.md)**: RANGE / LIST / HASH partitioning and pg_partman management
- **[Property Graph Query](property_graph_query.md)**: SQL/PGQ property graph query (PG 19+)

## Feature Highlights

```python
from rhosocial.activerecord.model import ActiveRecord

class Article(ActiveRecord):
    __table_name__ = "articles"
    title: str
    tags: list  # PostgreSQL ARRAY
    metadata: dict  # PostgreSQL JSONB
    id: UUID  # PostgreSQL UUID
```

💡 *AI Prompt:* "What makes PostgreSQL's JSONB better than regular JSON storage?"

## Related Topics

- **[Backend Specific Features](../backend_specific_features/README.md)**: SSL/TLS, materialized views, introspection
