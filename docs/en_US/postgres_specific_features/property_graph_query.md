# SQL/PGQ Availability

PostgreSQL 19 Beta 4 withdrew SQL/PGQ. The PostgreSQL backend therefore keeps
`supports_graph_match()` and `supports_graph_table()` disabled for every server
version, including PostgreSQL 19 and later.

The expression and formatter plumbing remains available for controlled compatibility
testing, but graph formatters fail with `UnsupportedFeatureError` by default.

## Capability Detection

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

dialect = PostgresDialect(version=(19, 0, 4))
assert dialect.supports_graph_match() is False
assert dialect.supports_graph_table() is False
```

## Explicit Override

A future implementation can opt in explicitly without relying on a version check:

```python
dialect = PostgresDialect(
    version=(19, 0, 4),
    graph_feature_overrides={
        "graph_match": True,
        "graph_table": True,
    },
)
```

Only use these overrides when the target server or a dedicated compatibility layer
actually provides the corresponding feature.
