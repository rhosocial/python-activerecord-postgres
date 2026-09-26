# SQL/PGQ Availability

PostgreSQL 19 Beta 4 withdrew SQL/PGQ before the 19.0 release, and no earlier
release shipped it either. The PostgreSQL backend therefore keeps
`supports_graph_match()` and `supports_graph_table()` disabled for every server
version, including PostgreSQL 19 and later.

The expression and formatter plumbing remains available for controlled compatibility
testing, but graph formatters fail with `UnsupportedFeatureError` by default.

## Why the capability is version-independent

Every graph grammar production was pulled from the parser, so `CREATE PROPERTY
GRAPH`, `CREATE VERTEX TABLE`, `CREATE EDGE TABLE`, `MATCH`, `GRAPH_TABLE` and
`PROPERTIES` are now syntax errors. The supporting catalog objects went with
them:

| Catalog probe | 19beta3 | 19beta4 |
| --- | --- | --- |
| `pg_proc` graph/vertex/edge functions | 1 | 0 |
| `pg_type` graph/agtype types | 14 | 0 |
| `pg_class` property-graph relations | 2 | 0 |
| `GRAPH_TABLE`/`VERTEX`/`EDGE`/`PROPERTY` keywords | 4 | 0 |

Because no release has ever shipped the feature, support is resolved from
explicit opt-ins rather than from `self.version` — a version number is not
evidence of support, and inferring it would re-advertise a feature that does not
exist.

Note that these formatters were never validated against a real server: even
against 19beta3, which did have the feature, 8 of the 9 generated statements
were rejected. Treat the opt-in below as a compatibility hook, not as a working
feature.

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
actually provides the corresponding feature. `supports_graph_table()` requires
both keys, since `GRAPH_TABLE` wraps a `MATCH` clause. Quantified paths and
comma-separated patterns have no override path and stay off unconditionally.
