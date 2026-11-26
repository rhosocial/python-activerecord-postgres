# pgstattuple

`pgstattuple` returns tuple-level statistics: live/dead tuple counts, free space, and index page utilization.

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pgstattuple import (
    pgstattuple, pgstatindex, pg_relpages,
)
sql, params = pgstattuple(dialect, "orders").to_sql()
row = backend.fetch_one(f"SELECT ({sql}).tuple_count AS count", params)
```

Capability: `dialect.supports_pgstattuple()`. Creation: `CREATE EXTENSION pgstattuple;`
