# TimescaleDB

TimescaleDB adds time-series **hypertables** to PostgreSQL. This backend wraps hypertable creation; compression and retention policies are not covered yet.

The multi-arch `postgres-exts-N` images deliberately do **not** include TimescaleDB. A separate amd64-only image family (`postgres-exts-ts-N`) provides it for PG15–18, and requires `shared_preload_libraries = timescaledb`.

```python
from rhosocial.activerecord.backend.expression import QueryStatement
from rhosocial.activerecord.backend.impl.postgres.functions.timescaledb import create_hypertable

call = create_hypertable(dialect, "metrics", "ts")
backend.execute(*QueryStatement(dialect=dialect, select=[call]).to_sql())
```

Capability: `dialect.supports_timescaledb()`. Creation: `CREATE EXTENSION timescaledb;`
