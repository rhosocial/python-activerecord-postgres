# pg_logicalinspect

`pg_logicalinspect` inspects serialized logical decoding snapshots. Available on **PostgreSQL 18+**; restricted to superusers and `pg_read_server_files` members by default.

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_logicalinspect import (
    pg_get_logical_snapshot_meta, pg_get_logical_snapshot_info,
)
backend.execute(*pg_get_logical_snapshot_meta(dialect, "0-40796E18.snap").to_sql())
```

Capability: `dialect.supports_pg_logicalinspect()`. Creation: `CREATE EXTENSION pg_logicalinspect;`
