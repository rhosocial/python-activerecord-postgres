# pg_logicalinspect

`pg_logicalinspect` 用于检查序列化的逻辑解码快照，仅 **PostgreSQL 18+** 可用；默认限超级用户与 `pg_read_server_files` 角色。

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pg_logicalinspect import (
    pg_get_logical_snapshot_meta, pg_get_logical_snapshot_info,
)
backend.execute(*pg_get_logical_snapshot_meta(dialect, "0-40796E18.snap").to_sql())
```

能力探测：`dialect.supports_pg_logicalinspect()`。启用：`CREATE EXTENSION pg_logicalinspect;`
