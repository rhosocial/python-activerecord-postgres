# TimescaleDB

TimescaleDB 为 PostgreSQL 增加时序 **hypertable**。本后端目前只封装 hypertable 创建，压缩与保留策略暂不涉及。

多架构的 `postgres-exts-N` 镜像刻意**不带** TimescaleDB；另有仅 amd64 的 `postgres-exts-ts-N` 镜像族在 PG15–18 上提供，并要求 `shared_preload_libraries = timescaledb`。

```python
from rhosocial.activerecord.backend.expression import QueryStatement
from rhosocial.activerecord.backend.impl.postgres.functions.timescaledb import create_hypertable

call = create_hypertable(dialect, "metrics", "ts")
backend.execute(*QueryStatement(dialect=dialect, select=[call]).to_sql())
```

能力探测：`dialect.supports_timescaledb()`。启用：`CREATE EXTENSION timescaledb;`
