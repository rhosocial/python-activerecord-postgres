# pgstattuple

`pgstattuple` 返回元组级统计：存活/死亡元组数、空闲空间、索引页利用率等。

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pgstattuple import (
    pgstattuple, pgstatindex, pg_relpages,
)
sql, params = pgstattuple(dialect, "orders").to_sql()
row = backend.fetch_one(f"SELECT ({sql}).tuple_count AS count", params)
```

能力探测：`dialect.supports_pgstattuple()`。启用：`CREATE EXTENSION pgstattuple;`
