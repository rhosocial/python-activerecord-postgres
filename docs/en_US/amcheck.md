# amcheck

`amcheck` verifies the logical consistency of B-tree indexes and heap pages. It raises an error when corruption is found; it cannot prove absence of corruption.

```python
from rhosocial.activerecord.backend.impl.postgres.functions.amcheck import (
    SkipMode, bt_index_check, bt_index_parent_check, verify_heapam,
)
backend.execute(*bt_index_check(dialect, "idx_orders", heapallindexed=True).to_sql())
backend.execute(*bt_index_parent_check(dialect, "idx_orders").to_sql())
backend.execute(*verify_heapam(dialect, "orders", skip=SkipMode.ALL_VISIBLE).to_sql())
```

`verify_heapam` returns one row per detected corruption. Permission may be granted to non-superusers, but consider the privacy implications of corruption reports.

Capability: `dialect.supports_amcheck()`. Creation: `CREATE EXTENSION amcheck;`
