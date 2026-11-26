# amcheck

`amcheck` 检查 B-tree 索引与堆页的逻辑一致性；发现损坏时抛错，但不能证明「没有损坏」。

```python
from rhosocial.activerecord.backend.impl.postgres.functions.amcheck import (
    SkipMode, bt_index_check, bt_index_parent_check, verify_heapam,
)
backend.execute(*bt_index_check(dialect, "idx_orders", heapallindexed=True).to_sql())
backend.execute(*bt_index_parent_check(dialect, "idx_orders").to_sql())
backend.execute(*verify_heapam(dialect, "orders", skip=SkipMode.ALL_VISIBLE).to_sql())
```

`verify_heapam` 每发现一处损坏返回一行。可授权普通用户执行，但需考虑损坏报告中的隐私信息。

能力探测：`dialect.supports_amcheck()`。启用：`CREATE EXTENSION amcheck;`
