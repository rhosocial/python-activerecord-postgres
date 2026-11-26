# pageinspect

`pageinspect` 提供数据库页面的底层访问，常用于诊断 `amcheck` 报告的损坏；多数函数需要超级用户权限。

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pageinspect import (
    get_raw_page, page_header, bt_page_stats, heap_page_items,
)
sql, params = get_raw_page(dialect, "orders", "main", 0).to_sql()
page = backend.fetch_one(f"SELECT {sql} AS page", params)["page"]
sql, params = page_header(dialect, page).to_sql()
```

能力探测：`dialect.supports_pageinspect()`。启用：`CREATE EXTENSION pageinspect;`
