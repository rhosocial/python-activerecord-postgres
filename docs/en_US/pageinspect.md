# pageinspect

`pageinspect` provides low-level access to the contents of database pages, usually for diagnosing corruption reported by `amcheck`. Most functions require superuser privileges.

```python
from rhosocial.activerecord.backend.impl.postgres.functions.pageinspect import (
    get_raw_page, page_header, bt_page_stats, heap_page_items,
)
sql, params = get_raw_page(dialect, "orders", "main", 0).to_sql()
page = backend.fetch_one(f"SELECT {sql} AS page", params)["page"]
sql, params = page_header(dialect, page).to_sql()
```

Capability: `dialect.supports_pageinspect()`. Creation: `CREATE EXTENSION pageinspect;`
