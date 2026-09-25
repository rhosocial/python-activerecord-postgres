# SQL/PGQ 可用性

PostgreSQL 19 Beta 4 已撤回 SQL/PGQ。因此，PostgreSQL 后端对所有服务器版本都默认关闭
`supports_graph_match()` 和 `supports_graph_table()`，包括 PostgreSQL 19 及更高版本。

表达式和格式化器管线仍保留用于受控兼容性测试，但默认情况下，属性图格式化器会抛出
`UnsupportedFeatureError`。

## 能力检测

```python
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect

dialect = PostgresDialect(version=(19, 0, 4))
assert dialect.supports_graph_match() is False
assert dialect.supports_graph_table() is False
```

## 显式覆盖

未来实现可以显式启用这些能力，而不依赖版本判断：

```python
dialect = PostgresDialect(
    version=(19, 0, 4),
    graph_feature_overrides={
        "graph_match": True,
        "graph_table": True,
    },
)
```

仅当目标服务器或专用兼容层实际提供相应功能时，才应使用这些覆盖项。
