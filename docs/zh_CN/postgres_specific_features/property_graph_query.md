# SQL/PGQ 可用性

PostgreSQL 19 Beta 4 在 19.0 正式发布前撤回了 SQL/PGQ，更早的版本也从未发布过该特性。
因此，PostgreSQL 后端对所有服务器版本都默认关闭 `supports_graph_match()` 和
`supports_graph_table()`，包括 PostgreSQL 19 及更高版本。

表达式和格式化器管线仍保留用于受控兼容性测试，但默认情况下，属性图格式化器会抛出
`UnsupportedFeatureError`。

## 为何该能力与版本无关

所有图相关的语法产生式都已从解析器中移除，`CREATE PROPERTY GRAPH`、
`CREATE VERTEX TABLE`、`CREATE EDGE TABLE`、`MATCH`、`GRAPH_TABLE` 和
`PROPERTIES` 现在都是语法错误。相关的系统目录对象也随之消失：

| 目录探针 | 19beta3 | 19beta4 |
| --- | --- | --- |
| `pg_proc` 中 graph/vertex/edge 函数 | 1 | 0 |
| `pg_type` 中 graph/agtype 类型 | 14 | 0 |
| `pg_class` 中属性图关系 | 2 | 0 |
| `GRAPH_TABLE`/`VERTEX`/`EDGE`/`PROPERTY` 关键字 | 4 | 0 |

由于从来没有任何版本发布过该特性，支持与否取决于显式 opt-in，而非 `self.version`
—— 版本号不能作为支持与否的证据，据此推断等于重新宣传一个并不存在的特性。

请注意，这些格式化器从未针对真实服务器验证过：即使在确实具备该特性的 19beta3 上，
生成的 9 条语句中也有 8 条被拒绝。因此下面的覆盖项应视为兼容性钩子，
而不是可用特性。

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
`supports_graph_table()` 需要同时提供两个键，因为 `GRAPH_TABLE` 包裹着 `MATCH`
子句。量化路径和逗号分隔模式没有覆盖入口，始终保持关闭。
